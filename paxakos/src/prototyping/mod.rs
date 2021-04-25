use std::cell::Cell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::stream::Stream;
use thiserror::Error;

use crate::append::{AppendError, RetryPolicy};
use crate::communicator::{AcceptanceOrRejection, AcceptanceOrRejectionFor, Committed};
use crate::communicator::{Communicator, PromiseOrRejection, PromiseOrRejectionFor};
use crate::error::BoxError;
use crate::state::{LogEntryOf, NodeIdOf, NodeOf};
use crate::{CoordNum, LogEntry, NodeInfo};
use crate::{RequestHandler, RoundNum, State};

/// A `NodeInfo` implementation for prototyping.
#[derive(Clone, Copy, Debug, Default)]
pub struct PrototypingNode(usize);

static NODE_ID_DISPENSER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl PrototypingNode {
    pub fn new() -> Self {
        Self(NODE_ID_DISPENSER.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    pub fn with_id(id: usize) -> Self {
        Self(id)
    }
}

impl NodeInfo for PrototypingNode {
    type Id = usize;

    fn id(&self) -> Self::Id {
        self.0
    }
}

#[derive(Debug)]
pub struct RetryIndefinitely(u64);

impl RetryIndefinitely {
    pub fn without_pausing() -> Self {
        Self(0)
    }

    pub fn pausing_up_to(duration: std::time::Duration) -> Self {
        Self(duration.as_millis() as u64)
    }
}

#[async_trait]
impl RetryPolicy for RetryIndefinitely {
    async fn eval(&mut self, _err: AppendError) -> Result<(), BoxError> {
        if self.0 > 0 {
            use rand::Rng;

            let delay = rand::thread_rng().gen_range(0..=self.0);
            let delay = std::time::Duration::from_millis(delay);

            futures_timer::Delay::new(delay).await;
        }

        Ok(())
    }
}

type RequestHandlers<S, R, C> = HashMap<NodeIdOf<S>, RequestHandler<S, R, C>>;
type EventListeners<S, R, C> = Vec<mpsc::Sender<DirectCommunicatorEvent<S, R, C>>>;

#[derive(Clone, Debug)]
pub struct DirectCommunicators<S: State, R: RoundNum, C: CoordNum> {
    request_handlers: Arc<Mutex<RequestHandlers<S, R, C>>>,
    failure_rate: Cell<f32>,
    e2e_delay_ms_distr: Cell<rand_distr::Normal<f32>>,
    event_listeners: Arc<Mutex<EventListeners<S, R, C>>>,
}

impl<S: State, R: RoundNum, C: CoordNum> DirectCommunicators<S, R, C> {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::with_characteristics(0.0, rand_distr::Normal::new(0.0, 0.0).unwrap())
    }

    pub fn with_characteristics(
        failure_rate: f32,
        e2e_delay_ms_distr: rand_distr::Normal<f32>,
    ) -> Self {
        Self {
            request_handlers: Arc::new(Mutex::new(HashMap::new())),
            failure_rate: Cell::new(failure_rate),
            e2e_delay_ms_distr: Cell::new(e2e_delay_ms_distr),
            event_listeners: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[allow(dead_code)]
    pub fn failure_rate(&mut self, failure_rate: f32) {
        self.failure_rate.set(failure_rate);
    }

    #[allow(dead_code)]
    pub fn register(&self, node_id: NodeIdOf<S>, handler: RequestHandler<S, R, C>) {
        futures::executor::block_on(async {
            let mut handlers = self.request_handlers.lock().await;
            handlers.insert(node_id, handler);
        });
    }

    #[allow(dead_code)]
    pub fn events(&self) -> impl Stream<Item = DirectCommunicatorEvent<S, R, C>> {
        let (send, recv) = mpsc::channel(16);

        futures::executor::block_on(async {
            let mut listeners = self.event_listeners.lock().await;
            listeners.push(send);
        });

        recv
    }

    pub fn create_communicator_for(&self, node_id: NodeIdOf<S>) -> DirectCommunicator<S, R, C> {
        DirectCommunicator {
            set: self.clone(),
            node_id,
        }
    }
}

impl<S: State, R: RoundNum, C: CoordNum> Default for DirectCommunicators<S, R, C> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct DirectCommunicatorEvent<S: State, R: RoundNum, C: CoordNum> {
    pub sender: NodeIdOf<S>,
    pub receiver: NodeIdOf<S>,
    pub e2e_delay: std::time::Duration,
    pub dropped: bool,
    pub payload: DirectCommunicatorPayload<S, R, C>,
}

#[derive(Clone, Debug)]
pub enum DirectCommunicatorPayload<S: State, R: RoundNum, C: CoordNum> {
    Prepare {
        round_num: R,
        coord_num: C,
    },
    Promise(bool),
    Propose {
        round_num: R,
        coord_num: C,
        log_entry: Arc<LogEntryOf<S>>,
    },
    Accept(bool),
    Commit {
        round_num: R,
        coord_num: C,
        log_entry: Arc<LogEntryOf<S>>,
    },
    CommitById {
        round_num: R,
        coord_num: C,
    },
    Committed,
}

#[derive(Debug, Error)]
pub enum DirectCommunicatorError {
    #[error("other")]
    Other,

    #[error("timeout")]
    Timeout,
}

#[derive(Clone, Debug)]
pub struct DirectCommunicator<S: State, R: RoundNum, C: CoordNum> {
    set: DirectCommunicators<S, R, C>,
    node_id: NodeIdOf<S>,
}

macro_rules! send_fn {
    (
        $self:ident, $receivers:ident $(, $non_copy_arg:ident)* ;
        $method:ident $(, $arg:ident)* ;
        $request_payload:expr;
        $response_payload:expr;
    ) => {{
        $receivers
            .iter()
            .map(move |receiver| {
                let this = $self.clone();
                let receiver_id = receiver.id();

                $( send_fn!(@ $non_copy_arg); )*

                (
                    receiver,
                    async move {
                        let e2e_delay = delay(&this.set.e2e_delay_ms_distr.get());
                        let dropped = roll_for_failure(this.set.failure_rate.get());

                        {
                            let listeners = this.set.event_listeners.lock().await;
                            for mut l in listeners.iter().cloned() {
                                let _ = l.send(DirectCommunicatorEvent {
                                    sender: this.node_id.clone(),
                                    receiver: receiver_id,
                                    e2e_delay,
                                    dropped,
                                    payload: $request_payload,
                                }).await;
                            }
                        }

                        futures_timer::Delay::new(e2e_delay).await;

                        if dropped {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        let result = {
                            let handlers = this.set.request_handlers.lock().await;
                            let handler = match handlers.get(&receiver_id) {
                                Some(handler) => handler,
                                None => return Err(DirectCommunicatorError::Other),
                            };

                            handler.$method($($arg),*)
                        }
                        .await;
                        let response = result
                            .try_into()
                            .map_err(|_| DirectCommunicatorError::Other);

                        let e2e_delay = delay(&this.set.e2e_delay_ms_distr.get());
                        let dropped = roll_for_failure(this.set.failure_rate.get());

                        {
                            let listeners = this.set.event_listeners.lock().await;
                            for mut l in listeners.iter().cloned() {
                                let _ = l.send(DirectCommunicatorEvent {
                                    sender: receiver_id,
                                    receiver: this.node_id.clone(),
                                    e2e_delay,
                                    dropped,
                                    payload: $response_payload(&response),
                                }).await;
                            }
                        }

                        futures_timer::Delay::new(e2e_delay).await;

                        if dropped {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        response
                    }
                    .boxed_local(),
                )
            })
            .collect()
    }};

    (@ $non_copy_arg:ident) => {
        let $non_copy_arg = $non_copy_arg.clone();
    }
}

impl<S: State, R: RoundNum, C: CoordNum> Communicator for DirectCommunicator<S, R, C> {
    type Node = NodeOf<S>;

    type RoundNum = R;
    type CoordNum = C;

    type LogEntry = LogEntryOf<S>;

    type Error = DirectCommunicatorError;

    type SendPrepare = LocalBoxFuture<'static, Result<PromiseOrRejectionFor<Self>, Self::Error>>;
    type SendProposal =
        LocalBoxFuture<'static, Result<AcceptanceOrRejectionFor<Self>, Self::Error>>;
    type SendCommit = LocalBoxFuture<'static, Result<Committed, Self::Error>>;
    type SendCommitById = LocalBoxFuture<'static, Result<Committed, Self::Error>>;

    fn send_prepare<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
    ) -> Vec<(&'a Self::Node, Self::SendPrepare)> {
        send_fn!(
            self, receivers;
            handle_prepare, round_num, coord_num;
            DirectCommunicatorPayload::Prepare { round_num, coord_num };
            |r| DirectCommunicatorPayload::Promise(matches!(r, &Ok(PromiseOrRejection::Promise(_))));
        )
    }

    fn send_proposal<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(&'a Self::Node, Self::SendProposal)> {
        send_fn!(
            self, receivers, log_entry;
            handle_proposal, round_num, coord_num, log_entry;
            DirectCommunicatorPayload::Propose { round_num, coord_num, log_entry: log_entry.clone() };
            |r| DirectCommunicatorPayload::Accept(matches!(r, &Ok(AcceptanceOrRejection::Acceptance)));
        )
    }

    fn send_commit<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(&'a Self::Node, Self::SendCommit)> {
        send_fn!(
            self, receivers, log_entry;
            handle_commit, round_num, coord_num, log_entry;
            DirectCommunicatorPayload::Commit { round_num, coord_num, log_entry: log_entry.clone() };
            |_| DirectCommunicatorPayload::Committed;
        )
    }

    fn send_commit_by_id<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry_id: <Self::LogEntry as LogEntry>::Id,
    ) -> Vec<(&'a Self::Node, Self::SendCommitById)> {
        send_fn!(
            self, receivers;
            handle_commit_by_id, round_num, coord_num, log_entry_id;
            DirectCommunicatorPayload::CommitById { round_num, coord_num };
            |_| DirectCommunicatorPayload::Committed;
        )
    }
}

fn roll_for_failure(rate: f32) -> bool {
    use rand::Rng;

    rand::thread_rng().gen::<f32>() < rate
}

fn delay(distr: &rand_distr::Normal<f32>) -> std::time::Duration {
    use rand::distributions::Distribution;

    let delay_ms = distr.sample(&mut rand::thread_rng());
    let delay_ms = delay_ms as u64;

    std::time::Duration::from_millis(delay_ms)
}
