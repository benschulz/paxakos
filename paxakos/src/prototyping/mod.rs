use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::stream::Stream;
use thiserror::Error;

use crate::append::AppendError;
use crate::append::RetryPolicy;
use crate::communicator::Acceptance;
use crate::communicator::AcceptanceFor;
use crate::communicator::Committed;
use crate::communicator::Communicator;
use crate::communicator::Vote;
use crate::communicator::VoteFor;
use crate::error::BoxError;
use crate::state::LogEntryOf;
use crate::state::NodeIdOf;
use crate::state::NodeOf;
use crate::CoordNum;
use crate::LogEntry;
use crate::NodeInfo;
use crate::RequestHandler;
use crate::RoundNum;
use crate::State;

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
pub struct RetryIndefinitely<C>(u64, crate::util::PhantomSend<C>);

impl<C> RetryIndefinitely<C> {
    pub fn without_pausing() -> Self {
        Self(0, crate::util::PhantomSend::new())
    }

    pub fn pausing_up_to(duration: std::time::Duration) -> Self {
        Self(duration.as_millis() as u64, crate::util::PhantomSend::new())
    }
}

#[async_trait]
impl<C> RetryPolicy for RetryIndefinitely<C>
where
    C: Communicator,
{
    type Communicator = C;

    async fn eval(&mut self, _err: AppendError<Self::Communicator>) -> Result<(), BoxError> {
        if self.0 > 0 {
            use rand::Rng;

            let delay = rand::thread_rng().gen_range(0..=self.0);
            let delay = std::time::Duration::from_millis(delay);

            futures_timer::Delay::new(delay).await;
        }

        Ok(())
    }
}

type RequestHandlers<S, R, C, A, J> =
    HashMap<NodeIdOf<S>, RequestHandler<S, DirectCommunicator<S, R, C, A, J>>>;
type EventListeners<S, R, C> = Vec<mpsc::Sender<DirectCommunicatorEvent<S, R, C>>>;
type PacketLossRates<S> = HashMap<(NodeIdOf<S>, NodeIdOf<S>), f32>;
type E2eDelays<S> = HashMap<(NodeIdOf<S>, NodeIdOf<S>), rand_distr::Normal<f32>>;

#[derive(Debug)]
pub struct DirectCommunicators<S, R, C, A, J>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync + 'static,
    J: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::type_complexity)]
    request_handlers: Arc<Mutex<RequestHandlers<S, R, C, A, J>>>,
    default_packet_loss: f32,
    default_e2e_delay: rand_distr::Normal<f32>,
    packet_loss: Arc<Mutex<PacketLossRates<S>>>,
    e2e_delay: Arc<Mutex<E2eDelays<S>>>,
    event_listeners: Arc<Mutex<EventListeners<S, R, C>>>,
    _p: std::marker::PhantomData<A>,
}

impl<S, R, C, A, J> DirectCommunicators<S, R, C, A, J>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync,
    J: std::fmt::Debug + Send + Sync,
{
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::with_characteristics(0.0, rand_distr::Normal::new(0.0, 0.0).unwrap())
    }

    pub fn with_characteristics(packet_loss: f32, e2e_delay: rand_distr::Normal<f32>) -> Self {
        Self {
            request_handlers: Arc::new(Mutex::new(HashMap::new())),
            default_packet_loss: packet_loss,
            default_e2e_delay: e2e_delay,
            packet_loss: Arc::new(Mutex::new(HashMap::new())),
            e2e_delay: Arc::new(Mutex::new(HashMap::new())),
            event_listeners: Arc::new(Mutex::new(Vec::new())),
            _p: std::marker::PhantomData,
        }
    }

    #[allow(dead_code)]
    pub async fn set_packet_loss(&mut self, from: NodeIdOf<S>, to: NodeIdOf<S>, packet_loss: f32) {
        let mut link = self.packet_loss.lock().await;
        link.insert((from, to), packet_loss);
    }

    #[allow(dead_code)]
    pub async fn set_delay(
        &mut self,
        from: NodeIdOf<S>,
        to: NodeIdOf<S>,
        delay: rand_distr::Normal<f32>,
    ) {
        let mut link = self.e2e_delay.lock().await;
        link.insert((from, to), delay);
    }

    #[allow(dead_code)]
    pub fn register(
        &self,
        node_id: NodeIdOf<S>,
        handler: RequestHandler<S, DirectCommunicator<S, R, C, A, J>>,
    ) {
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

    pub fn create_communicator_for(
        &self,
        node_id: NodeIdOf<S>,
    ) -> DirectCommunicator<S, R, C, A, J> {
        DirectCommunicator {
            set: self.clone(),
            node_id,
        }
    }
}

impl<S, R, C, A, J> Clone for DirectCommunicators<S, R, C, A, J>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync,
    J: std::fmt::Debug + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            request_handlers: Arc::clone(&self.request_handlers),
            default_packet_loss: self.default_packet_loss,
            default_e2e_delay: self.default_e2e_delay,
            packet_loss: Arc::clone(&self.packet_loss),
            e2e_delay: Arc::clone(&self.e2e_delay),
            event_listeners: Arc::clone(&self.event_listeners),
            _p: std::marker::PhantomData,
        }
    }
}

impl<S, R, C, A, J> Default for DirectCommunicators<S, R, C, A, J>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync,
    J: std::fmt::Debug + Send + Sync,
{
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
    Committed(bool),
}

#[derive(Debug, Error)]
pub enum DirectCommunicatorError {
    #[error("other")]
    Other,

    #[error("timeout")]
    Timeout,
}

#[derive(Debug)]
pub struct DirectCommunicator<S, R, C, A, J>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync + 'static,
    J: std::fmt::Debug + Send + Sync + 'static,
{
    set: DirectCommunicators<S, R, C, A, J>,
    node_id: NodeIdOf<S>,
}

impl<S, R, C, A, J> Clone for DirectCommunicator<S, R, C, A, J>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync,
    J: std::fmt::Debug + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            set: self.set.clone(),
            node_id: self.node_id,
        }
    }
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
                        let (packet_loss_rate_there, packet_loss_rate_back) = {
                            let per_link = this.set.packet_loss.lock().await;

                            let there = per_link.get(&(this.node_id, receiver_id)).copied();
                            let there = there.unwrap_or(this.set.default_packet_loss);

                            let back = per_link.get(&(receiver_id, this.node_id)).copied();
                            let back = back.unwrap_or(this.set.default_packet_loss);

                            (there, back)
                        };
                        let (e2e_delay_distr_there, e2e_delay_distr_back) = {
                            let per_link = this.set.e2e_delay.lock().await;

                            let there = per_link.get(&(this.node_id, receiver_id)).copied();
                            let there = there.unwrap_or(this.set.default_e2e_delay);

                            let back = per_link.get(&(receiver_id, this.node_id)).copied();
                            let back = back.unwrap_or(this.set.default_e2e_delay);

                            (there, back)
                        };

                        let e2e_delay = delay(&e2e_delay_distr_there);
                        let dropped = roll_for_failure(packet_loss_rate_there);

                        {
                            let listeners = this.set.event_listeners.lock().await;
                            for mut l in listeners.iter().cloned() {
                                let _ = l.send(DirectCommunicatorEvent {
                                    sender: this.node_id,
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

                        let e2e_delay = delay(&e2e_delay_distr_back);
                        let dropped = roll_for_failure(packet_loss_rate_back);

                        {
                            let listeners = this.set.event_listeners.lock().await;
                            for mut l in listeners.iter().cloned() {
                                let _ = l.send(DirectCommunicatorEvent {
                                    sender: receiver_id,
                                    receiver: this.node_id,
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

impl<S, R, C, A, J> Communicator for DirectCommunicator<S, R, C, A, J>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
    A: std::fmt::Debug + Send + Sync + 'static,
    J: std::fmt::Debug + Send + Sync + 'static,
{
    type Node = NodeOf<S>;

    type RoundNum = R;
    type CoordNum = C;

    type LogEntry = LogEntryOf<S>;

    type Error = DirectCommunicatorError;
    type Abstention = A;
    type Rejection = J;

    type SendPrepare = LocalBoxFuture<'static, Result<VoteFor<Self>, Self::Error>>;
    type SendProposal = LocalBoxFuture<'static, Result<AcceptanceFor<Self>, Self::Error>>;
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
            |r| DirectCommunicatorPayload::Promise(matches!(r, &Ok(Vote::Given(_))));
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
            |r| DirectCommunicatorPayload::Accept(matches!(r, &Ok(Acceptance::Given)));
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
            |r| DirectCommunicatorPayload::Committed(matches!(r, &Ok(_)));
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
            |r| DirectCommunicatorPayload::Committed(matches!(r, &Ok(_)));
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
