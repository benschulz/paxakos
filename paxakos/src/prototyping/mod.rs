#![allow(missing_docs)]

use std::collections::HashMap;
use std::convert::Infallible;
use std::convert::TryInto;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::stream::Stream;
use thiserror::Error;

use crate::append::AppendError;
use crate::communicator::Acceptance;
use crate::communicator::AcceptanceFor;
use crate::communicator::Committed;
use crate::communicator::Communicator;
use crate::communicator::Vote;
use crate::communicator::VoteFor;
use crate::error::ShutDown;
use crate::invocation::AbstainOf;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;
use crate::invocation::NodeIdOf;
use crate::invocation::NodeOf;
use crate::invocation::RoundNumOf;
use crate::invocation::YeaOf;
use crate::retry::RetryPolicy;
use crate::LogEntry;
use crate::NodeInfo;
use crate::RequestHandler;

/// A `NodeInfo` implementation for prototyping.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
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
pub struct RetryIndefinitely<I>(u64, crate::util::PhantomSend<I>);

impl<I> RetryIndefinitely<I> {
    pub fn without_pausing() -> Self {
        Self(0, crate::util::PhantomSend::new())
    }

    pub fn pausing_up_to(duration: std::time::Duration) -> Self {
        Self(duration.as_millis() as u64, crate::util::PhantomSend::new())
    }
}

impl<I: Invocation> RetryPolicy for RetryIndefinitely<I> {
    type Invocation = I;
    type Error = Infallible;
    type StaticError = ShutDown;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn eval(&mut self, _err: AppendError<Self::Invocation>) -> Self::Future {
        let limit = self.0;

        async move {
            if limit > 0 {
                use rand::Rng;

                let delay = rand::thread_rng().gen_range(0..=limit);
                let delay = std::time::Duration::from_millis(delay);

                sleep(delay).await;
            }

            Ok(())
        }
        .boxed()
    }
}

type RequestHandlers<I> = HashMap<NodeIdOf<I>, RequestHandler<I>>;
type EventListeners<I> = Vec<mpsc::Sender<DirectCommunicatorEvent<I>>>;
type PacketLossRates<I> = HashMap<(NodeIdOf<I>, NodeIdOf<I>), f32>;
type E2eDelays<I> = HashMap<(NodeIdOf<I>, NodeIdOf<I>), rand_distr::Normal<f32>>;

#[derive(Debug)]
pub struct DirectCommunicators<I: Invocation> {
    #[allow(clippy::type_complexity)]
    request_handlers: Arc<Mutex<RequestHandlers<I>>>,
    default_packet_loss: f32,
    default_e2e_delay: rand_distr::Normal<f32>,
    packet_loss: Arc<Mutex<PacketLossRates<I>>>,
    e2e_delay: Arc<Mutex<E2eDelays<I>>>,
    event_listeners: Arc<Mutex<EventListeners<I>>>,
}

impl<I: Invocation> DirectCommunicators<I> {
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
        }
    }

    pub async fn set_packet_loss(&mut self, from: NodeIdOf<I>, to: NodeIdOf<I>, packet_loss: f32) {
        let mut link = self.packet_loss.lock().await;
        link.insert((from, to), packet_loss);
    }

    pub async fn set_delay(
        &mut self,
        from: NodeIdOf<I>,
        to: NodeIdOf<I>,
        delay: rand_distr::Normal<f32>,
    ) {
        let mut link = self.e2e_delay.lock().await;
        link.insert((from, to), delay);
    }

    pub async fn register(&self, node_id: NodeIdOf<I>, handler: RequestHandler<I>) {
        let mut handlers = self.request_handlers.lock().await;
        handlers.insert(node_id, handler);
    }

    pub fn events(&self) -> impl Stream<Item = DirectCommunicatorEvent<I>> {
        let (send, recv) = mpsc::channel(16);

        futures::executor::block_on(async {
            let mut listeners = self.event_listeners.lock().await;
            listeners.push(send);
        });

        recv
    }

    pub fn create_communicator_for(&self, node_id: NodeIdOf<I>) -> DirectCommunicator<I> {
        DirectCommunicator {
            set: self.clone(),
            node_id,
        }
    }
}

impl<I: Invocation> Clone for DirectCommunicators<I> {
    fn clone(&self) -> Self {
        Self {
            request_handlers: Arc::clone(&self.request_handlers),
            default_packet_loss: self.default_packet_loss,
            default_e2e_delay: self.default_e2e_delay,
            packet_loss: Arc::clone(&self.packet_loss),
            e2e_delay: Arc::clone(&self.e2e_delay),
            event_listeners: Arc::clone(&self.event_listeners),
        }
    }
}

impl<I: Invocation> Default for DirectCommunicators<I> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct DirectCommunicatorEvent<I: Invocation> {
    pub sender: NodeIdOf<I>,
    pub receiver: NodeIdOf<I>,
    pub e2e_delay: std::time::Duration,
    pub dropped: bool,
    pub payload: DirectCommunicatorPayload<I>,
}

#[derive(Clone, Debug)]
pub enum DirectCommunicatorPayload<I: Invocation> {
    Prepare {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    },
    Promise(bool),
    Propose {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        log_entry: Arc<LogEntryOf<I>>,
    },
    Accept(bool),
    Commit {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        log_entry: Arc<LogEntryOf<I>>,
    },
    CommitById {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
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
pub struct DirectCommunicator<I: Invocation> {
    set: DirectCommunicators<I>,
    node_id: NodeIdOf<I>,
}

impl<I: Invocation> Clone for DirectCommunicator<I> {
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

                        sleep(e2e_delay).await;

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

                        sleep(e2e_delay).await;

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

impl<I: Invocation + 'static> Communicator for DirectCommunicator<I> {
    type Node = NodeOf<I>;

    type RoundNum = RoundNumOf<I>;
    type CoordNum = CoordNumOf<I>;

    type LogEntry = LogEntryOf<I>;

    type Error = DirectCommunicatorError;

    type SendPrepare = LocalBoxFuture<'static, Result<VoteFor<Self>, Self::Error>>;
    type Abstain = AbstainOf<I>;

    type SendProposal = LocalBoxFuture<'static, Result<AcceptanceFor<Self>, Self::Error>>;
    type Yea = YeaOf<I>;
    type Nay = NayOf<I>;

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
            |r| DirectCommunicatorPayload::Accept(matches!(r, &Ok(Acceptance::Given(_))));
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

async fn sleep(duration: std::time::Duration) {
    if duration > std::time::Duration::ZERO {
        futures_timer::Delay::new(duration).await;
    }
}

fn delay(distr: &rand_distr::Normal<f32>) -> std::time::Duration {
    use rand::distributions::Distribution;

    let delay_ms = distr.sample(&mut rand::thread_rng());
    let delay_ms = delay_ms as u64;

    std::time::Duration::from_millis(delay_ms)
}
