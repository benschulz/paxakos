use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::future::{FutureExt, LocalBoxFuture};
use thiserror::Error;

use crate::append::{AppendError, RetryPolicy};
use crate::communicator::{AcceptanceOrRejection, Communicator, PromiseOrRejection};
use crate::error::BoxError;
use crate::state::{LogEntryOf, NodeIdOf, NodeOf};
use crate::{AcceptError, CoordNum, LogEntry, NodeInfo, PrepareError};
use crate::{Rejection, RequestHandler, RoundNum, State};

/// A `NodeInfo` implementation for prototyping.
#[derive(Clone, Copy, Debug)]
pub struct PrototypingNode(usize);

static NODE_ID_DISPENSER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl PrototypingNode {
    pub fn new() -> Self {
        Self(NODE_ID_DISPENSER.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
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

            let delay = rand::thread_rng().gen_range(0, self.0);
            let delay = std::time::Duration::from_millis(delay);

            futures_timer::Delay::new(delay).await;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DirectCommunicator<S: State, R: RoundNum, C: CoordNum> {
    request_handlers: Arc<Mutex<HashMap<NodeIdOf<S>, RequestHandler<S, R, C>>>>,
    failure_rate: Cell<f32>,
    e2e_delay_ms_distr: Cell<rand_distr::Normal<f32>>,
}

#[derive(Debug, Error)]
pub enum DirectCommunicatorError {
    #[error("other")]
    Other,

    #[error("timeout")]
    Timeout,
}

impl<S: State, R: RoundNum, C: CoordNum> DirectCommunicator<S, R, C> {
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
        }
    }

    #[allow(dead_code)]
    pub fn faiure_rate(&mut self, failure_rate: f32) {
        self.failure_rate.set(failure_rate);
    }

    #[allow(dead_code)]
    pub fn register(&self, node_id: NodeIdOf<S>, handler: RequestHandler<S, R, C>) {
        let mut handlers = self.request_handlers.lock().unwrap();

        handlers.insert(node_id, handler);
    }

    #[allow(dead_code)]
    pub fn node_ids(&self) -> Vec<NodeIdOf<S>> {
        let handlers = self.request_handlers.lock().unwrap();

        handlers.keys().copied().collect()
    }
}

impl<S: State, R: RoundNum, C: CoordNum> Communicator for DirectCommunicator<S, R, C> {
    type Node = NodeOf<S>;

    type RoundNum = R;
    type CoordNum = C;

    type LogEntry = LogEntryOf<S>;

    type Error = DirectCommunicatorError;

    fn send_prepare<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
    ) -> Vec<(
        &'a Self::Node,
        LocalBoxFuture<
            'static,
            Result<PromiseOrRejection<Self::RoundNum, Self::CoordNum, Self::LogEntry>, Self::Error>,
        >,
    )> {
        receivers
            .iter()
            .map(move |receiver| {
                let this = self.clone();
                let receiver_id = receiver.id();

                (
                    receiver,
                    async move {
                        futures_timer::Delay::new(delay(&this.e2e_delay_ms_distr.get())).await;

                        if roll_for_failure(this.failure_rate.get()) {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        let response = {
                            let handlers = this.request_handlers.lock().unwrap();
                            let handler = match handlers.get(&receiver_id) {
                                Some(handler) => handler,
                                None => return Err(DirectCommunicatorError::Other),
                            };

                            handler.handle_prepare(round_num, coord_num)
                        }
                        .await;

                        if roll_for_failure(this.failure_rate.get()) {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        match response {
                            Ok(promise) => Ok(PromiseOrRejection::Promise(promise)),
                            Err(PrepareError::Conflict(coord_num)) => {
                                Ok(PromiseOrRejection::Rejection(Rejection::Conflict {
                                    coord_num,
                                }))
                            }
                            Err(PrepareError::Converged(coord_num, log_entry)) => {
                                Ok(PromiseOrRejection::Rejection(Rejection::Converged {
                                    coord_num,
                                    log_entry,
                                }))
                            }
                            Err(_) => Err(DirectCommunicatorError::Other),
                        }
                    }
                    .boxed_local(),
                )
            })
            .collect()
    }

    fn send_proposal<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(
        &'a Self::Node,
        LocalBoxFuture<
            'static,
            Result<AcceptanceOrRejection<Self::CoordNum, Self::LogEntry>, Self::Error>,
        >,
    )> {
        receivers
            .iter()
            .map(move |receiver| {
                let this = self.clone();
                let receiver_id = receiver.id();
                let log_entry = Arc::clone(&log_entry);

                (
                    receiver,
                    async move {
                        futures_timer::Delay::new(delay(&this.e2e_delay_ms_distr.get())).await;

                        if roll_for_failure(this.failure_rate.get()) {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        let response = {
                            let handlers = this.request_handlers.lock().unwrap();
                            let handler = match handlers.get(&receiver_id) {
                                Some(handler) => handler,
                                None => return Err(DirectCommunicatorError::Other),
                            };

                            handler.handle_proposal(round_num, coord_num, log_entry)
                        }
                        .await;

                        if roll_for_failure(this.failure_rate.get()) {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        match response {
                            Ok(()) => Ok(AcceptanceOrRejection::Acceptance),
                            Err(AcceptError::Conflict(coord_num)) => {
                                Ok(AcceptanceOrRejection::Rejection(Rejection::Conflict {
                                    coord_num,
                                }))
                            }
                            Err(AcceptError::Converged(coord_num, log_entry)) => {
                                Ok(AcceptanceOrRejection::Rejection(Rejection::Converged {
                                    coord_num,
                                    log_entry,
                                }))
                            }
                            Err(_) => Err(DirectCommunicatorError::Other),
                        }
                    }
                    .boxed_local(),
                )
            })
            .collect()
    }

    fn send_commit<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(
        &'a Self::Node,
        LocalBoxFuture<'static, Result<(), Self::Error>>,
    )> {
        receivers
            .iter()
            .map(move |receiver| {
                let this = self.clone();
                let receiver_id = receiver.id();
                let log_entry = Arc::clone(&log_entry);

                (
                    receiver,
                    async move {
                        futures_timer::Delay::new(delay(&this.e2e_delay_ms_distr.get())).await;

                        if roll_for_failure(this.failure_rate.get()) {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        let response = {
                            let handlers = this.request_handlers.lock().unwrap();
                            let handler = match handlers.get(&receiver_id) {
                                Some(handler) => handler,
                                None => return Err(DirectCommunicatorError::Other),
                            };

                            handler.handle_commit(round_num, log_entry)
                        }
                        .await;

                        if roll_for_failure(this.failure_rate.get()) {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        response.map_err(|_| DirectCommunicatorError::Other)
                    }
                    .boxed_local(),
                )
            })
            .collect()
    }

    fn send_commit_by_id<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        log_entry_id: <Self::LogEntry as LogEntry>::Id,
    ) -> Vec<(
        &'a Self::Node,
        LocalBoxFuture<'static, Result<(), Self::Error>>,
    )> {
        receivers
            .iter()
            .map(move |receiver| {
                let this = self.clone();
                let receiver_id = receiver.id();

                (
                    receiver,
                    async move {
                        futures_timer::Delay::new(delay(&this.e2e_delay_ms_distr.get())).await;

                        if roll_for_failure(this.failure_rate.get()) {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        let response = {
                            let handlers = this.request_handlers.lock().unwrap();
                            let handler = match handlers.get(&receiver_id) {
                                Some(handler) => handler,
                                None => return Err(DirectCommunicatorError::Other),
                            };

                            handler.handle_commit_by_id(round_num, log_entry_id)
                        }
                        .await;

                        if roll_for_failure(this.failure_rate.get()) {
                            return Err(DirectCommunicatorError::Timeout);
                        }

                        response.map_err(|_| DirectCommunicatorError::Other)
                    }
                    .boxed_local(),
                )
            })
            .collect()
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
