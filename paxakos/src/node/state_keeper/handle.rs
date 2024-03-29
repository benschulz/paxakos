use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::task::Poll;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::FutureExt;
use futures::sink::SinkExt;
use pin_project::pin_project;

use crate::error::AcceptError;
use crate::error::AffirmSnapshotError;
use crate::error::CommitError;
use crate::error::Disoriented;
use crate::error::InstallSnapshotError;
use crate::error::PrepareError;
use crate::error::ReadStaleError;
use crate::invocation::CommitFor;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryIdOf;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;
use crate::invocation::NodeOf;
use crate::invocation::OutcomeOf;
use crate::invocation::PromiseFor;
use crate::invocation::RoundNumOf;
use crate::invocation::SnapshotFor;
use crate::invocation::StateOf;
use crate::invocation::YeaOf;
use crate::node::state_keeper::msg::ReadStaleFunc;
use crate::node::Commit;
use crate::LogEntry;

use super::error::AcquireRoundNumError;
use super::error::ClusterError;
use super::error::ShutDown;
use super::msg::DynReadFunc;
use super::msg::Request;
use super::msg::Response;
use super::ProofOfLife;
use super::RoundNumReservation;

#[doc(hidden)]
macro_rules! dispatch_state_keeper_req {
    ($self:ident, $name:ident) => {{
        let req = Request::$name;

        let (s, r) = oneshot::channel();

        let mut sender = $self.sender.clone();

        async move {
            sender.send((req, s)).await.map_err(|_| ShutDown)?;

            match r.await.expect("request not handled: state keeper task panicked") {
                Response::$name(r) => Ok(match r {
                    Ok(v) => v,
                    #[allow(unreachable_code)]
                    Err(e) => return Err(e.into()),
                }),
                _ => unreachable!(),
            }
        }
    }};

    ($self:ident, $name:ident, $args:tt) => {{
        let req = Request::$name $args;

        let (s, r) = oneshot::channel();

        let mut sender = $self.sender.clone();

        async move {
            sender.send((req, s)).await.map_err(|_| ShutDown)?;

            match r.await.map_err(|_| ShutDown)? {
                Response::$name(r) => Ok(match r {
                    Ok(v) => v,
                    #[allow(unreachable_code)]
                    Err(e) => return Err(e.into()),
                }),
                _ => unreachable!(),
            }
        }
    }};
}

#[derive(Debug)]
pub struct StateKeeperHandle<I: Invocation> {
    sender: mpsc::Sender<super::RequestAndResponseSender<I>>,
}

impl<I: Invocation> Clone for StateKeeperHandle<I> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<I: Invocation> StateKeeperHandle<I> {
    pub(super) fn new(sender: mpsc::Sender<super::RequestAndResponseSender<I>>) -> Self {
        Self { sender }
    }

    pub fn prepare_snapshot(&self) -> impl Future<Output = Result<SnapshotFor<I>, ShutDown>> {
        dispatch_state_keeper_req!(self, PrepareSnapshot)
    }

    pub fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<I>,
    ) -> impl Future<Output = Result<(), AffirmSnapshotError>> {
        dispatch_state_keeper_req!(self, AffirmSnapshot, { snapshot })
    }

    pub fn install_snapshot(
        &self,
        snapshot: SnapshotFor<I>,
    ) -> impl Future<Output = Result<(), InstallSnapshotError>> {
        dispatch_state_keeper_req!(self, InstallSnapshot, { snapshot })
    }

    pub fn read_stale<F, T>(
        &self,
        proof_of_life: &ProofOfLife,
        f: F,
    ) -> impl Future<Output = Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<I>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.read_stale_infallibly(proof_of_life, |r| r.map(f))
    }

    pub fn read_stale_infallibly<F, T>(
        &self,
        _proof_of_life: &ProofOfLife,
        f: F,
    ) -> impl Future<Output = T>
    where
        F: FnOnce(Result<&StateOf<I>, crate::error::Disoriented>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.try_read_stale_infallibly(|r| {
            f(r.map_err(|err| match err {
                ReadStaleError::Disoriented => crate::error::Disoriented,
                ReadStaleError::ShutDown => {
                    panic!("proof of life given: state keeper task panicked")
                }
            }))
        })
    }

    pub fn try_read_stale<F, T>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<T, crate::error::ReadStaleError>>
    where
        F: FnOnce(&StateOf<I>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.try_read_stale_infallibly(|r| r.map(f))
    }

    pub fn try_read_stale_infallibly<F, T>(&self, f: F) -> impl Future<Output = T>
    where
        F: FnOnce(Result<&StateOf<I>, crate::error::ReadStaleError>) -> T + Send + 'static,
        T: Send + 'static,
    {
        let f: DynReadFunc<'static, I> = Box::new(|r| Box::new(f(r)) as Box<dyn Any + Send>);
        let f = Arc::new(std::sync::Mutex::new(Some(f)));

        let req = Request::ReadStale {
            f: ReadStaleFunc(f),
        };

        let (s, r) = oneshot::channel();

        let mut sender = self.sender.clone();

        let result = async move {
            TrySend::new(&mut sender, (req, s))
                .await
                .map_err(|(req, _)| Some(req))?;

            match r
                .await
                .expect("request not handled: state keeper task panicked")
            {
                Response::ReadStale(r) => r.map_err(|_| None),
                _ => unreachable!(),
            }
        };

        async move {
            let t = result
                .await
                .map_err(|f| {
                    let f = f.expect("this future was dropped");
                    let f = match f {
                        Request::ReadStale { f } => f.0,
                        _ => unreachable!(),
                    };
                    let f = Arc::try_unwrap(f).ok().unwrap();
                    let f = f.into_inner().unwrap().unwrap();

                    f(Err(ReadStaleError::ShutDown))
                })
                .unwrap_or_else(|t| t);

            *t.downcast::<T>().expect("downcast failed")
        }
    }

    pub fn read_stale_scoped<'read, F, T>(
        &self,
        proof_of_life: &ProofOfLife,
        f: F,
    ) -> impl Future<Output = Result<T, Disoriented>> + 'read
    where
        F: FnOnce(&StateOf<I>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.read_stale_scoped_infallibly(proof_of_life, |r| r.map(f))
    }

    pub fn read_stale_scoped_infallibly<'read, F, T>(
        &self,
        _proof_of_life: &ProofOfLife,
        f: F,
    ) -> impl Future<Output = T> + 'read
    where
        F: FnOnce(Result<&StateOf<I>, crate::error::Disoriented>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.try_read_stale_scoped_infallibly(|r| {
            f(r.map_err(|err| match err {
                ReadStaleError::Disoriented => crate::error::Disoriented,
                ReadStaleError::ShutDown => {
                    panic!("proof of life given: state keeper task panicked")
                }
            }))
        })
    }

    pub fn try_read_stale_scoped<'read, F, T>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<T, crate::error::ReadStaleError>> + 'read
    where
        F: FnOnce(&StateOf<I>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.try_read_stale_scoped_infallibly(|r| r.map(f))
    }

    pub fn try_read_stale_scoped_infallibly<'read, F, T>(
        &self,
        f: F,
    ) -> impl Future<Output = T> + 'read
    where
        F: FnOnce(Result<&StateOf<I>, crate::error::ReadStaleError>) -> T + Send + 'read,
        T: Send + 'static,
    {
        #[pin_project]
        struct TakeOnDrop<T, F>(TakeOnDropInner<T>, #[pin] F);

        struct TakeOnDropInner<T>(Arc<std::sync::Mutex<Option<T>>>);

        impl<T, F: Future> Future for TakeOnDrop<T, F> {
            type Output = F::Output;

            fn poll(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> Poll<Self::Output> {
                self.project().1.poll(cx)
            }
        }

        impl<T> Drop for TakeOnDropInner<T> {
            fn drop(&mut self) {
                // If the lock was poisoned, the read was already (attempted to be) evaluated.
                // => Nothing left to do.
                if let Ok(mut f) = self.0.lock() {
                    f.take();
                }
            }
        }

        let f: DynReadFunc<'read, I> = Box::new(|s| Box::new(f(s)) as Box<dyn Any + Send>);
        let f = unsafe { std::mem::transmute::<DynReadFunc<'read, I>, DynReadFunc<'static, I>>(f) };
        let f = Arc::new(std::sync::Mutex::new(Some(f)));

        let req = Request::ReadStale {
            f: ReadStaleFunc(Arc::clone(&f)),
        };

        let (s, r) = oneshot::channel();

        let mut sender = self.sender.clone();

        let result = async move {
            TrySend::new(&mut sender, (req, s))
                .await
                .map_err(|(req, _)| Some(req))?;

            match r
                .await
                .expect("request not handled: state keeper task panicked")
            {
                Response::ReadStale(r) => r.map_err(|_| None),
                _ => unreachable!(),
            }
        };

        TakeOnDrop(TakeOnDropInner(f), async move {
            let t = result
                .await
                .map_err(|f| {
                    let f = f.expect("this future was dropped");
                    let f = match f {
                        Request::ReadStale { f } => f.0,
                        _ => unreachable!(),
                    };
                    let f = Arc::try_unwrap(f).ok().unwrap();
                    let f = f.into_inner().unwrap().unwrap();

                    f(Err(ReadStaleError::ShutDown))
                })
                .unwrap_or_else(|t| t);

            *t.downcast::<T>().expect("downcast failed")
        })
    }

    pub fn await_commit_of(
        &self,
        entry_id: LogEntryIdOf<I>,
    ) -> impl Future<Output = Result<oneshot::Receiver<(RoundNumOf<I>, OutcomeOf<I>)>, ShutDown>>
    {
        dispatch_state_keeper_req!(self, AwaitCommitOf, { entry_id })
    }

    pub fn reserve_round_num(
        &self,
        range: RangeInclusive<RoundNumOf<I>>,
    ) -> impl Future<Output = Result<RoundNumReservation<RoundNumOf<I>>, AcquireRoundNumError>>
    {
        dispatch_state_keeper_req!(self, AcquireRoundNum, { range })
    }

    pub fn accepted_entry_of(
        &self,
        round_num: RoundNumOf<I>,
    ) -> impl Future<Output = Result<Option<Arc<LogEntryOf<I>>>, ShutDown>> {
        dispatch_state_keeper_req!(self, AcceptedEntryOf, { round_num })
    }

    /// Returns the nodes that make up the cluster for the given instance of
    /// Multi Paxos.
    ///
    /// The nodes may be returned in "arbitrary" order. However, the order must
    /// be consistent for the same round number, across the whole network.
    pub fn cluster_for(
        &self,
        round_num: RoundNumOf<I>,
    ) -> impl Future<Output = Result<Vec<NodeOf<I>>, ClusterError<RoundNumOf<I>>>> + Send + 'static
    {
        dispatch_state_keeper_req!(self, Cluster, { round_num })
    }

    pub fn observe_coord_num(
        &self,
        coord_num: CoordNumOf<I>,
    ) -> impl Future<Output = Result<(), ShutDown>> {
        dispatch_state_keeper_req!(self, ObservedCoordNum, { coord_num })
    }

    pub fn greatest_observed_coord_num(
        &self,
    ) -> impl Future<Output = Result<CoordNumOf<I>, ShutDown>> {
        dispatch_state_keeper_req!(self, GreatestObservedCoordNum)
    }

    pub fn prepare_entry(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    ) -> impl Future<Output = Result<PromiseFor<I>, PrepareError<I>>> {
        self.handle_prepare(round_num, coord_num)
    }

    pub fn handle_prepare(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    ) -> impl Future<Output = Result<PromiseFor<I>, PrepareError<I>>> {
        dispatch_state_keeper_req!(self, PrepareEntry, {
            round_num,
            coord_num,
        })
    }

    pub fn accept_entry(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: Arc<LogEntryOf<I>>,
    ) -> impl Future<Output = Result<YeaOf<I>, AcceptError<I>>> {
        dispatch_state_keeper_req!(self, AcceptEntry, { round_num, coord_num, entry })
    }

    pub fn handle_proposal(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: impl Into<Arc<LogEntryOf<I>>>,
    ) -> impl Future<Output = Result<YeaOf<I>, AcceptError<I>>> {
        let entry = entry.into();
        dispatch_state_keeper_req!(self, AcceptEntry, { round_num, coord_num, entry })
    }

    pub fn test_acceptability_of_entries(
        &self,
        coord_num: CoordNumOf<I>,
        entries: Vec<(RoundNumOf<I>, Arc<LogEntryOf<I>>)>,
    ) -> impl Future<Output = Result<Option<NayOf<I>>, ShutDown>> {
        dispatch_state_keeper_req!(self, TestAcceptabilityOfEntries, { coord_num, entries })
    }

    pub fn accept_entries(
        &self,
        coord_num: CoordNumOf<I>,
        entries: Vec<(RoundNumOf<I>, Arc<LogEntryOf<I>>)>,
    ) -> impl Future<Output = Result<(), AcceptError<I>>> {
        dispatch_state_keeper_req!(self, AcceptEntries, { coord_num, entries })
    }

    pub fn commit_entry(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: Arc<LogEntryOf<I>>,
    ) -> impl Future<Output = Result<CommitFor<I>, CommitError<I>>> {
        let recv = self.await_commit_of(entry.id());
        let commit = self.handle_commit(round_num, coord_num, entry);

        async move {
            let recv = recv.await?;

            commit.await.map(|_| Commit::new(round_num, recv))
        }
    }

    pub fn handle_commit(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: impl Into<Arc<LogEntryOf<I>>>,
    ) -> impl Future<Output = Result<(), CommitError<I>>> {
        dispatch_state_keeper_req!(self, CommitEntry, {
            round_num,
            coord_num,
            entry: entry.into(),
        })
    }

    pub fn handle_commit_by_id(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry_id: <LogEntryOf<I> as LogEntry>::Id,
    ) -> impl Future<Output = Result<(), CommitError<I>>> {
        dispatch_state_keeper_req!(self, CommitEntryById, {
            round_num,
            coord_num,
            entry_id,
        })
    }

    pub fn assume_leadership(
        &self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    ) -> impl Future<Output = Result<(), ShutDown>> {
        dispatch_state_keeper_req!(
            self,
            AssumeLeadership,
            {
                round_num,
                coord_num,
            }
        )
    }

    pub fn eject(
        &self,
        reason: crate::invocation::EjectionOf<I>,
    ) -> impl Future<Output = Result<bool, ShutDown>> {
        dispatch_state_keeper_req!(self, Eject, (reason))
    }

    pub fn force_active(&self) -> impl Future<Output = Result<bool, ShutDown>> {
        dispatch_state_keeper_req!(self, ForceActive)
    }

    pub fn shut_down(&self, _proof_of_life: ProofOfLife) -> impl Future<Output = ()> {
        dispatch_state_keeper_req!(self, Shutdown).map(ShutDown::rule_out)
    }
}

struct TrySend<'a, T> {
    sender: &'a mut mpsc::Sender<T>,
    message: Option<T>,
}

impl<'a, T> Unpin for TrySend<'a, T> {}

impl<'a, T> TrySend<'a, T> {
    pub fn new(sender: &'a mut mpsc::Sender<T>, message: T) -> Self {
        Self {
            sender,
            message: Some(message),
        }
    }

    fn take_message(&mut self) -> T {
        self.message
            .take()
            .expect("polled TrySend after completion")
    }
}

impl<'a, T> Future for TrySend<'a, T> {
    type Output = Result<(), T>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let message = self.take_message();

                match self.sender.try_send(message) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(err) => {
                        if err.is_disconnected() {
                            Poll::Ready(Err(err.into_inner()))
                        } else if err.is_full() {
                            // we were promised a slot in the queue!
                            unreachable!()
                        } else {
                            unreachable!("unexpected branch")
                        }
                    }
                }
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(self.take_message())),
            Poll::Pending => Poll::Pending,
        }
    }
}
