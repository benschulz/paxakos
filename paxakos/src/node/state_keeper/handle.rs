use std::fmt::Debug;
use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::FutureExt;
use futures::sink::SinkExt;

use crate::communicator::Communicator;
use crate::communicator::CoordNumOf;
use crate::communicator::PromiseFor;
use crate::communicator::RoundNumOf;
use crate::error::AcceptError;
use crate::error::AffirmSnapshotError;
use crate::error::CommitError;
use crate::error::Disoriented;
use crate::error::InstallSnapshotError;
use crate::error::PrepareError;
use crate::error::PrepareSnapshotError;
use crate::error::ReadStaleError;
use crate::node::Commit;
use crate::node::Snapshot;
use crate::state::LogEntryIdOf;
use crate::state::LogEntryOf;
use crate::state::NodeOf;
use crate::state::OutcomeOf;
use crate::state::State;
use crate::LogEntry;

use super::error::AcquireRoundNumError;
use super::error::ClusterError;
use super::error::ShutDown;
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

            match r.await.map_err(|_| ShutDown)? {
                Response::$name(r) => Ok(r?),
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
                Response::$name(r) => Ok(r?),
                _ => unreachable!(),
            }
        }
    }};
}

#[derive(Debug)]
pub struct StateKeeperHandle<S: State, C: Communicator> {
    sender: mpsc::Sender<super::RequestAndResponseSender<S, C>>,
}

impl<S, C> Clone for StateKeeperHandle<S, C>
where
    S: State,
    C: Communicator,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<S: State, C: Communicator> StateKeeperHandle<S, C> {
    pub(super) fn new(sender: mpsc::Sender<super::RequestAndResponseSender<S, C>>) -> Self {
        Self { sender }
    }

    pub fn prepare_snapshot(
        &self,
    ) -> impl Future<Output = Result<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>, PrepareSnapshotError>>
    {
        dispatch_state_keeper_req!(self, PrepareSnapshot)
    }

    pub fn affirm_snapshot(
        &self,
        snapshot: Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
    ) -> impl Future<Output = Result<(), AffirmSnapshotError>> {
        dispatch_state_keeper_req!(self, AffirmSnapshot, { snapshot })
    }

    pub fn install_snapshot(
        &self,
        snapshot: Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
    ) -> impl Future<Output = Result<(), InstallSnapshotError>> {
        dispatch_state_keeper_req!(self, InstallSnapshot, { snapshot })
    }

    pub fn read_stale(
        &self,
        _proof_of_life: &ProofOfLife,
    ) -> impl Future<Output = Result<Arc<S>, Disoriented>> {
        self.try_read_stale().map(|r| {
            r.map_err(|e| match e {
                ReadStaleError::ShutDown => unreachable!("proof of life given"),
                ReadStaleError::Disoriented => Disoriented,
            })
        })
    }

    pub fn try_read_stale(&self) -> impl Future<Output = Result<Arc<S>, ReadStaleError>> {
        dispatch_state_keeper_req!(self, ReadStale)
    }

    pub fn await_commit_of(
        &self,
        entry_id: LogEntryIdOf<S>,
    ) -> impl Future<Output = Result<oneshot::Receiver<(RoundNumOf<C>, OutcomeOf<S>)>, ShutDown>>
    {
        dispatch_state_keeper_req!(self, AwaitCommitOf, { entry_id })
    }

    pub fn reserve_round_num(
        &self,
        range: RangeInclusive<RoundNumOf<C>>,
    ) -> impl Future<Output = Result<RoundNumReservation<RoundNumOf<C>>, AcquireRoundNumError>>
    {
        dispatch_state_keeper_req!(self, AcquireRoundNum, { range })
    }

    pub fn accepted_entry_of(
        &self,
        round_num: RoundNumOf<C>,
    ) -> impl Future<Output = Result<Option<Arc<LogEntryOf<S>>>, ShutDown>> {
        dispatch_state_keeper_req!(self, AcceptedEntryOf, { round_num })
    }

    /// Returns the nodes that make up the cluster for the given instance of
    /// Multi Paxos.
    ///
    /// The nodes may be returned in "arbitrary" order. However, the order must
    /// be consistent for the same round number, across the whole network.
    pub fn cluster_for(
        &self,
        round_num: RoundNumOf<C>,
    ) -> impl Future<Output = Result<Vec<NodeOf<S>>, ClusterError<RoundNumOf<C>>>> {
        dispatch_state_keeper_req!(self, Cluster, { round_num })
    }

    pub fn observe_coord_num(
        &self,
        coord_num: CoordNumOf<C>,
    ) -> impl Future<Output = Result<(), ShutDown>> {
        dispatch_state_keeper_req!(self, ObservedCoordNum, { coord_num })
    }

    pub fn highest_observed_coord_num(
        &self,
    ) -> impl Future<Output = Result<CoordNumOf<C>, ShutDown>> {
        dispatch_state_keeper_req!(self, HighestObservedCoordNum)
    }

    pub fn prepare_entry(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
    ) -> impl Future<Output = Result<PromiseFor<C>, PrepareError<C>>> {
        self.handle_prepare(round_num, coord_num)
    }

    pub fn handle_prepare(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
    ) -> impl Future<Output = Result<PromiseFor<C>, PrepareError<C>>> {
        dispatch_state_keeper_req!(self, PrepareEntry, {
            round_num,
            coord_num,
        })
    }

    pub fn accept_entry(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry: Arc<LogEntryOf<S>>,
    ) -> impl Future<Output = Result<(), AcceptError<C>>> {
        dispatch_state_keeper_req!(self, AcceptEntry, { round_num, coord_num, entry })
    }

    pub fn handle_proposal(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry: impl Into<Arc<LogEntryOf<S>>>,
    ) -> impl Future<Output = Result<(), AcceptError<C>>> {
        let entry = entry.into();
        dispatch_state_keeper_req!(self, AcceptEntry, { round_num, coord_num, entry })
    }

    pub fn accept_entries(
        &self,
        coord_num: CoordNumOf<C>,
        entries: Vec<(RoundNumOf<C>, Arc<LogEntryOf<S>>)>,
    ) -> impl Future<Output = Result<usize, AcceptError<C>>> {
        dispatch_state_keeper_req!(self, AcceptEntries, { coord_num, entries })
    }

    pub fn commit_entry(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry: Arc<LogEntryOf<S>>,
    ) -> impl Future<Output = Result<Commit<S, RoundNumOf<C>>, CommitError<S>>> {
        let recv = self.await_commit_of(entry.id());
        let commit = self.handle_commit(round_num, coord_num, entry);

        async move {
            let recv = recv.await?;

            commit.await.map(|_| Commit::new(round_num, recv))
        }
    }

    pub fn handle_commit(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry: impl Into<Arc<LogEntryOf<S>>>,
    ) -> impl Future<Output = Result<(), CommitError<S>>> {
        dispatch_state_keeper_req!(self, CommitEntry, {
            round_num,
            coord_num,
            entry: entry.into(),
        })
    }

    pub fn handle_commit_by_id(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry_id: <LogEntryOf<S> as LogEntry>::Id,
    ) -> impl Future<Output = Result<(), CommitError<S>>> {
        dispatch_state_keeper_req!(self, CommitEntryById, {
            round_num,
            coord_num,
            entry_id,
        })
    }

    pub fn assume_leadership(
        &self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
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

    pub fn force_active(&self) -> impl Future<Output = Result<bool, ShutDown>> {
        dispatch_state_keeper_req!(self, ForceActive)
    }

    pub fn shut_down(&self, _proof_of_life: ProofOfLife) -> impl Future<Output = ()> {
        dispatch_state_keeper_req!(self, Shutdown).map(ShutDown::rule_out)
    }
}
