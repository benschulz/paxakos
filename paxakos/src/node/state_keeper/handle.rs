use std::fmt::Debug;
use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::future::{FutureExt, TryFutureExt};
use futures::sink::SinkExt;

use crate::error::{AcceptError, AffirmSnapshotError, CommitError, Disoriented};
use crate::error::{InstallSnapshotError, PrepareError, PrepareSnapshotError, ReadStaleError};
use crate::node::{Commit, Snapshot};
use crate::state::{LogEntryIdOf, LogEntryOf, NodeOf, OutcomeOf, State};
use crate::{CoordNum, LogEntry, Promise, RoundNum};

use super::error::{AcquireRoundNumError, ClusterError, ShutDown};
use super::msg::{Request, Response};
use super::{ProofOfLife, RoundNumReservation};

#[derive(Clone, Debug)]
pub struct StateKeeperHandle<S: State, R: RoundNum, C: CoordNum> {
    sender: mpsc::Sender<super::RequestAndResponseSender<S, R, C>>,
}

impl<S: State, R: RoundNum, C: CoordNum> StateKeeperHandle<S, R, C> {
    pub(super) fn new(sender: mpsc::Sender<super::RequestAndResponseSender<S, R, C>>) -> Self {
        Self { sender }
    }

    pub fn prepare_snapshot(
        &self,
    ) -> impl Future<Output = Result<Snapshot<S, R, C>, PrepareSnapshotError>> {
        crate::dispatch_state_keeper_req!(self, PrepareSnapshot)
    }

    pub fn affirm_snapshot(
        &self,
        snapshot: Snapshot<S, R, C>,
    ) -> impl Future<Output = Result<(), AffirmSnapshotError>> {
        crate::dispatch_state_keeper_req!(self, AffirmSnapshot, { snapshot })
    }

    pub fn install_snapshot(
        &self,
        snapshot: Snapshot<S, R, C>,
    ) -> impl Future<Output = Result<(), InstallSnapshotError>> {
        crate::dispatch_state_keeper_req!(self, InstallSnapshot, { snapshot })
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
        crate::dispatch_state_keeper_req!(self, ReadStale)
    }

    pub fn await_commit_of(
        &self,
        entry_id: LogEntryIdOf<S>,
    ) -> impl Future<Output = Result<oneshot::Receiver<(R, OutcomeOf<S>)>, ShutDown>> {
        crate::dispatch_state_keeper_req!(self, AwaitCommitOf, { entry_id })
    }

    pub fn reserve_round_num(
        &self,
        range: RangeInclusive<R>,
    ) -> impl Future<Output = Result<RoundNumReservation<R>, AcquireRoundNumError>> {
        crate::dispatch_state_keeper_req!(self, AcquireRoundNum, { range })
    }

    pub fn accepted_entry_of(
        &self,
        round_num: R,
    ) -> impl Future<Output = Result<Option<Arc<LogEntryOf<S>>>, ShutDown>> {
        crate::dispatch_state_keeper_req!(self, AcceptedEntryOf, { round_num })
    }

    /// Returns the nodes that make up the cluster for the given instance of
    /// Multi Paxos.
    ///
    /// The nodes may be returned in "arbitrary" order. However, the order must
    /// be consistent for the same round number, across the whole network.
    pub fn cluster_for(
        &self,
        round_num: R,
    ) -> impl Future<Output = Result<Vec<NodeOf<S>>, ClusterError<R>>> {
        crate::dispatch_state_keeper_req!(self, Cluster, { round_num })
    }

    pub fn observe_coord_num(&self, coord_num: C) -> impl Future<Output = Result<(), ShutDown>> {
        crate::dispatch_state_keeper_req!(self, ObservedCoordNum, { coord_num })
    }

    pub fn highest_observed_coord_num(&self) -> impl Future<Output = Result<C, ShutDown>> {
        crate::dispatch_state_keeper_req!(self, HighestObservedCoordNum)
    }

    pub fn prepare_entry(
        &self,
        round_num: R,
        coord_num: C,
    ) -> impl Future<Output = Result<Promise<R, C, LogEntryOf<S>>, PrepareError<S, C>>> {
        self.handle_prepare(round_num, coord_num)
    }

    pub fn handle_prepare(
        &self,
        round_num: R,
        coord_num: C,
    ) -> impl Future<Output = Result<Promise<R, C, LogEntryOf<S>>, PrepareError<S, C>>> {
        crate::dispatch_state_keeper_req!(self, PrepareEntry, {
            round_num,
            coord_num,
        })
    }

    pub fn accept_entry(
        &self,
        round_num: R,
        coord_num: C,
        entry: Arc<LogEntryOf<S>>,
    ) -> impl Future<Output = Result<(), AcceptError<S, C>>> {
        crate::dispatch_state_keeper_req!(self, AcceptEntry, { round_num, coord_num, entry })
    }

    pub fn handle_proposal(
        &self,
        round_num: R,
        coord_num: C,
        entry: impl Into<Arc<LogEntryOf<S>>>,
    ) -> impl Future<Output = Result<(), AcceptError<S, C>>> {
        let entry = entry.into();
        crate::dispatch_state_keeper_req!(self, AcceptEntry, { round_num, coord_num, entry })
    }

    pub fn accept_entries(
        &self,
        coord_num: C,
        entries: Vec<(R, Arc<LogEntryOf<S>>)>,
    ) -> impl Future<Output = Result<usize, AcceptError<S, C>>> {
        crate::dispatch_state_keeper_req!(self, AcceptEntries, { coord_num, entries })
    }

    pub fn commit_entry(
        &self,
        round_num: R,
        coord_num: C,
        entry: Arc<LogEntryOf<S>>,
    ) -> impl Future<Output = Result<Commit<S, R>, CommitError<S>>> {
        let recv = self.await_commit_of(entry.id());
        let commit = self.handle_commit(round_num, coord_num, entry);

        async move {
            let recv = recv.await?;

            commit.await.map(|_| Commit::new(round_num, recv))
        }
    }

    pub fn handle_commit(
        &self,
        round_num: R,
        coord_num: C,
        entry: impl Into<Arc<LogEntryOf<S>>>,
    ) -> impl Future<Output = Result<(), CommitError<S>>> {
        crate::dispatch_state_keeper_req!(self, CommitEntry, {
            round_num,
            coord_num,
            entry: entry.into(),
        })
    }

    pub fn handle_commit_by_id(
        &self,
        round_num: R,
        coord_num: C,
        entry_id: <LogEntryOf<S> as LogEntry>::Id,
    ) -> impl Future<Output = Result<(), CommitError<S>>> {
        crate::dispatch_state_keeper_req!(self, CommitEntryById, {
            round_num,
            coord_num,
            entry_id,
        })
    }

    pub fn assume_leadership(
        &self,
        round_num: R,
        coord_num: C,
    ) -> impl Future<Output = Result<(), ShutDown>> {
        crate::dispatch_state_keeper_req!(
            self,
            AssumeLeadership,
            {
                round_num,
                coord_num,
            }
        )
    }

    pub fn force_active(&self) -> impl Future<Output = Result<bool, ()>> {
        crate::dispatch_state_keeper_req!(self, ForceActive).map_err(ShutDown::into_unit::<bool>)
    }

    pub fn shut_down(&self, _proof_of_life: ProofOfLife) -> impl Future<Output = ()> {
        crate::dispatch_state_keeper_req!(self, Shutdown).map(ShutDown::rule_out)
    }
}
