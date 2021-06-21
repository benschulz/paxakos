use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::channel::oneshot;

use crate::communicator::Communicator;
use crate::communicator::CoordNumOf;
use crate::communicator::PromiseFor;
use crate::communicator::RoundNumOf;
use crate::communicator::YeaOf;
use crate::error::AcceptError;
use crate::error::AffirmSnapshotError;
use crate::error::CommitError;
use crate::error::InstallSnapshotError;
use crate::error::PrepareError;
use crate::error::PrepareSnapshotError;
use crate::error::ReadStaleError;
use crate::node::Snapshot;
use crate::state::LogEntryIdOf;
use crate::state::LogEntryOf;
use crate::state::NodeOf;
use crate::state::OutcomeOf;
use crate::state::State;
use crate::RoundNum;

use super::error::AcquireRoundNumError;
use super::error::ClusterError;
use super::RoundNumReservation;

#[derive(Debug)]
pub enum Request<S: State, C: Communicator> {
    PrepareSnapshot,
    AffirmSnapshot {
        snapshot: Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
    },
    InstallSnapshot {
        snapshot: Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
    },

    ReadStale,

    AwaitCommitOf {
        entry_id: LogEntryIdOf<S>,
    },

    AcquireRoundNum {
        range: RangeInclusive<RoundNumOf<C>>,
    },

    AcceptedEntryOf {
        round_num: RoundNumOf<C>,
    },

    Cluster {
        round_num: RoundNumOf<C>,
    },

    ObservedCoordNum {
        coord_num: CoordNumOf<C>,
    },
    HighestObservedCoordNum,

    PrepareEntry {
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
    },

    AcceptEntry {
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry: Arc<LogEntryOf<S>>,
    },
    AcceptEntries {
        coord_num: CoordNumOf<C>,
        entries: Vec<(RoundNumOf<C>, Arc<LogEntryOf<S>>)>,
    },

    CommitEntry {
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry: Arc<LogEntryOf<S>>,
    },

    CommitEntryById {
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        entry_id: LogEntryIdOf<S>,
    },

    AssumeLeadership {
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
    },

    ForceActive,

    Shutdown,
}

#[derive(Debug)]
pub enum Response<S: State, C: Communicator> {
    PrepareSnapshot(Result<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>, PrepareSnapshotError>),
    AffirmSnapshot(Result<(), AffirmSnapshotError>),
    InstallSnapshot(Result<(), InstallSnapshotError>),

    ReadStale(Result<Arc<S>, ReadStaleError>),

    AwaitCommitOf(Result<oneshot::Receiver<(RoundNumOf<C>, OutcomeOf<S>)>, !>),

    AcquireRoundNum(Result<RoundNumReservation<RoundNumOf<C>>, AcquireRoundNumError>),

    AcceptedEntryOf(Result<Option<Arc<LogEntryOf<S>>>, !>),

    Cluster(Result<Vec<NodeOf<S>>, ClusterError<RoundNumOf<C>>>),

    ObservedCoordNum(Result<(), !>),
    HighestObservedCoordNum(Result<CoordNumOf<C>, !>),

    PrepareEntry(Result<PromiseFor<C>, PrepareError<C>>),

    AcceptEntry(Result<YeaOf<C>, AcceptError<C>>),
    AcceptEntries(Result<(), AcceptError<C>>),

    CommitEntry(Result<(), CommitError<S>>),

    CommitEntryById(Result<(), CommitError<S>>),

    AssumeLeadership(Result<(), !>),

    ForceActive(Result<bool, !>),

    Shutdown(Result<(), !>),
}

#[derive(Debug)]
pub enum Release<R: RoundNum> {
    RoundNum(R),
}
