use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::channel::oneshot;

use crate::error::AcceptError;
use crate::error::AffirmSnapshotError;
use crate::error::CommitError;
use crate::error::InstallSnapshotError;
use crate::error::PrepareError;
use crate::error::PrepareSnapshotError;
use crate::error::ReadStaleError;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryIdOf;
use crate::invocation::LogEntryOf;
use crate::invocation::NodeOf;
use crate::invocation::OutcomeOf;
use crate::invocation::PromiseFor;
use crate::invocation::RoundNumOf;
use crate::invocation::SnapshotFor;
use crate::invocation::StateOf;
use crate::invocation::YeaOf;
use crate::RoundNum;

use super::error::AcquireRoundNumError;
use super::error::ClusterError;
use super::RoundNumReservation;

#[derive(Debug)]
pub enum Request<I: Invocation> {
    PrepareSnapshot,
    AffirmSnapshot {
        snapshot: SnapshotFor<I>,
    },
    InstallSnapshot {
        snapshot: SnapshotFor<I>,
    },

    ReadStale,

    AwaitCommitOf {
        entry_id: LogEntryIdOf<I>,
    },

    AcquireRoundNum {
        range: RangeInclusive<RoundNumOf<I>>,
    },

    AcceptedEntryOf {
        round_num: RoundNumOf<I>,
    },

    Cluster {
        round_num: RoundNumOf<I>,
    },

    ObservedCoordNum {
        coord_num: CoordNumOf<I>,
    },
    HighestObservedCoordNum,

    PrepareEntry {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    },

    AcceptEntry {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: Arc<LogEntryOf<I>>,
    },
    AcceptEntries {
        coord_num: CoordNumOf<I>,
        entries: Vec<(RoundNumOf<I>, Arc<LogEntryOf<I>>)>,
    },

    CommitEntry {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry: Arc<LogEntryOf<I>>,
    },

    CommitEntryById {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        entry_id: LogEntryIdOf<I>,
    },

    AssumeLeadership {
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
    },

    ForceActive,

    Shutdown,
}

#[derive(Debug)]
pub enum Response<I: Invocation> {
    PrepareSnapshot(Result<SnapshotFor<I>, PrepareSnapshotError>),
    AffirmSnapshot(Result<(), AffirmSnapshotError>),
    InstallSnapshot(Result<(), InstallSnapshotError>),

    ReadStale(Result<Arc<StateOf<I>>, ReadStaleError>),

    AwaitCommitOf(Result<oneshot::Receiver<(RoundNumOf<I>, OutcomeOf<I>)>, !>),

    AcquireRoundNum(Result<RoundNumReservation<RoundNumOf<I>>, AcquireRoundNumError>),

    AcceptedEntryOf(Result<Option<Arc<LogEntryOf<I>>>, !>),

    Cluster(Result<Vec<NodeOf<I>>, ClusterError<RoundNumOf<I>>>),

    ObservedCoordNum(Result<(), !>),
    HighestObservedCoordNum(Result<CoordNumOf<I>, !>),

    PrepareEntry(Result<PromiseFor<I>, PrepareError<I>>),

    AcceptEntry(Result<YeaOf<I>, AcceptError<I>>),
    AcceptEntries(Result<(), AcceptError<I>>),

    CommitEntry(Result<(), CommitError<I>>),

    CommitEntryById(Result<(), CommitError<I>>),

    AssumeLeadership(Result<(), !>),

    ForceActive(Result<bool, !>),

    Shutdown(Result<(), !>),
}

#[derive(Debug)]
pub enum Release<R: RoundNum> {
    RoundNum(R),
}
