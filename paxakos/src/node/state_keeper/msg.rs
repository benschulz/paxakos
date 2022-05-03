use std::any::Any;
use std::convert::Infallible;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::channel::oneshot;

use crate::error::AcceptError;
use crate::error::AffirmSnapshotError;
use crate::error::CommitError;
use crate::error::InstallSnapshotError;
use crate::error::PrepareError;
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

    ReadStale {
        f: ReadStaleFunc<I>,
    },

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
    GreatestObservedCoordNum,

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

pub struct ReadStaleFunc<I: Invocation>(
    pub Box<dyn FnOnce(&StateOf<I>) -> Box<dyn Any + Send> + Send>,
);

impl<I: Invocation> std::fmt::Debug for ReadStaleFunc<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ReadStaleFunc(..)").finish()
    }
}

type PendingCommit<I> = oneshot::Receiver<(RoundNumOf<I>, OutcomeOf<I>)>;

#[derive(Debug)]
pub enum Response<I: Invocation> {
    PrepareSnapshot(Result<SnapshotFor<I>, Infallible>),
    AffirmSnapshot(Result<(), AffirmSnapshotError>),
    InstallSnapshot(Result<(), InstallSnapshotError>),

    ReadStale(Result<Box<dyn Any + Send>, ReadStaleError>),

    AwaitCommitOf(Result<PendingCommit<I>, Infallible>),

    AcquireRoundNum(Result<RoundNumReservation<RoundNumOf<I>>, AcquireRoundNumError>),

    AcceptedEntryOf(Result<Option<Arc<LogEntryOf<I>>>, Infallible>),

    Cluster(Result<Vec<NodeOf<I>>, ClusterError<RoundNumOf<I>>>),

    ObservedCoordNum(Result<(), Infallible>),
    GreatestObservedCoordNum(Result<CoordNumOf<I>, Infallible>),

    PrepareEntry(Result<PromiseFor<I>, PrepareError<I>>),

    AcceptEntry(Result<YeaOf<I>, AcceptError<I>>),
    AcceptEntries(Result<(), AcceptError<I>>),

    CommitEntry(Result<(), CommitError<I>>),

    CommitEntryById(Result<(), CommitError<I>>),

    AssumeLeadership(Result<(), Infallible>),

    ForceActive(Result<bool, Infallible>),

    Shutdown(Result<(), Infallible>),
}

#[derive(Debug)]
pub enum Release<R: RoundNum> {
    RoundNum(R),
}
