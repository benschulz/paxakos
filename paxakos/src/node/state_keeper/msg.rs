use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::channel::oneshot;

use crate::error::{AcceptError, AffirmSnapshotError, CommitError, InstallSnapshotError};
use crate::error::{PrepareError, PrepareSnapshotError, ReadStaleError};
use crate::node::Snapshot;
use crate::state::{LogEntryIdOf, LogEntryOf, NodeOf, OutcomeOf, State};
use crate::{CoordNum, Promise, RoundNum};

use super::error::{AcquireRoundNumError, ClusterError};
use super::RoundNumReservation;

#[derive(Debug)]
pub enum Request<S: State, R: RoundNum, C: CoordNum> {
    PrepareSnapshot,
    AffirmSnapshot {
        snapshot: Snapshot<S, R, C>,
    },
    InstallSnapshot {
        snapshot: Snapshot<S, R, C>,
    },

    ReadStale,

    AwaitCommitOf {
        entry_id: LogEntryIdOf<S>,
    },

    AcquireRoundNum {
        range: RangeInclusive<R>,
    },

    AcceptedEntryOf {
        round_num: R,
    },

    Cluster {
        round_num: R,
    },

    ObservedCoordNum {
        coord_num: C,
    },
    HighestObservedCoordNum,

    PrepareEntry {
        round_num: R,
        coord_num: C,
    },

    AcceptEntry {
        round_num: R,
        coord_num: C,
        entry: Arc<LogEntryOf<S>>,
    },
    AcceptEntries {
        coord_num: C,
        entries: Vec<(R, Arc<LogEntryOf<S>>)>,
    },

    CommitEntry {
        round_num: R,
        coord_num: C,
        entry: Arc<LogEntryOf<S>>,
    },

    CommitEntryById {
        round_num: R,
        coord_num: C,
        entry_id: LogEntryIdOf<S>,
    },

    AssumeLeadership {
        round_num: R,
        coord_num: C,
    },

    ForceActive,

    Shutdown,
}

#[derive(Debug)]
pub enum Response<S: State, R: RoundNum, C: CoordNum> {
    PrepareSnapshot(Result<Snapshot<S, R, C>, PrepareSnapshotError>),
    AffirmSnapshot(Result<(), AffirmSnapshotError>),
    InstallSnapshot(Result<(), InstallSnapshotError>),

    ReadStale(Result<Arc<S>, ReadStaleError>),

    AwaitCommitOf(Result<oneshot::Receiver<(R, OutcomeOf<S>)>, !>),

    AcquireRoundNum(Result<RoundNumReservation<R>, AcquireRoundNumError>),

    AcceptedEntryOf(Result<Option<Arc<LogEntryOf<S>>>, !>),

    Cluster(Result<Vec<NodeOf<S>>, ClusterError<R>>),

    ObservedCoordNum(Result<(), !>),
    HighestObservedCoordNum(Result<C, !>),

    PrepareEntry(Result<Promise<R, C, LogEntryOf<S>>, PrepareError<S, C>>),

    AcceptEntry(Result<(), AcceptError<S, C>>),
    AcceptEntries(Result<usize, AcceptError<S, C>>),

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
