use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::channel::oneshot;

use crate::communicator::{Communicator, CoordNumOf, RoundNumOf};
use crate::error::{AcceptError, AffirmSnapshotError, CommitError, InstallSnapshotError};
use crate::error::{PrepareError, PrepareSnapshotError, ReadStaleError};
use crate::node::Snapshot;
use crate::state::{LogEntryIdOf, LogEntryOf, NodeOf, OutcomeOf, State};
use crate::{Promise, RoundNum};

use super::error::{AcquireRoundNumError, ClusterError};
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

    PrepareEntry(Result<Promise<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<S>>, PrepareError<C>>),

    AcceptEntry(Result<(), AcceptError<C>>),
    AcceptEntries(Result<usize, AcceptError<C>>),

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
