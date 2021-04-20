use std::sync::Arc;

use crate::communicator::{CoordNumOf, RoundNumOf};
use crate::node::{NodeStatus, Shutdown, Snapshot};
use crate::state::{EventOf, LogEntryOf, State};
use crate::{CoordNum, RoundNum};

pub type ShutdownEventFor<S> = ShutdownEvent<
    <S as Shutdown>::State,
    RoundNumOf<<S as Shutdown>::Communicator>,
    CoordNumOf<<S as Shutdown>::Communicator>,
>;

/// Emitted by `Node`'s [`poll_events`][crate::Node::poll_events] method.
#[non_exhaustive]
#[derive(Debug)]
pub enum Event<S: State, R: RoundNum> {
    Init {
        status: NodeStatus,
        state: Option<Arc<S>>,
    },

    StatusChange {
        old_status: NodeStatus,
        new_status: NodeStatus,
    },

    /// A snapshot was installed.
    Install { round: R, state: Arc<S> },

    /// An entry has been committed to the log.
    ///
    /// The event does not imply that the entry was applied to the shared state.
    Commit {
        /// The round for which `log_entry` was committed.
        round: R,

        /// The log entry which was committed.
        log_entry: Arc<LogEntryOf<S>>,
    },

    /// The next log entry was applied to the state.
    Apply {
        round: R,
        log_entry: Arc<LogEntryOf<S>>,
        result: EventOf<S>,
    },

    /// A log entry was queued, preceeding entries are still missing.
    ///
    /// Note: This event is emitted even when the queued entry is within the
    /// concurrency bound or if this node created the gap itself. The second
    /// case can arise when the leader tries to concurrently append multiple
    /// entries and abandons some of the earlier appends.
    Gaps(Vec<Gap<R>>),
}

#[derive(Clone, Debug)]
pub struct Gap<R: RoundNum> {
    /// The point in time when the gap appeared.
    pub since: std::time::Instant,

    /// The locations of the gap within the log.
    pub rounds: std::ops::Range<R>,
}

/// Emitted by `Shutdown`'s [`poll_shutdown`][crate::Shutdown::poll_shutdown]
/// method.
#[derive(Debug)]
pub enum ShutdownEvent<S: State, R: RoundNum, C: CoordNum> {
    Regular(Event<S, R>),
    #[non_exhaustive]
    Last {
        snapshot: Option<Snapshot<S, R, C>>,
    },
}
