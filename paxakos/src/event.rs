use std::sync::Arc;
use std::time::Instant;

use crate::communicator::{Communicator, CoordNumOf, RoundNumOf};
use crate::node::{NodeStatus, Shutdown, Snapshot};
use crate::state::{EventOf, LogEntryOf, NodeOf, State};
use crate::RoundNum;

pub type ShutdownEventFor<S> = ShutdownEvent<<S as Shutdown>::State, <S as Shutdown>::Communicator>;

/// Emitted by `Node`'s [`poll_events`][crate::Node::poll_events] method.
#[non_exhaustive]
pub enum Event<S: State, C: Communicator> {
    Init {
        status: NodeStatus,
        round: RoundNumOf<C>,
        state: Option<Arc<S>>,
    },

    StatusChange {
        old_status: NodeStatus,
        new_status: NodeStatus,
    },

    /// The node is transitioning to [partially active
    /// participation][crate::node::Participation].
    Activate(RoundNumOf<C>),

    /// A snapshot was installed.
    Install { round: RoundNumOf<C>, state: Arc<S> },

    /// An entry has been committed to the log.
    ///
    /// The event does not imply that the entry was applied to the shared state.
    Commit {
        /// The round for which `log_entry` was committed.
        round: RoundNumOf<C>,

        /// The log entry which was committed.
        log_entry: Arc<LogEntryOf<S>>,
    },

    /// The next log entry was applied to the state.
    Apply {
        round: RoundNumOf<C>,
        log_entry: Arc<LogEntryOf<S>>,
        result: EventOf<S>,
    },

    /// A log entry was queued, preceeding entries are still missing.
    ///
    /// Note: This event is emitted even when the queued entry is within the
    /// concurrency bound or if this node created the gap itself. The second
    /// case can arise when the leader tries to concurrently append multiple
    /// entries and abandons some of the earlier appends.
    Gaps(Vec<Gap<RoundNumOf<C>>>),

    /// This node received a (potentially indirect) directive for the given
    /// round and from the given node. The node used the mandate obtained with
    /// the given coordination number to issue the directive.
    ///
    /// This event is not emitted when this node is disoriented or lagging.
    Directive {
        leader: NodeOf<S>,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        timestamp: Instant,
    },
}

impl<S: State, C: Communicator> std::fmt::Debug for Event<S, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::Init {
                status,
                round,
                state,
            } => f
                .debug_struct("Event::Init")
                .field("status", status)
                .field("round", round)
                .field("state", state)
                .finish(),
            Event::StatusChange {
                old_status,
                new_status,
            } => f
                .debug_struct("Event::StatusChange")
                .field("old_status", old_status)
                .field("new_status", new_status)
                .finish(),
            Event::Activate(round) => f.debug_tuple("Event::Activate").field(round).finish(),
            Event::Install { round, state } => f
                .debug_struct("Event::Install")
                .field("round", round)
                .field("state", state)
                .finish(),
            Event::Commit { round, log_entry } => f
                .debug_struct("Event::Commit")
                .field("round", round)
                .field("log_entry", log_entry)
                .finish(),
            Event::Apply {
                round,
                log_entry,
                result,
            } => f
                .debug_struct("Event::Apply")
                .field("round", round)
                .field("log_entry", log_entry)
                .field("result", result)
                .finish(),
            Event::Gaps(gaps) => f.debug_tuple("Event::Gaps").field(gaps).finish(),
            Event::Directive {
                leader,
                round_num,
                coord_num,
                timestamp,
            } => f
                .debug_struct("Event::Directive")
                .field("leader", leader)
                .field("round_num", round_num)
                .field("coord_num", coord_num)
                .field("timestamp", timestamp)
                .finish(),
        }
    }
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
pub enum ShutdownEvent<S: State, C: Communicator> {
    Regular(Event<S, C>),
    #[non_exhaustive]
    Last {
        snapshot: Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>,
    },
}

impl<S: State, C: Communicator> From<Event<S, C>> for ShutdownEvent<S, C> {
    fn from(e: Event<S, C>) -> Self {
        ShutdownEvent::Regular(e)
    }
}

impl<S: State, C: Communicator> std::fmt::Debug for ShutdownEvent<S, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownEvent::Regular(e) => f.debug_tuple("ShutdownEvent::Regular").field(e).finish(),
            ShutdownEvent::Last { snapshot } => f
                .debug_struct("ShutdownEvent::Last")
                .field("snapshot", snapshot)
                .finish(),
        }
    }
}
