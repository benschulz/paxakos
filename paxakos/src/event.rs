use std::sync::Arc;
use std::time::Instant;

use crate::invocation::CoordNumOf;
use crate::invocation::EventOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::NodeOf;
use crate::invocation::RoundNumOf;
use crate::invocation::SnapshotFor;
use crate::invocation::StateOf;
use crate::node::NodeStatus;
use crate::RoundNum;

/// Emitted by `Node`'s [`poll_events`][crate::Node::poll_events] method.
#[non_exhaustive]
pub enum Event<I: Invocation> {
    Init {
        status: NodeStatus,
        round: RoundNumOf<I>,
        state: Option<Arc<StateOf<I>>>,
    },

    StatusChange {
        old_status: NodeStatus,
        new_status: NodeStatus,
    },

    /// The node is transitioning to [partially active
    /// participation][crate::node::Participation].
    Activate(RoundNumOf<I>),

    /// A snapshot was installed.
    Install {
        round: RoundNumOf<I>,
        state: Arc<StateOf<I>>,
    },

    /// An entry has been committed to the log.
    ///
    /// The event does not imply that the entry was applied to the shared state.
    Commit {
        /// The round for which `log_entry` was committed.
        round: RoundNumOf<I>,

        /// The log entry which was committed.
        log_entry: Arc<LogEntryOf<I>>,
    },

    /// The next log entry was applied to the state.
    Apply {
        round: RoundNumOf<I>,
        log_entry: Arc<LogEntryOf<I>>,
        result: EventOf<I>,
    },

    /// A log entry was queued, preceeding entries are still missing.
    ///
    /// Note: This event is emitted even when the queued entry is within the
    /// concurrency bound or if this node created the gap itself. The second
    /// case can arise when the leader tries to concurrently append multiple
    /// entries and abandons some of the earlier appends.
    Gaps(Vec<Gap<RoundNumOf<I>>>),

    /// This node received a (potentially indirect) directive for the given
    /// round and from the given node. The node used the mandate obtained with
    /// the given coordination number to issue the directive.
    ///
    /// This event is not emitted when this node is disoriented or lagging.
    Directive {
        leader: NodeOf<I>,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        timestamp: Instant,
    },
}

impl<I: Invocation> std::fmt::Debug for Event<I> {
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
pub enum ShutdownEvent<I: Invocation> {
    Regular(Event<I>),
    #[non_exhaustive]
    Last {
        snapshot: Option<SnapshotFor<I>>,
    },
}

impl<I: Invocation> From<Event<I>> for ShutdownEvent<I> {
    fn from(e: Event<I>) -> Self {
        ShutdownEvent::Regular(e)
    }
}

impl<I: Invocation> std::fmt::Debug for ShutdownEvent<I> {
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
