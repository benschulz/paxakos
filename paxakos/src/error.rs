//! Defines various error types.

use std::convert::Infallible;
use std::sync::Arc;

use thiserror::Error;

use crate::append::AppendError;
use crate::invocation::AbstainOf;
use crate::invocation::CommunicationErrorOf;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryIdOf;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;

/// Reason spawning a node failed.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum SpawnError<E> {
    /// Failed to start task to maintain `State`.
    #[error("executor raised an error")]
    ExecutorError(E),

    /// A decoration's [`wrap` method][crate::decoration::Decoration::wrap]
    /// failed.
    #[error("a node decoration raised an error")]
    Decoration(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Reason node snapshot couldn't be affirmed.
#[derive(Debug, Error)]
pub enum AffirmSnapshotError {
    /// The given snapshot is unknown.
    #[error("unknown snapshot")]
    Unknown,

    /// Node is shut down.
    #[error("node is shut down")]
    ShutDown,
}

/// Reason node snapshot couldn't be installed.
#[derive(Debug, Error)]
pub enum InstallSnapshotError {
    /// The node's current state is more up-to-date.
    #[error("snapshot is outdated")]
    Outdated,

    /// Node is shut down.
    #[error("node is shut down")]
    ShutDown,
}

/// Reason node's state couldn't be read.
#[derive(Debug, Error)]
pub enum ReadStaleError {
    /// Node doesn't have state.
    #[error("node is disoriented")]
    Disoriented,

    /// Node is shut down.
    #[error("node is shut down")]
    ShutDown,
}

/// Reason preparing round for proposals failed.
#[derive(Error)]
pub enum PrepareError<I: Invocation> {
    /// Node abstained from voting.
    #[error("promise war deliberately withheld")]
    Abstained(AbstainOf<I>),

    /// Another node es running for leader with a greater coordination number.
    #[error("conflicting promise")]
    Supplanted(CoordNumOf<I>),

    /// Round is already settled.
    #[error("round already converged")]
    Converged(CoordNumOf<I>, Option<(CoordNumOf<I>, Arc<LogEntryOf<I>>)>),

    /// Node is in passive mode.
    #[error("node is passive")]
    Passive,

    /// Node is shut down.
    #[error("node is shut down")]
    ShutDown,
}

impl<I: Invocation> std::fmt::Debug for PrepareError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrepareError::Abstained(abstention) => f
                .debug_tuple("PrepareError::Abstained")
                .field(abstention)
                .finish(),
            PrepareError::Supplanted(coord_num) => f
                .debug_tuple("PrepareError::Supplanted")
                .field(coord_num)
                .finish(),
            PrepareError::Converged(coord_num, converged) => f
                .debug_tuple("PrepareError::Converged")
                .field(coord_num)
                .field(converged)
                .finish(),
            PrepareError::Passive => f.debug_tuple("PrepareError::Passive").finish(),
            PrepareError::ShutDown => f.debug_tuple("PrepareError::ShutDown").finish(),
        }
    }
}

impl<I: Invocation> From<PrepareError<I>> for AppendError<I> {
    fn from(e: PrepareError<I>) -> Self {
        match e {
            PrepareError::Abstained(reason) => AppendError::NoQuorum {
                abstentions: vec![reason],
                communication_errors: Vec::new(),
                discards: Vec::new(),
                rejections: Vec::new(),
            },
            PrepareError::Supplanted(_) => AppendError::Lost,
            PrepareError::Converged(_, log_entry) => AppendError::Converged {
                caught_up: log_entry.is_some(),
            },
            PrepareError::Passive => AppendError::Passive,
            PrepareError::ShutDown => AppendError::ShutDown,
        }
    }
}

/// A proposal could not be accepted.
#[derive(Error)]
pub enum AcceptError<I: Invocation> {
    /// Another node has become leader with a greater coordination number.
    #[error("conflicting promise")]
    Supplanted(CoordNumOf<I>),

    /// Round is already settled.
    #[error("round already converged")]
    Converged(CoordNumOf<I>, Option<(CoordNumOf<I>, Arc<LogEntryOf<I>>)>),

    /// Node is in passive mode.
    #[error("node is passive")]
    Passive,

    /// Node rejected the proposal.
    #[error("proposal was rejected")]
    Rejected(NayOf<I>),

    /// Node is shut down.
    #[error("node is shut down")]
    ShutDown,
}

impl<I: Invocation> std::fmt::Debug for AcceptError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptError::Supplanted(coord_num) => f
                .debug_tuple("AcceptError::Conflict")
                .field(coord_num)
                .finish(),
            AcceptError::Converged(coord_num, converged) => f
                .debug_tuple("AcceptError::Converged")
                .field(coord_num)
                .field(converged)
                .finish(),
            AcceptError::Passive => f.debug_tuple("AcceptError::Passive").finish(),
            AcceptError::Rejected(rejection) => f
                .debug_tuple("AcceptError::Rejected")
                .field(rejection)
                .finish(),
            AcceptError::ShutDown => f.debug_tuple("AcceptError::ShutDown").finish(),
        }
    }
}

impl<I: Invocation> From<AcceptError<I>> for AppendError<I> {
    fn from(e: AcceptError<I>) -> Self {
        match e {
            AcceptError::Supplanted(_) => AppendError::Lost,
            AcceptError::Converged(_, log_entry) => AppendError::Converged {
                caught_up: log_entry.is_some(),
            },
            AcceptError::Passive => AppendError::Passive,
            AcceptError::Rejected(reason) => AppendError::Unacceptable(reason),
            AcceptError::ShutDown => AppendError::ShutDown,
        }
    }
}

/// Committing a log entry failed.
#[non_exhaustive]
#[derive(Error)]
pub enum CommitError<I: Invocation> {
    /// Node doesn't have state.
    #[error("node is disoriented")]
    Disoriented,

    /// The given id could not be resolved to a log entry.
    #[error("given log entry id is invalid")]
    InvalidEntryId(LogEntryIdOf<I>),

    /// Node is shut down.
    #[error("node is shut down")]
    ShutDown,
}

impl<I: Invocation> std::fmt::Debug for CommitError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::Disoriented => f.debug_tuple("CommitError::Disoriented").finish(),
            CommitError::InvalidEntryId(entry_id) => f
                .debug_tuple("CommitError::InvalidEntryId")
                .field(entry_id)
                .finish(),
            CommitError::ShutDown => f.debug_tuple("CommitError::ShutDown").finish(),
        }
    }
}

impl<I: Invocation> From<CommitError<I>> for AppendError<I> {
    fn from(e: CommitError<I>) -> Self {
        match e {
            CommitError::Disoriented => AppendError::Disoriented,
            CommitError::InvalidEntryId(_) => unreachable!(),
            CommitError::ShutDown => AppendError::ShutDown,
        }
    }
}

impl<I: Invocation> From<CommitError<I>> for PollError<I> {
    fn from(e: CommitError<I>) -> Self {
        match e {
            CommitError::Disoriented => PollError::Disoriented,
            CommitError::InvalidEntryId(_) => unreachable!(),
            CommitError::ShutDown => PollError::ShutDown,
        }
    }
}

/// Node doesn't have state.
#[derive(Clone, Copy, Debug, Error)]
#[error("node is disoriented")]
pub struct Disoriented;

/// Node is shut down.
#[derive(Clone, Copy, Debug, Error)]
#[error("node is shut down")]
pub struct ShutDown;

impl From<Infallible> for ShutDown {
    fn from(_: Infallible) -> Self {
        Self
    }
}

impl From<ShutDownOr<Infallible>> for ShutDown {
    fn from(_: ShutDownOr<Infallible>) -> Self {
        Self
    }
}

/// Node is shut down.
#[derive(Clone, Copy, Debug)]
pub enum ShutDownOr<E> {
    /// An event other than 'shut down' occured.
    Other(E),

    /// Node is shut down.
    ShutDown,
}

impl<E> ShutDownOr<E> {
    /// Applies the mapping function to the other event, if present.
    pub fn map<T, F: FnOnce(E) -> T>(self, f: F) -> ShutDownOr<T> {
        match self {
            ShutDownOr::Other(e) => ShutDownOr::Other(f(e)),
            ShutDownOr::ShutDown => ShutDownOr::ShutDown,
        }
    }

    /// Expect an event other than 'shut down' to have occured.
    ///
    /// Panics if a shut down occured.
    pub fn expect_other(self) -> E {
        match self {
            ShutDownOr::Other(e) => e,
            ShutDownOr::ShutDown => panic!("Node is unexpectedly shut down."),
        }
    }
}

impl<E> From<E> for ShutDownOr<E> {
    fn from(e: E) -> Self {
        ShutDownOr::Other(e)
    }
}

/// Reason a poll failed.
#[derive(Error)]
#[non_exhaustive]
pub enum PollError<I: Invocation> {
    /// A decoration failed the poll.
    #[error("a node decoration raised an error")]
    Decoration(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// Node does not currently know the shared state.
    #[error("node is disoriented")]
    Disoriented,

    /// Node already knows the log entry for the given round.
    #[error("round has converged locally")]
    LocallyConverged,

    /// While the round has converged, none of the nodes was still retaining the
    /// log entry.
    #[error("log entry is no longer retained")]
    NotRetained,

    /// Node is shut down.
    #[error("node is shut down")]
    ShutDown,

    /// Other nodes either did not respond or abstained.
    #[error("received no useful responses")]
    UselessResponses {
        /// All abstentions received.
        abstentions: Vec<AbstainOf<I>>,
        /// All encountered errors.
        communication_errors: Vec<CommunicationErrorOf<I>>,
    },
}

impl<I: Invocation> std::fmt::Debug for PollError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollError::Decoration(err) => {
                f.debug_tuple("PollError::Decoration").field(err).finish()
            }
            PollError::Disoriented => f.debug_tuple("PollError::Disoriented").finish(),
            PollError::LocallyConverged => f.debug_tuple("PollError::LocallyConverged").finish(),
            PollError::NotRetained => f.debug_tuple("PollError::NotRetained").finish(),
            PollError::ShutDown => f.debug_tuple("PollError::ShutDown").finish(),
            PollError::UselessResponses {
                abstentions,
                communication_errors,
            } => f
                .debug_struct("PollError::UselessResponses")
                .field("abstentions", abstentions)
                .field("communication_errors", communication_errors)
                .finish(),
        }
    }
}
