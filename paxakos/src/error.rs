use std::convert::Infallible;
use std::sync::Arc;

use thiserror::Error;

use crate::append::AppendError;
use crate::invocation::AbstainOf;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryIdOf;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum SpawnError {
    #[error("invalid working directory")]
    InvalidWorkingDir(std::path::PathBuf, #[source] BoxError),

    #[error("I/O error")]
    IoError(#[from] IoError),

    #[error("other error")]
    Other(#[source] BoxError),
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct IoError(String, #[source] std::io::Error);

impl IoError {
    pub(crate) fn new(context: impl Into<String>, source: std::io::Error) -> Self {
        Self(context.into(), source)
    }

    pub(crate) fn invalid_data(
        context: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::new(
            context,
            std::io::Error::new(std::io::ErrorKind::InvalidData, source),
        )
    }
}

#[derive(Debug, Error)]
pub enum PrepareSnapshotError {
    #[error("I/O error")]
    IoError(#[from] IoError),

    #[error("node is disoriented")]
    Disoriented,

    #[error("node is shut down")]
    ShutDown,
}

#[derive(Debug, Error)]
pub enum AffirmSnapshotError {
    #[error("I/O error")]
    IoError(#[from] IoError),

    #[error("unknown snapshot")]
    Unknown,

    #[error("node is shut down")]
    ShutDown,
}

#[derive(Debug, Error)]
pub enum InstallSnapshotError {
    #[error("I/O error")]
    IoError(#[from] IoError),

    #[error("snapshot is outdated")]
    Outdated,

    #[error("node is shut down")]
    ShutDown,
}

#[derive(Debug, Error)]
pub enum ReadStaleError {
    #[error("node is disoriented")]
    Disoriented,

    #[error("node is shut down")]
    ShutDown,
}

/// Preparing a round for proposals failed.
#[derive(Error)]
pub enum PrepareError<I: Invocation> {
    #[error("promise war deliberately withheld")]
    Abstained(AbstainOf<I>),

    #[error("conflicting promise")]
    Supplanted(CoordNumOf<I>),

    #[error("round already converged")]
    Converged(CoordNumOf<I>, Option<(CoordNumOf<I>, Arc<LogEntryOf<I>>)>),

    #[error("node is passive")]
    Passive,

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
                .debug_tuple("PrepareError::Conflict")
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
    #[error("conflicting promise")]
    Supplanted(CoordNumOf<I>),

    #[error("round already converged")]
    Converged(CoordNumOf<I>, Option<(CoordNumOf<I>, Arc<LogEntryOf<I>>)>),

    #[error("node is passive")]
    Passive,

    #[error("proposal was rejected")]
    Rejected(NayOf<I>),

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
            AcceptError::Rejected(reason) => AppendError::NoQuorum {
                abstentions: Vec::new(),
                communication_errors: Vec::new(),
                rejections: vec![reason],
            },
            AcceptError::ShutDown => AppendError::ShutDown,
        }
    }
}

/// Committing a log entry failed.
#[non_exhaustive]
#[derive(Error)]
pub enum CommitError<I: Invocation> {
    #[error("node is disoriented")]
    Disoriented,

    /// The given id could not be resolved to a log entry.
    #[error("given log entry id is invalid")]
    InvalidEntryId(LogEntryIdOf<I>),

    /// The paxakos node was shut down.
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

#[derive(Clone, Copy, Debug, Error)]
#[error("node is disoriented")]
pub struct Disoriented;

#[derive(Clone, Copy, Debug, Error)]
#[error("node is shut down")]
pub struct ShutDown;

impl From<Infallible> for ShutDown {
    fn from(_: Infallible) -> Self {
        Self
    }
}
