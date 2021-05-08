use std::sync::Arc;

use thiserror::Error;

use crate::append::AppendError;
use crate::communicator::{Communicator, CoordNumOf, LogEntryOf};
use crate::state::{LogEntryIdOf, State};

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
pub enum PrepareError<C: Communicator> {
    #[error("conflicting promise")]
    Conflict(CoordNumOf<C>),

    #[error("round already converged")]
    Converged(CoordNumOf<C>, Option<(CoordNumOf<C>, Arc<LogEntryOf<C>>)>),

    #[error("node is passive")]
    Passive,

    #[error("node is shut down")]
    ShutDown,
}

impl<C: Communicator> std::fmt::Debug for PrepareError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrepareError::Conflict(coord_num) => f
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

impl<C: Communicator> From<PrepareError<C>> for AppendError<C> {
    fn from(e: PrepareError<C>) -> Self {
        match e {
            PrepareError::Conflict(_) => AppendError::Lost,
            PrepareError::Converged(_, _) => AppendError::Converged,
            PrepareError::Passive => AppendError::Passive,
            PrepareError::ShutDown => AppendError::ShutDown,
        }
    }
}

/// A proposal could not be accepted.
#[derive(Error)]
pub enum AcceptError<C: Communicator> {
    #[error("conflicting promise")]
    Conflict(CoordNumOf<C>),

    #[error("round already converged")]
    Converged(CoordNumOf<C>, Option<(CoordNumOf<C>, Arc<LogEntryOf<C>>)>),

    #[error("node is passive")]
    Passive,

    #[error("node is shut down")]
    ShutDown,
}

impl<C: Communicator> std::fmt::Debug for AcceptError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptError::Conflict(coord_num) => f
                .debug_tuple("AcceptError::Conflict")
                .field(coord_num)
                .finish(),
            AcceptError::Converged(coord_num, converged) => f
                .debug_tuple("AcceptError::Converged")
                .field(coord_num)
                .field(converged)
                .finish(),
            AcceptError::Passive => f.debug_tuple("AcceptError::Passive").finish(),
            AcceptError::ShutDown => f.debug_tuple("AcceptError::ShutDown").finish(),
        }
    }
}

impl<C: Communicator> From<AcceptError<C>> for AppendError<C> {
    fn from(e: AcceptError<C>) -> Self {
        match e {
            AcceptError::Conflict(_) => AppendError::Lost,
            AcceptError::Converged(_, _) => AppendError::Converged,
            AcceptError::Passive => AppendError::Passive,
            AcceptError::ShutDown => AppendError::ShutDown,
        }
    }
}

/// Committing a log entry failed.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CommitError<S: State> {
    #[error("node is disoriented")]
    Disoriented,

    /// The given id could not be resolved to a log entry.
    #[error("given log entry id is invalid")]
    InvalidEntryId(LogEntryIdOf<S>),

    /// The paxakos node was shut down.
    #[error("node is shut down")]
    ShutDown,
}

impl<S: State, C: Communicator> From<CommitError<S>> for AppendError<C> {
    fn from(e: CommitError<S>) -> Self {
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
