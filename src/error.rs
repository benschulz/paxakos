use std::sync::Arc;

use thiserror::Error;

use crate::append::AppendError;
use crate::state::{LogEntryIdOf, LogEntryOf, State};
use crate::CoordNum;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum SpawnError {
    #[error("invalid log file")]
    InvalidObligationsFile(std::path::PathBuf, #[source] BoxError),

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

#[derive(Debug, Error)]
pub enum PrepareError<S: State, C: CoordNum> {
    #[error("conflicting promise")]
    Conflict(C),

    #[error("round already converged")]
    Converged(C, Option<Arc<LogEntryOf<S>>>),

    #[error("I/O error")]
    IoError(#[from] IoError),

    #[error("node is passive")]
    Passive,

    #[error("node is stalled")]
    Stalled,

    #[error("node is shut down")]
    ShutDown,
}

impl<S: State, C: CoordNum> From<PrepareError<S, C>> for AppendError {
    fn from(e: PrepareError<S, C>) -> Self {
        match e {
            PrepareError::Conflict(_) => AppendError::Lost,
            PrepareError::Converged(_, _) => AppendError::Converged,
            PrepareError::IoError(e) => AppendError::IoError(e),
            PrepareError::Passive => AppendError::Passive,
            PrepareError::Stalled => AppendError::Stalled,
            PrepareError::ShutDown => AppendError::ShutDown,
        }
    }
}

#[derive(Debug, Error)]
pub enum AcceptError<S: State, C: CoordNum> {
    #[error("conflicting promise")]
    Conflict(C),

    #[error("round already converged")]
    Converged(C, Option<Arc<LogEntryOf<S>>>),

    #[error("I/O error")]
    IoError(#[from] IoError),

    #[error("node is passive")]
    Passive,

    #[error("node is stalled")]
    Stalled,

    #[error("node is shut down")]
    ShutDown,
}

impl<S: State, C: CoordNum> From<AcceptError<S, C>> for AppendError {
    fn from(e: AcceptError<S, C>) -> Self {
        match e {
            AcceptError::Conflict(_) => AppendError::Lost,
            AcceptError::Converged(_, _) => AppendError::Converged,
            AcceptError::IoError(e) => AppendError::IoError(e),
            AcceptError::Passive => AppendError::Passive,
            AcceptError::Stalled => AppendError::Stalled,
            AcceptError::ShutDown => AppendError::ShutDown,
        }
    }
}

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

impl<S: State> From<CommitError<S>> for AppendError {
    fn from(e: CommitError<S>) -> Self {
        match e {
            CommitError::Disoriented => AppendError::Disoriented,
            CommitError::InvalidEntryId(_) => unreachable!(),
            CommitError::ShutDown => AppendError::ShutDown,
        }
    }
}
