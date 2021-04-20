use thiserror::Error;

use crate::append::AppendError;
use crate::error::{AcceptError, AffirmSnapshotError, CommitError, InstallSnapshotError};
use crate::error::{PrepareError, PrepareSnapshotError, ReadStaleError};
use crate::state::State;
use crate::{CoordNum, RoundNum};

#[derive(Debug)]
pub struct ShutDown;

impl ShutDown {
    pub(super) fn into_unit<T>(self) {}

    pub(super) fn rule_out<T>(result: Result<T, ShutDown>) -> T {
        result.unwrap()
    }
}

impl From<ShutDown> for AppendError {
    fn from(_: ShutDown) -> Self {
        AppendError::ShutDown
    }
}

impl From<ShutDown> for ReadStaleError {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<S: State, C: CoordNum> From<ShutDown> for PrepareError<S, C> {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<S: State, C: CoordNum> From<ShutDown> for AcceptError<S, C> {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<S: State> From<ShutDown> for CommitError<S> {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl From<ShutDown> for AcquireRoundNumError {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<R: RoundNum> From<ShutDown> for ClusterError<R> {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl From<ShutDown> for PrepareSnapshotError {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl From<ShutDown> for AffirmSnapshotError {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl From<ShutDown> for InstallSnapshotError {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

#[derive(Debug, Error)]
pub enum AcquireRoundNumError {
    #[error("the round has already converged")]
    Converged,

    #[error("node is disoriented")]
    Disoriented,

    #[error("node is shut down")]
    ShutDown,
}

impl From<AcquireRoundNumError> for AppendError {
    fn from(e: AcquireRoundNumError) -> Self {
        match e {
            AcquireRoundNumError::Converged => AppendError::Converged,
            AcquireRoundNumError::Disoriented => AppendError::Disoriented,
            AcquireRoundNumError::ShutDown => AppendError::ShutDown,
        }
    }
}

#[derive(Debug, Error)]
pub enum ClusterError<R: RoundNum> {
    #[error("the round has already converged")]
    Converged(R),

    #[error("node is disoriented")]
    Disoriented,

    #[error("node is shut down")]
    ShutDown,
}

impl<R: RoundNum> From<ClusterError<R>> for AppendError {
    fn from(e: ClusterError<R>) -> Self {
        match e {
            ClusterError::Disoriented => AppendError::Disoriented,
            ClusterError::Converged(_) => AppendError::Converged,
            ClusterError::ShutDown => AppendError::ShutDown,
        }
    }
}
