use thiserror::Error;

use crate::append::AppendError;
use crate::communicator::{Communicator, RoundNumOf};
use crate::error::{AcceptError, AffirmSnapshotError, CommitError, InstallSnapshotError};
use crate::error::{PrepareError, PrepareSnapshotError, ReadStaleError};
use crate::state::State;
use crate::RoundNum;

pub use crate::error::ShutDown;

impl ShutDown {
    pub(super) fn rule_out<T>(result: Result<T, ShutDown>) -> T {
        result.unwrap()
    }
}

impl<C: Communicator> From<ShutDown> for AppendError<C> {
    fn from(_: ShutDown) -> Self {
        AppendError::ShutDown
    }
}

impl From<ShutDown> for ReadStaleError {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<C: Communicator> From<ShutDown> for PrepareError<C> {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<C: Communicator> From<ShutDown> for AcceptError<C> {
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

impl<C: Communicator> From<AcquireRoundNumError> for AppendError<C> {
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

impl<C: Communicator> From<ClusterError<RoundNumOf<C>>> for AppendError<C> {
    fn from(e: ClusterError<RoundNumOf<C>>) -> Self {
        match e {
            ClusterError::Disoriented => AppendError::Disoriented,
            ClusterError::Converged(_) => AppendError::Converged,
            ClusterError::ShutDown => AppendError::ShutDown,
        }
    }
}
