use thiserror::Error;

use crate::append::AppendError;
use crate::error::AcceptError;
use crate::error::AffirmSnapshotError;
use crate::error::CommitError;
use crate::error::InstallSnapshotError;
use crate::error::PrepareError;
use crate::error::ReadStaleError;
use crate::invocation::Invocation;
use crate::invocation::RoundNumOf;
use crate::RoundNum;

pub use crate::error::ShutDown;

impl ShutDown {
    pub(crate) fn rule_out<T>(result: Result<T, ShutDown>) -> T {
        result.unwrap()
    }
}

impl<I: Invocation> From<ShutDown> for AppendError<I> {
    fn from(_: ShutDown) -> Self {
        AppendError::ShutDown
    }
}

impl From<ShutDown> for ReadStaleError {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<I: Invocation> From<ShutDown> for PrepareError<I> {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<I: Invocation> From<ShutDown> for AcceptError<I> {
    fn from(_: ShutDown) -> Self {
        Self::ShutDown
    }
}

impl<I: Invocation> From<ShutDown> for CommitError<I> {
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

impl<I: Invocation> From<AcquireRoundNumError> for AppendError<I> {
    fn from(e: AcquireRoundNumError) -> Self {
        match e {
            AcquireRoundNumError::Converged => AppendError::Converged { caught_up: true },
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

impl<I: Invocation> From<ClusterError<RoundNumOf<I>>> for AppendError<I> {
    fn from(e: ClusterError<RoundNumOf<I>>) -> Self {
        match e {
            ClusterError::Disoriented => AppendError::Disoriented,
            ClusterError::Converged(_) => AppendError::Converged { caught_up: true },
            ClusterError::ShutDown => AppendError::ShutDown,
        }
    }
}
