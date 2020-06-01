use std::ops::RangeInclusive;

use async_trait::async_trait;
use num_traits::{Bounded, Zero};
use thiserror::Error;

use crate::error::BoxError;
use crate::RoundNum;

#[derive(Debug)]
pub struct AppendArgs<R: RoundNum> {
    pub round: RangeInclusive<R>,
    pub importance: Importance,
    pub retry_policy: Box<dyn RetryPolicy>,
}

impl<R: RoundNum> Default for AppendArgs<R> {
    fn default() -> Self {
        Self {
            round: Zero::zero()..=Bounded::max_value(),
            importance: Importance::GainLeadership,
            retry_policy: Box::new(DoNotRetry),
        }
    }
}

/// Describes the importance of an append operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Importance {
    /// If necessary, leadership should be gained.
    ///
    /// Note: Just because leadership _should_ be gained does not imply that it
    /// can or will be gained. It merely implies that the node will not
    /// immediately give up when it doesn't have leadership.
    GainLeadership,

    /// Leadership should only be maintained.
    ///
    /// Using this mode implies that the given append is of low importance. We
    /// wish only to go through with it if it does not require an election
    /// cycle.
    MaintainLeadership(Peeryness),
}

/// Whether to inquire with other nodes about the round in question.
///
/// If a node has to abandon an append due to [lack of status][Maintain] it may
/// still wish to inquire with other nodes whether the round in question has
/// converged. This can be achieved by indicating an inquisitiveness of "peery".
///
/// [Maintain]: Importance::MaintainLeadership
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Peeryness {
    /// Inquire with up to a quorum of other nodes.
    ///
    /// By inquiring about a round with (up to) a quorum of other nodes one
    /// learns whether the round has already converged or not. This can be a
    /// convenient mechanism for keeping up with the distributed log without
    /// upsetting the status quo.
    Peery,

    /// Immediately abandon an append if status is insufficient.
    Unpeery,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AppendError {
    /// Append was aborted.
    #[error("append was aborted")]
    Aborted(BoxError),

    /// The chosen round had already converged.
    #[error("round had already converged")]
    Converged,

    /// Node does not currently know the shared state.
    #[error("node is disoriented")]
    Disoriented,

    /// Node was removed from the cluster.
    #[error("node was removed from the cluster")]
    Exiled,

    /// Node either lost its mandate or failed in acquiring one.
    #[error("node lost its mandate or failed in acquiring one")]
    Lost,

    /// Failed in achieving achieve a quorum.
    ///
    /// This commonly indicates communication errors.
    #[error("node could not achieve a quorum")]
    NoQuorum,

    /// An I/O error was encountered.
    #[error("I/O error")]
    IoError(crate::error::IoError),

    /// Catch-all, this may be refined over time.
    #[error("uncategorized error occured")]
    Other(crate::error::BoxError),

    /// Node is in passive mode.
    #[error("node is passive")]
    Passive,

    /// Node was forced to append a different entry for the chosen round.
    #[error("node was forced to append a different entry")]
    Railroaded,

    /// Node is shut down.
    #[error("node is shut down")]
    ShutDown,

    /// Node is stalled.
    ///
    /// A node is stalled when it failed to write to its obligation log. Once
    /// that happens it can no longer make promises or accept entries. This is
    /// because there is no expectation that they will be remembered after a
    /// potential crash.
    // TODO allow recovery from this state
    #[error("node is stalled")]
    Stalled,
}

#[async_trait]
pub trait RetryPolicy: std::fmt::Debug + Send {
    async fn eval(&mut self, err: AppendError) -> Result<(), BoxError>;
}

#[derive(Clone, Copy, Debug)]
pub struct DoNotRetry;

#[async_trait]
impl RetryPolicy for DoNotRetry {
    async fn eval(&mut self, _err: AppendError) -> Result<(), BoxError> {
        Err(Box::new(AbortedError))
    }
}

#[derive(Clone, Debug, Error)]
#[error("append was aborted")]
pub struct AbortedError;
