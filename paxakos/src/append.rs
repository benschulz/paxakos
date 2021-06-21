use std::ops::RangeInclusive;

use async_trait::async_trait;
use num_traits::{Bounded, Zero};
use thiserror::Error;

use crate::communicator::{AbstentionOf, Communicator, ErrorOf, RejectionOf, RoundNumOf};
use crate::error::BoxError;

pub struct AppendArgs<C: Communicator> {
    pub round: RangeInclusive<RoundNumOf<C>>,
    pub importance: Importance,
    // TODO box internally
    pub retry_policy: Box<dyn RetryPolicy<Communicator = C> + Send>,
}

impl<C: Communicator> Default for AppendArgs<C> {
    fn default() -> Self {
        Self {
            round: Zero::zero()..=Bounded::max_value(),
            importance: Importance::GainLeadership,
            retry_policy: Box::new(DoNotRetry::new()),
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

#[derive(Error)]
#[non_exhaustive]
pub enum AppendError<C: Communicator> {
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

    /// Failed in achieving a quorum.
    #[error("node could not achieve a quorum")]
    NoQuorum {
        abstentions: Vec<AbstentionOf<C>>,
        communication_errors: Vec<ErrorOf<C>>,
        rejections: Vec<RejectionOf<C>>,
    },

    /// Catch-all, this may be refined over time.
    #[error("uncategorized error occured")]
    // TODO remove, introduce new variant for decorations
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
}

impl<C: Communicator> std::fmt::Debug for AppendError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::Aborted(err) => f.debug_tuple("AppendError::Aborted").field(err).finish(),
            AppendError::Converged => f.debug_tuple("AppendError::Converged").finish(),
            AppendError::Disoriented => f.debug_tuple("AppendError::Disoriented").finish(),
            AppendError::Exiled => f.debug_tuple("AppendError::Exiled").finish(),
            AppendError::Lost => f.debug_tuple("AppendError::Lost").finish(),
            AppendError::NoQuorum {
                abstentions,
                communication_errors,
                rejections,
            } => f
                .debug_struct("AppendError::NoQuorum")
                .field("abstentions", abstentions)
                .field("communication_errors", communication_errors)
                .field("rejections", rejections)
                .finish(),
            AppendError::Other(err) => f.debug_tuple("AppendError::Other").field(err).finish(),
            AppendError::Passive => f.debug_tuple("AppendError::Passive").finish(),
            AppendError::Railroaded => f.debug_tuple("AppendError::Railroaded").finish(),
            AppendError::ShutDown => f.debug_tuple("AppendError::ShutDown").finish(),
        }
    }
}

#[async_trait]
pub trait RetryPolicy {
    type Communicator: Communicator;

    async fn eval(&mut self, err: AppendError<Self::Communicator>) -> Result<(), BoxError>;
}

#[derive(Clone, Copy, Debug)]
pub struct DoNotRetry<C>(crate::util::PhantomSend<C>);

impl<C> DoNotRetry<C>
where
    C: Communicator,
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(crate::util::PhantomSend::new())
    }
}

#[async_trait]
impl<C> RetryPolicy for DoNotRetry<C>
where
    C: Communicator,
{
    type Communicator = C;

    async fn eval(&mut self, _err: AppendError<Self::Communicator>) -> Result<(), BoxError> {
        Err(Box::new(AbortedError))
    }
}

#[derive(Clone, Debug, Error)]
#[error("append was aborted")]
pub struct AbortedError;
