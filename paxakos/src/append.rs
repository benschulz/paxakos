use std::ops::RangeInclusive;

use num_traits::Bounded;
use num_traits::Zero;
use thiserror::Error;

use crate::error::BoxError;
use crate::invocation::AbstainOf;
use crate::invocation::CommunicationErrorOf;
use crate::invocation::Invocation;
use crate::invocation::NayOf;
use crate::invocation::RoundNumOf;
use crate::retry::DoNotRetry;
use crate::retry::RetryPolicy;

pub struct AppendArgs<I: Invocation> {
    pub round: RangeInclusive<RoundNumOf<I>>,
    pub importance: Importance,
    // TODO box internally
    pub retry_policy: Box<dyn RetryPolicy<Invocation = I> + Send>,
}

impl<C: Invocation> Default for AppendArgs<C> {
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
pub enum AppendError<I: Invocation> {
    /// Append was aborted.
    #[error("append was aborted")]
    Aborted(BoxError),

    /// The chosen round had already converged.
    #[error("round had already converged")]
    Converged {
        /// Whether this node has already learned the converged on value for
        /// this round.
        caught_up: bool,
    },

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
        abstentions: Vec<AbstainOf<I>>,
        communication_errors: Vec<CommunicationErrorOf<I>>,
        rejections: Vec<NayOf<I>>,
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

impl<I: Invocation> std::fmt::Debug for AppendError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::Aborted(err) => f.debug_tuple("AppendError::Aborted").field(err).finish(),
            AppendError::Converged { caught_up } => f
                .debug_struct("AppendError::Converged")
                .field("caught_up", caught_up)
                .finish(),
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
