//! Defines the [`Communicator`] trait and related types.

use std::convert::TryFrom;
use std::convert::TryInto;
use std::future::Future;
use std::sync::Arc;

use crate::invocation;
use crate::invocation::Invocation;
use crate::log_entry::LogEntry;
use crate::AcceptError;
use crate::CommitError;
use crate::Conflict;
use crate::CoordNum;
use crate::NodeInfo;
use crate::PrepareError;
use crate::Promise;
use crate::RoundNum;

/// Shorthand to extract `Abstain` type out of `C`.
pub type AbstainOf<C> = <C as Communicator>::Abstain;
/// Shorthand to extract `CoordNum` type out of `C`.
pub type CoordNumOf<C> = <C as Communicator>::CoordNum;
/// Shorthand to extract `Error` type out of `C`.
pub type ErrorOf<C> = <C as Communicator>::Error;
/// Shorthand to extract `LogEntry` type out of `C`.
pub type LogEntryOf<C> = <C as Communicator>::LogEntry;
/// Shorthand to extract log entry `Id` type out of `C`.
pub type LogEntryIdOf<C> = <LogEntryOf<C> as LogEntry>::Id;
/// Shorthand to extract `Nay` type out of `C`.
pub type NayOf<C> = <C as Communicator>::Nay;
/// Shorthand to extract `Node` type (`impl NodeInfo`) out of `C`.
pub type NodeOf<C> = <C as Communicator>::Node;
/// Shorthand to extract node (`impl NodeInfo`) `Id` type out of `C`.
pub type NodeIdOf<C> = <NodeOf<C> as NodeInfo>::Id;
/// Shorthand to extract `RoundNum` type out of `C`.
pub type RoundNumOf<C> = <C as Communicator>::RoundNum;
/// Shorthand to extract `Yea` type out of `C`.
pub type YeaOf<C> = <C as Communicator>::Yea;

/// Invokes `Acceptance` type constructor so as to be compatible with `C`.
pub type AcceptanceFor<C> = Acceptance<CoordNumOf<C>, LogEntryOf<C>, YeaOf<C>, NayOf<C>>;
/// Invokes `Conflict` type constructor so as to be compatible with `C`.
pub type ConflictFor<C> = Conflict<CoordNumOf<C>, LogEntryOf<C>>;
/// Invokes `Promise` type constructor so as to be compatible with `C`.
pub type PromiseFor<C> = Promise<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<C>>;
/// Invokes `Vote` type constructor so as to be compatible with `C`.
pub type VoteFor<C> = Vote<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<C>, AbstainOf<C>>;

/// Defines how [`Node`][crate::Node]s call others'
/// [`RequestHandler`][crate::RequestHandler]s.
///
/// The [simplest possible
/// implementation][crate::prototyping::DirectCommunicator] directly calls
/// `RequestHandler` methods, requiring that all nodes live in the same process.
/// This is useful for prototyping and testing. Most other use cases require a
/// different, custom implementation.
///
/// # Soundness
///
/// Implementations must be secure, i.e. prevent forgery and replay attacks.
/// This implicitly means that a restarted node will not see messages intended
/// for its previous run, i.e. delayed messages sent over a connectionless
/// protocol. Failure to shield a node from such messages may cause it to come
/// out of passive participation mode early and lead to inconsistency.
pub trait Communicator: Send + Sized + 'static {
    /// NodeInfo
    type Node: NodeInfo;

    /// The round number type.
    type RoundNum: RoundNum;
    /// The coordination number type.
    type CoordNum: CoordNum;

    /// The log entry type.
    type LogEntry: LogEntry;

    /// The communication error type.
    type Error: std::fmt::Debug + Send + Sync + 'static;

    /// Type of future returned from `send_prepare`.
    type SendPrepare: Future<Output = Result<VoteFor<Self>, Self::Error>> + Send;
    /// Information sent along with abstentions.
    type Abstain: std::fmt::Debug + Send + Sync + 'static;

    /// Type of future returned from `send_proposal`.
    type SendProposal: Future<Output = Result<AcceptanceFor<Self>, Self::Error>> + Send;
    /// Information sent along with yea votes.
    type Yea: std::fmt::Debug + Send + Sync + 'static;
    /// Information sent along with nay votes.
    type Nay: std::fmt::Debug + Send + Sync + 'static;

    /// Type of future returned from `send_commit`.
    type SendCommit: Future<Output = Result<Committed, Self::Error>> + Send;
    /// Type of future returned from `send_commit_by_id`.
    type SendCommitById: Future<Output = Result<Committed, Self::Error>> + Send;

    /// Send a prepare message to all `receivers`.
    ///
    /// Implementations should attempt to call each receivers'
    /// [`RequestHandler::handle_prepare`][crate::RequestHandler::
    /// handle_prepare]. The return value must contain exactly one entry per
    /// receiver with a future of `handle_prepare`'s result.
    fn send_prepare<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
    ) -> Vec<(&'a Self::Node, Self::SendPrepare)>;

    /// Send a proposal message to all `receivers`.
    ///
    /// Implementations should attempt to call each receivers'
    /// [`RequestHandler::handle_proposal`][crate::RequestHandler::
    /// handle_proposal]. The return value must contain exactly one entry per
    /// receiver with a future of `handle_proposal`'s result.
    fn send_proposal<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(&'a Self::Node, Self::SendProposal)>;

    /// Send a commit message to all `receivers`.
    ///
    /// Implementations should attempt to call each receivers'
    /// [`RequestHandler::handle_commit`][crate::RequestHandler::
    /// handle_commit]. The return value must contain exactly one entry per
    /// receiver with a future of `handle_commit`'s result.
    fn send_commit<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(&'a Self::Node, Self::SendCommit)>;

    /// Send a commit-by-id message to all `receivers`.
    ///
    /// Implementations should attempt to call each receivers'
    /// [`RequestHandler::handle_commit_by_id`][crate::RequestHandler::
    /// handle_commit_by_id]. The return value must contain exactly one entry
    /// per receiver with a future of `handle_commit_by_id`'s result.
    fn send_commit_by_id<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry_id: <Self::LogEntry as LogEntry>::Id,
    ) -> Vec<(&'a Self::Node, Self::SendCommitById)>;
}

/// A vote cast in a leader election.
#[derive(Debug)]
pub enum Vote<R, C, E, A> {
    /// The node voted for the candidate.
    Given(Promise<R, C, E>),
    /// The node couldn't vote for the candidate.
    Conflicted(Conflict<C, E>),
    /// The node abstained, refusing to vote at all.
    Abstained(A),
}

impl<I: Invocation> TryFrom<Result<invocation::PromiseFor<I>, PrepareError<I>>>
    for Vote<
        invocation::RoundNumOf<I>,
        invocation::CoordNumOf<I>,
        invocation::LogEntryOf<I>,
        invocation::AbstainOf<I>,
    >
{
    type Error = PrepareError<I>;

    fn try_from(
        result: Result<invocation::PromiseFor<I>, PrepareError<I>>,
    ) -> Result<Self, Self::Error> {
        result
            .map(Vote::Given)
            .or_else(|err| err.try_into().map(Vote::Conflicted))
    }
}

impl<I: Invocation> TryFrom<PrepareError<I>>
    for Conflict<invocation::CoordNumOf<I>, invocation::LogEntryOf<I>>
{
    type Error = PrepareError<I>;

    fn try_from(error: PrepareError<I>) -> Result<Self, Self::Error> {
        match error {
            PrepareError::Supplanted(coord_num) => Ok(Conflict::Supplanted { coord_num }),
            PrepareError::Converged(coord_num, log_entry) => Ok(Conflict::Converged {
                coord_num,
                log_entry,
            }),
            _ => Err(error),
        }
    }
}

impl<R, C, E, A> From<Result<Promise<R, C, E>, Conflict<C, E>>> for Vote<R, C, E, A> {
    fn from(result: Result<Promise<R, C, E>, Conflict<C, E>>) -> Self {
        match result {
            Ok(promise) => Vote::Given(promise),
            Err(rejection) => Vote::Conflicted(rejection),
        }
    }
}

/// A vote cast on a proposal.
#[derive(Debug)]
pub enum Acceptance<C, E, Y, X> {
    /// The node voted for the proposal.
    Given(Y),
    /// The node couldn't vote for the proposal.
    Conflicted(Conflict<C, E>),
    /// The node voted against the proposal.
    Refused(X),
}

impl<I: Invocation> TryFrom<Result<invocation::YeaOf<I>, AcceptError<I>>>
    for Acceptance<
        invocation::CoordNumOf<I>,
        invocation::LogEntryOf<I>,
        invocation::YeaOf<I>,
        invocation::NayOf<I>,
    >
{
    type Error = AcceptError<I>;

    fn try_from(result: Result<invocation::YeaOf<I>, AcceptError<I>>) -> Result<Self, Self::Error> {
        result
            .map(Acceptance::Given)
            .or_else(|err| err.try_into().map(Acceptance::Conflicted))
            .or_else(|err| {
                if let AcceptError::Rejected(nay) = err {
                    Ok(Acceptance::Refused(nay))
                } else {
                    Err(err)
                }
            })
    }
}

impl<I: Invocation> TryFrom<AcceptError<I>>
    for Conflict<invocation::CoordNumOf<I>, invocation::LogEntryOf<I>>
{
    type Error = AcceptError<I>;

    fn try_from(error: AcceptError<I>) -> Result<Self, Self::Error> {
        match error {
            AcceptError::Supplanted(coord_num) => Ok(Conflict::Supplanted { coord_num }),
            AcceptError::Converged(coord_num, log_entry) => Ok(Conflict::Converged {
                coord_num,
                log_entry,
            }),
            _ => Err(error),
        }
    }
}

impl<C, E, Y, X> From<Result<Y, Conflict<C, E>>> for Acceptance<C, E, Y, X> {
    fn from(result: Result<Y, Conflict<C, E>>) -> Self {
        result
            .map(Acceptance::Given)
            .unwrap_or_else(Acceptance::Conflicted)
    }
}

/// Node successfully committed the log entry.
pub struct Committed;

impl From<()> for Committed {
    fn from(_: ()) -> Self {
        Self
    }
}

impl<I: Invocation> TryFrom<Result<(), CommitError<I>>> for Committed {
    type Error = CommitError<I>;

    fn try_from(result: Result<(), CommitError<I>>) -> Result<Self, Self::Error> {
        result.map(|_| Committed)
    }
}
