use std::convert::TryFrom;
use std::convert::TryInto;
use std::future::Future;
use std::sync::Arc;

use crate::invocation;
use crate::invocation::Invocation;
use crate::log::LogEntry;
use crate::AcceptError;
use crate::CommitError;
use crate::Conflict;
use crate::CoordNum;
use crate::NodeInfo;
use crate::PrepareError;
use crate::Promise;
use crate::RoundNum;

pub type AbstainOf<C> = <C as Communicator>::Abstain;
pub type CoordNumOf<C> = <C as Communicator>::CoordNum;
pub type ErrorOf<C> = <C as Communicator>::Error;
pub type LogEntryOf<C> = <C as Communicator>::LogEntry;
pub type LogEntryIdOf<C> = <LogEntryOf<C> as LogEntry>::Id;
pub type NayOf<C> = <C as Communicator>::Nay;
pub type NodeOf<C> = <C as Communicator>::Node;
pub type NodeIdOf<C> = <NodeOf<C> as NodeInfo>::Id;
pub type RoundNumOf<C> = <C as Communicator>::RoundNum;
pub type YeaOf<C> = <C as Communicator>::Yea;

pub type AcceptanceFor<C> = Acceptance<CoordNumOf<C>, LogEntryOf<C>, YeaOf<C>, NayOf<C>>;
pub type ConflictFor<C> = Conflict<CoordNumOf<C>, LogEntryOf<C>>;
pub type PromiseFor<C> = Promise<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<C>>;
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
pub trait Communicator: Sized + 'static {
    type Node: NodeInfo;

    type RoundNum: RoundNum;
    type CoordNum: CoordNum;

    type LogEntry: LogEntry;

    type Error: std::fmt::Debug + Send + Sync + 'static;

    type SendPrepare: Future<Output = Result<VoteFor<Self>, Self::Error>>;
    type Abstain: std::fmt::Debug + Send + Sync + 'static;

    type SendProposal: Future<Output = Result<AcceptanceFor<Self>, Self::Error>>;
    type Yea: std::fmt::Debug + Send + Sync + 'static;
    type Nay: std::fmt::Debug + Send + Sync + 'static;

    type SendCommit: Future<Output = Result<Committed, Self::Error>>;
    type SendCommitById: Future<Output = Result<Committed, Self::Error>>;

    fn send_prepare<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
    ) -> Vec<(&'a Self::Node, Self::SendPrepare)>;

    fn send_proposal<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(&'a Self::Node, Self::SendProposal)>;

    fn send_commit<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(&'a Self::Node, Self::SendCommit)>;

    fn send_commit_by_id<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry_id: <Self::LogEntry as LogEntry>::Id,
    ) -> Vec<(&'a Self::Node, Self::SendCommitById)>;
}

#[derive(Debug)]
pub enum Vote<R, C, E, A> {
    Given(Promise<R, C, E>),
    Conflicted(Conflict<C, E>),
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

#[derive(Debug)]
pub enum Acceptance<C, E, Y, X> {
    Given(Y),
    Conflicted(Conflict<C, E>),
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
