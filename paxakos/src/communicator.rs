use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::sync::Arc;

use crate::state::{self, State};
use crate::{log::LogEntry, PrepareError};
use crate::{AcceptError, CommitError, CoordNum, NodeInfo, Promise, Rejection, RoundNum};

pub type CoordNumOf<C> = <C as Communicator>::CoordNum;
pub type ErrorOf<C> = <C as Communicator>::Error;
pub type LogEntryOf<C> = <C as Communicator>::LogEntry;
pub type LogEntryIdOf<C> = <LogEntryOf<C> as LogEntry>::Id;
pub type RoundNumOf<C> = <C as Communicator>::RoundNum;

pub type AcceptanceOrRejectionFor<C> = AcceptanceOrRejection<CoordNumOf<C>, LogEntryOf<C>>;
pub type PromiseOrRejectionFor<C> = PromiseOrRejection<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<C>>;

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
pub trait Communicator: 'static {
    type Node: NodeInfo;

    type RoundNum: RoundNum;
    type CoordNum: CoordNum;

    type LogEntry: LogEntry;

    type Error: Send + Sync + 'static;

    type SendPrepare: Future<Output = Result<PromiseOrRejectionFor<Self>, Self::Error>>;
    type SendProposal: Future<Output = Result<AcceptanceOrRejectionFor<Self>, Self::Error>>;
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

#[derive(Clone, Debug)]
pub enum PromiseOrRejection<R, C, E> {
    Promise(Promise<R, C, E>),
    Rejection(Rejection<C, E>),
}

impl<S: State, R: RoundNum, C: CoordNum>
    TryFrom<Result<Promise<R, C, state::LogEntryOf<S>>, PrepareError<S, C>>>
    for PromiseOrRejection<R, C, state::LogEntryOf<S>>
{
    type Error = PrepareError<S, C>;

    fn try_from(
        result: Result<Promise<R, C, state::LogEntryOf<S>>, PrepareError<S, C>>,
    ) -> Result<Self, Self::Error> {
        result
            .map(PromiseOrRejection::Promise)
            .or_else(|err| err.try_into().map(PromiseOrRejection::Rejection))
    }
}

impl<S: State, C: CoordNum> TryFrom<PrepareError<S, C>> for Rejection<C, state::LogEntryOf<S>> {
    type Error = PrepareError<S, C>;

    fn try_from(error: PrepareError<S, C>) -> Result<Self, Self::Error> {
        match error {
            PrepareError::Conflict(coord_num) => Ok(Rejection::Conflict { coord_num }),
            PrepareError::Converged(coord_num, log_entry) => Ok(Rejection::Converged {
                coord_num,
                log_entry,
            }),
            _ => Err(error),
        }
    }
}

impl<R, C, E> From<Result<Promise<R, C, E>, Rejection<C, E>>> for PromiseOrRejection<R, C, E> {
    fn from(result: Result<Promise<R, C, E>, Rejection<C, E>>) -> Self {
        match result {
            Ok(promise) => PromiseOrRejection::Promise(promise),
            Err(rejection) => PromiseOrRejection::Rejection(rejection),
        }
    }
}

#[derive(Clone, Debug)]
pub enum AcceptanceOrRejection<C, E> {
    Acceptance,
    Rejection(Rejection<C, E>),
}

impl<S: State, C: CoordNum> TryFrom<Result<(), AcceptError<S, C>>>
    for AcceptanceOrRejection<C, state::LogEntryOf<S>>
{
    type Error = AcceptError<S, C>;

    fn try_from(result: Result<(), AcceptError<S, C>>) -> Result<Self, Self::Error> {
        result
            .map(|_| AcceptanceOrRejection::Acceptance)
            .or_else(|err| err.try_into().map(AcceptanceOrRejection::Rejection))
    }
}

impl<S: State, C: CoordNum> TryFrom<AcceptError<S, C>> for Rejection<C, state::LogEntryOf<S>> {
    type Error = AcceptError<S, C>;

    fn try_from(error: AcceptError<S, C>) -> Result<Self, Self::Error> {
        match error {
            AcceptError::Conflict(coord_num) => Ok(Rejection::Conflict { coord_num }),
            AcceptError::Converged(coord_num, log_entry) => Ok(Rejection::Converged {
                coord_num,
                log_entry,
            }),
            _ => Err(error),
        }
    }
}

impl<C, E> From<Result<(), Rejection<C, E>>> for AcceptanceOrRejection<C, E> {
    fn from(result: Result<(), Rejection<C, E>>) -> Self {
        match result {
            Ok(_) => AcceptanceOrRejection::Acceptance,
            Err(rejection) => AcceptanceOrRejection::Rejection(rejection),
        }
    }
}

pub struct Committed;

impl<S: State> TryFrom<Result<(), CommitError<S>>> for Committed {
    type Error = CommitError<S>;

    fn try_from(result: Result<(), CommitError<S>>) -> Result<Self, Self::Error> {
        result.map(|_| Committed)
    }
}
