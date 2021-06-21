use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::sync::Arc;

use crate::log::LogEntry;
use crate::state::State;
use crate::{AcceptError, CommitError, CoordNum, NodeInfo};
use crate::{Conflict, PrepareError, Promise, RoundNum};

pub type AbstentionOf<C> = <C as Communicator>::Abstention;
pub type CoordNumOf<C> = <C as Communicator>::CoordNum;
pub type ErrorOf<C> = <C as Communicator>::Error;
pub type LogEntryOf<C> = <C as Communicator>::LogEntry;
pub type LogEntryIdOf<C> = <LogEntryOf<C> as LogEntry>::Id;
pub type NodeOf<C> = <C as Communicator>::Node;
pub type RejectionOf<C> = <C as Communicator>::Rejection;
pub type RoundNumOf<C> = <C as Communicator>::RoundNum;

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
    type Abstention: std::fmt::Debug + Send + Sync + 'static;
    type Rejection: std::fmt::Debug + Send + Sync + 'static;

    type SendPrepare: Future<Output = Result<Vote<Self>, Self::Error>>;
    type SendProposal: Future<Output = Result<Acceptance<Self>, Self::Error>>;
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
pub enum Vote<C: Communicator> {
    Given(Promise<C>),
    Conflicted(Conflict<C>),
    Abstained(AbstentionOf<C>),
}

impl<C: Communicator> TryFrom<Result<Promise<C>, PrepareError<C>>> for Vote<C> {
    type Error = PrepareError<C>;

    fn try_from(result: Result<Promise<C>, PrepareError<C>>) -> Result<Self, Self::Error> {
        result
            .map(Vote::Given)
            .or_else(|err| err.try_into().map(Vote::Conflicted))
    }
}

impl<C: Communicator> TryFrom<PrepareError<C>> for Conflict<C> {
    type Error = PrepareError<C>;

    fn try_from(error: PrepareError<C>) -> Result<Self, Self::Error> {
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

impl<C: Communicator> From<Result<Promise<C>, Conflict<C>>> for Vote<C> {
    fn from(result: Result<Promise<C>, Conflict<C>>) -> Self {
        match result {
            Ok(promise) => Vote::Given(promise),
            Err(rejection) => Vote::Conflicted(rejection),
        }
    }
}

#[derive(Debug)]
pub enum Acceptance<C: Communicator> {
    Given,
    Conflicted(Conflict<C>),
    Rejected(RejectionOf<C>),
}

impl<C: Communicator> TryFrom<Result<(), AcceptError<C>>> for Acceptance<C> {
    type Error = AcceptError<C>;

    fn try_from(result: Result<(), AcceptError<C>>) -> Result<Self, Self::Error> {
        result
            .map(|_| Acceptance::Given)
            .or_else(|err| err.try_into().map(Acceptance::Conflicted))
    }
}

impl<C: Communicator> TryFrom<AcceptError<C>> for Conflict<C> {
    type Error = AcceptError<C>;

    fn try_from(error: AcceptError<C>) -> Result<Self, Self::Error> {
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

impl<C: Communicator> From<Result<(), Conflict<C>>> for Acceptance<C> {
    fn from(result: Result<(), Conflict<C>>) -> Self {
        match result {
            Ok(_) => Acceptance::Given,
            Err(rejection) => Acceptance::Conflicted(rejection),
        }
    }
}

pub struct Committed;

impl From<()> for Committed {
    fn from(_: ()) -> Self {
        Self
    }
}

impl<S: State> TryFrom<Result<(), CommitError<S>>> for Committed {
    type Error = CommitError<S>;

    fn try_from(result: Result<(), CommitError<S>>) -> Result<Self, Self::Error> {
        result.map(|_| Committed)
    }
}
