use std::future::Future;
use std::sync::Arc;

use crate::{CoordNum, LogEntry, NodeInfo, Promise, Rejection, RoundNum};

pub type CoordNumOf<C> = <C as Communicator>::CoordNum;
pub type ErrorOf<C> = <C as Communicator>::Error;
pub type LogEntryOf<C> = <C as Communicator>::LogEntry;
pub type LogEntryIdOf<C> = <LogEntryOf<C> as LogEntry>::Id;
pub type RoundNumOf<C> = <C as Communicator>::RoundNum;

pub type AcceptanceOrRejectionFor<C> = AcceptanceOrRejection<CoordNumOf<C>, LogEntryOf<C>>;
pub type PromiseOrRejectionFor<C> = PromiseOrRejection<RoundNumOf<C>, CoordNumOf<C>, LogEntryOf<C>>;

pub trait Communicator: 'static {
    type Node: NodeInfo;

    type RoundNum: RoundNum;
    type CoordNum: CoordNum;

    type LogEntry: LogEntry;

    type Error: Send + Sync + 'static;

    type SendPrepare: Future<Output = Result<PromiseOrRejectionFor<Self>, Self::Error>>;
    type SendProposal: Future<Output = Result<AcceptanceOrRejectionFor<Self>, Self::Error>>;
    type SendCommit: Future<Output = Result<(), Self::Error>>;
    type SendCommitById: Future<Output = Result<(), Self::Error>>;

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
pub enum PromiseOrRejection<R, C, E>
where
    R: RoundNum,
    C: CoordNum,
    E: LogEntry,
{
    Promise(Promise<R, C, E>),
    Rejection(Rejection<C, E>),
}

#[derive(Clone, Debug)]
pub enum AcceptanceOrRejection<C, E>
where
    C: CoordNum,
    E: LogEntry,
{
    Acceptance,
    Rejection(Rejection<C, E>),
}
