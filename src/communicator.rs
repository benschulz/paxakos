use std::sync::Arc;

use futures::future::LocalBoxFuture;

use crate::{CoordNum, LogEntry, NodeInfo, Promise, Rejection, RoundNum};

pub type CoordNumOf<C> = <C as Communicator>::CoordNum;
pub type ErrorOf<C> = <C as Communicator>::Error;
pub type RoundNumOf<C> = <C as Communicator>::RoundNum;

pub trait Communicator: 'static {
    type Node: NodeInfo;

    type RoundNum: RoundNum;
    type CoordNum: CoordNum;

    type LogEntry: LogEntry;

    type Error: std::error::Error + Send + Sync + 'static;

    fn send_prepare<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
    ) -> Vec<(
        &'a Self::Node,
        LocalBoxFuture<
            'static,
            Result<PromiseOrRejection<Self::RoundNum, Self::CoordNum, Self::LogEntry>, Self::Error>,
        >,
    )>;

    fn send_proposal<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(
        &'a Self::Node,
        LocalBoxFuture<
            'static,
            Result<AcceptanceOrRejection<Self::CoordNum, Self::LogEntry>, Self::Error>,
        >,
    )>;

    fn send_commit<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        log_entry: Arc<Self::LogEntry>,
    ) -> Vec<(
        &'a Self::Node,
        LocalBoxFuture<'static, Result<(), Self::Error>>,
    )>;

    fn send_commit_by_id<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        log_entry_id: <Self::LogEntry as LogEntry>::Id,
    ) -> Vec<(
        &'a Self::Node,
        LocalBoxFuture<'static, Result<(), Self::Error>>,
    )>;
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
