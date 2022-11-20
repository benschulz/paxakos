use std::task::Poll;
use std::time::Duration;

use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;
use smallvec::SmallVec;

use crate::communicator::AbstainOf;
use crate::communicator::Acceptance;
use crate::communicator::AcceptanceFor;
use crate::communicator::Communicator;
use crate::communicator::CoordNumOf;
use crate::communicator::ErrorOf;
use crate::communicator::LogEntryOf;
use crate::communicator::NayOf;
use crate::communicator::NodeIdOf;
use crate::communicator::NodeOf;
use crate::communicator::RoundNumOf;
use crate::communicator::YeaOf;
use crate::LogEntry;
use crate::NodeInfo;

use super::Lease;

#[derive(Debug)]
pub struct Subscription<I>(mpsc::Receiver<Lease<I>>);

impl<I> futures::stream::Stream for Subscription<I> {
    type Item = Lease<I>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.0.poll_next_unpin(cx) {
            Poll::Ready(Some(lease)) => Poll::Ready(Some(lease)),
            _ => Poll::Pending,
        }
    }
}

pub trait ToLeaseDuration {
    fn to_lease_duration(&self) -> Duration;
}

impl ToLeaseDuration for Duration {
    fn to_lease_duration(&self) -> Duration {
        *self
    }
}

pub struct LeaseRecordingCommunicator<C: Communicator> {
    delegate: C,
    // TODO dead subscriptions are never cleared
    subscriptions: SmallVec<[mpsc::Sender<Lease<NodeIdOf<C>>>; 1]>,
}

impl<C: Communicator> LeaseRecordingCommunicator<C> {
    pub fn from(communicator: C) -> Self {
        Self {
            delegate: communicator,
            subscriptions: SmallVec::new(),
        }
    }

    pub fn subscribe(&mut self) -> Subscription<NodeIdOf<C>> {
        let (send, recv) = mpsc::channel(16);

        self.subscriptions.push(send);

        Subscription(recv)
    }
}

impl<C, Y> Communicator for LeaseRecordingCommunicator<C>
where
    C: Communicator<Yea = Y>,
    Y: ToLeaseDuration + std::fmt::Debug + Send + Sync + 'static,
{
    type Node = NodeOf<C>;

    type RoundNum = RoundNumOf<C>;
    type CoordNum = CoordNumOf<C>;

    type LogEntry = LogEntryOf<C>;

    type Error = ErrorOf<C>;

    type SendPrepare = <C as Communicator>::SendPrepare;
    type Abstain = AbstainOf<C>;

    type SendProposal = BoxFuture<'static, Result<AcceptanceFor<Self>, Self::Error>>;
    type Yea = YeaOf<C>;
    type Nay = NayOf<C>;

    type SendCommit = <C as Communicator>::SendCommit;
    type SendCommitById = <C as Communicator>::SendCommitById;

    fn send_prepare<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
    ) -> Vec<(&'a Self::Node, Self::SendPrepare)> {
        self.delegate.send_prepare(receivers, round_num, coord_num)
    }

    fn send_proposal<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: std::sync::Arc<Self::LogEntry>,
    ) -> Vec<(&'a Self::Node, Self::SendProposal)> {
        self.delegate
            .send_proposal(receivers, round_num, coord_num, log_entry)
            .into_iter()
            .map(|(r, s)| {
                let subscriptions = self.subscriptions.clone();
                let lessor = r.id();

                (
                    r,
                    async move {
                        let pre = instant::Instant::now();

                        Ok(match s.await? {
                            Acceptance::Given(y) => {
                                let duration = y.to_lease_duration();
                                let lease = Lease {
                                    lessor,
                                    end: pre + duration,
                                };

                                for mut s in subscriptions {
                                    let _ = s.send(lease).await;
                                }

                                Acceptance::Given(y)
                            }
                            Acceptance::Conflicted(c) => Acceptance::Conflicted(c),
                            Acceptance::Refused(n) => Acceptance::Refused(n),
                        })
                    }
                    .boxed(),
                )
            })
            .collect()
    }

    fn send_commit<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry: std::sync::Arc<Self::LogEntry>,
    ) -> Vec<(&'a Self::Node, Self::SendCommit)> {
        self.delegate
            .send_commit(receivers, round_num, coord_num, log_entry)
    }

    fn send_commit_by_id<'a>(
        &mut self,
        receivers: &'a [Self::Node],
        round_num: Self::RoundNum,
        coord_num: Self::CoordNum,
        log_entry_id: <Self::LogEntry as LogEntry>::Id,
    ) -> Vec<(&'a Self::Node, Self::SendCommitById)> {
        self.delegate
            .send_commit_by_id(receivers, round_num, coord_num, log_entry_id)
    }
}
