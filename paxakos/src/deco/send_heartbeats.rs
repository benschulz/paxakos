use std::task::Poll;
use std::time::Duration;

use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::StreamExt;

use crate::append::{AppendArgs, DoNotRetry, Importance, Peeryness};
use crate::applicable::ApplicableTo;
use crate::error::Disoriented;
use crate::node::builder::NodeBuilderWithAll;
use crate::node::{AppendResultFor, CommunicatorOf, CoordNumOf, LogEntryOf, NodeIdOf};
use crate::node::{NodeStatus, Participation, RoundNumOf, Snapshot, SnapshotFor, StateOf};
use crate::{Node, NodeBuilder, RoundNum};

use super::Decoration;
use super::{track_leadership::MaybeLeadershipAwareNode, LeadershipAwareNode};

pub trait SendHeartbeatsBuilderExt<I>: NodeBuilder
where
    I: 'static,
{
    fn send_heartbeats<C, P>(
        self,
        configure: C,
    ) -> NodeBuilderWithAll<SendHeartbeats<Self::Node, P, I>, Self::Voter>
    where
        Self::Node: MaybeLeadershipAwareNode<I>,
        C: FnOnce(SendHeartbeatsBuilderBlank<Self::Node>) -> SendHeartbeatsBuilder<Self::Node, P>,
        P: Fn() -> LogEntryOf<Self::Node> + 'static;
}

pub struct SendHeartbeatsBuilder<N, P> {
    entry_producer: P,
    interval: Duration,
    leader_interval: Option<Duration>,

    _node: std::marker::PhantomData<N>,
}

impl<B, I> SendHeartbeatsBuilderExt<I> for B
where
    B: NodeBuilder,
    <B as NodeBuilder>::Node: MaybeLeadershipAwareNode<I>,
    I: 'static,
{
    fn send_heartbeats<C, P>(
        self,
        configure: C,
    ) -> NodeBuilderWithAll<SendHeartbeats<Self::Node, P, I>, <B as NodeBuilder>::Voter>
    where
        C: FnOnce(SendHeartbeatsBuilderBlank<Self::Node>) -> SendHeartbeatsBuilder<Self::Node, P>,
        P: Fn() -> LogEntryOf<<B as NodeBuilder>::Node> + 'static,
    {
        self.decorated_with(configure(SendHeartbeatsBuilderBlank::new()).build())
    }
}

pub struct SendHeartbeatsBuilderBlank<N: Node>(std::marker::PhantomData<N>);

impl<N: Node> SendHeartbeatsBuilderBlank<N> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    pub fn with_entry<P>(self, entry_producer: P) -> SendHeartbeatsBuilderWithEntry<N, P>
    where
        P: Fn() -> LogEntryOf<N>,
    {
        SendHeartbeatsBuilderWithEntry {
            entry_producer,
            _node: std::marker::PhantomData,
        }
    }
}

pub struct SendHeartbeatsBuilderWithEntry<N, P> {
    entry_producer: P,

    _node: std::marker::PhantomData<N>,
}

impl<N, P> SendHeartbeatsBuilderWithEntry<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    pub fn every(self, interval: Duration) -> SendHeartbeatsBuilder<N, P> {
        SendHeartbeatsBuilder {
            entry_producer: self.entry_producer,
            interval,
            leader_interval: None,

            _node: std::marker::PhantomData,
        }
    }
}

// TODO enable something like on_converged().modulate_rate(0.5, 1.8)
impl<N, P> SendHeartbeatsBuilder<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    pub fn when_leading_every<I>(mut self, interval: Duration) -> Self
    where
        N: LeadershipAwareNode<I>,
    {
        self.leader_interval = Some(interval);
        self
    }

    fn build(self) -> SendHeartbeatsArgs<N, P> {
        SendHeartbeatsArgs {
            entry_producer: self.entry_producer,
            interval: self.interval,
            leader_interval: self.leader_interval,

            _node: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct SendHeartbeatsArgs<N, P> {
    entry_producer: P,
    interval: Duration,
    leader_interval: Option<Duration>,

    _node: std::marker::PhantomData<N>,
}

#[derive(Debug)]
pub struct SendHeartbeats<N, P, I>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    decorated: N,
    arguments: SendHeartbeatsArgs<N, P>,

    timer: Option<futures_timer::Delay>,

    appends: futures::stream::FuturesUnordered<LocalBoxFuture<'static, ()>>,

    _i: std::marker::PhantomData<I>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueuedGap<R: RoundNum> {
    round: R,
    due_time: std::time::Instant,
}

impl<R: RoundNum> Ord for QueuedGap<R> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.due_time
            .cmp(&other.due_time)
            .then_with(|| self.round.cmp(&other.round))
            .reverse()
    }
}

impl<R: RoundNum> PartialOrd for QueuedGap<R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<N, P, I> SendHeartbeats<N, P, I>
where
    N: Node + MaybeLeadershipAwareNode<I> + 'static,
    P: Fn() -> LogEntryOf<N> + 'static,
    I: 'static,
{
    fn new_timer(&self) -> futures_timer::Delay {
        let interval = if let Some(interval) = self.arguments.leader_interval {
            let leadership = self.leadership().expect("leadership not tracked");

            if leadership.first().map(|l| l.leader) == Some(self.id()) {
                interval
            } else {
                self.arguments.interval
            }
        } else {
            self.arguments.interval
        };

        futures_timer::Delay::new(interval)
    }

    fn send_heartbeat(&mut self) {
        let log_entry = (self.arguments.entry_producer)();

        let append = self
            .decorated
            .append(
                log_entry,
                AppendArgs {
                    importance: Importance::MaintainLeadership(Peeryness::Peery),
                    retry_policy: Box::new(DoNotRetry::new()),
                    ..Default::default()
                },
            )
            .map(|_| ())
            .boxed_local();

        self.appends.push(append);
    }
}

impl<N, P, I> Decoration for SendHeartbeats<N, P, I>
where
    N: Node + MaybeLeadershipAwareNode<I> + 'static,
    P: Fn() -> LogEntryOf<N> + 'static,
    I: 'static,
{
    type Arguments = SendHeartbeatsArgs<N, P>;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
        Ok(Self {
            decorated,
            arguments,

            timer: None,

            appends: futures::stream::FuturesUnordered::new(),

            _i: std::marker::PhantomData,
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N, F, I> Node for SendHeartbeats<N, F, I>
where
    N: Node + MaybeLeadershipAwareNode<I> + 'static,
    F: Fn() -> LogEntryOf<N> + 'static,
    I: 'static,
{
    type State = StateOf<N>;
    type Communicator = CommunicatorOf<N>;
    type Shutdown = <N as Node>::Shutdown;

    fn id(&self) -> NodeIdOf<Self> {
        self.decorated.id()
    }

    fn status(&self) -> crate::NodeStatus {
        self.decorated.status()
    }

    fn participation(&self) -> Participation<RoundNumOf<Self>> {
        self.decorated.participation()
    }

    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<crate::Event<Self::State, Self::Communicator>> {
        let event = self.decorated.poll_events(cx);

        if let Poll::Ready(event) = &event {
            match event {
                crate::Event::Init {
                    status: new_status, ..
                }
                | crate::Event::StatusChange { new_status, .. } => {
                    self.timer = match new_status {
                        NodeStatus::Disoriented => None,
                        _ => Some(self.new_timer()),
                    };
                }

                crate::Event::Install { .. } | crate::Event::Apply { .. } => {
                    self.timer = Some(self.new_timer());
                }

                _ => {}
            }
        }

        while let Some(timer) = &mut self.timer {
            if timer.poll_unpin(cx).is_pending() {
                break;
            }

            self.send_heartbeat();

            self.timer = Some(self.new_timer());
        }

        let _ = self.appends.poll_next_unpin(cx);

        event
    }

    fn handle(&self) -> crate::node::HandleFor<Self> {
        self.decorated.handle()
    }

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<'static, Result<SnapshotFor<Self>, crate::error::PrepareSnapshotError>>
    {
        self.decorated.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: Snapshot<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: Snapshot<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale(
        &self,
    ) -> futures::future::LocalBoxFuture<'_, Result<std::sync::Arc<Self::State>, Disoriented>> {
        self.decorated.read_stale()
    }

    fn append<A: ApplicableTo<Self::State> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<Self::Communicator>,
    ) -> futures::future::LocalBoxFuture<'static, AppendResultFor<Self, A>> {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}
