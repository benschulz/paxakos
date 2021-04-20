use std::task::Poll;
use std::time::Duration;

use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::StreamExt;

use crate::append::{AppendArgs, AppendError, DoNotRetry};
use crate::applicable::ApplicableTo;
use crate::node::builder::NodeBuilderWithAll;
use crate::node::{CommitFor, CommunicatorOf, CoordNumOf, LogEntryOf, NodeIdOf};
use crate::node::{NodeStatus, RoundNumOf, Snapshot, SnapshotFor, StateOf};
use crate::{Node, NodeBuilder, RoundNum};

use super::Decoration;
use super::LeadershipAwareNode;

pub trait EnsureLeadershipBuilderExt<I>: NodeBuilder
where
    I: 'static,
{
    fn ensure_leadership<C, P>(
        self,
        configure: C,
    ) -> NodeBuilderWithAll<EnsureLeadership<Self::Node, P, I>>
    where
        Self::Node: LeadershipAwareNode<I>,
        C: FnOnce(
            EnsureLeadershipBuilderBlank<Self::Node>,
        ) -> EnsureLeadershipBuilder<Self::Node, P>,
        P: Fn() -> LogEntryOf<Self::Node> + 'static;
}

// TODO jitter
pub struct EnsureLeadershipBuilder<N, P> {
    entry_producer: P,
    interval: Duration,

    _node: std::marker::PhantomData<N>,
}

impl<B, I> EnsureLeadershipBuilderExt<I> for B
where
    B: NodeBuilder,
    <B as NodeBuilder>::Node: LeadershipAwareNode<I>,
    I: 'static,
{
    fn ensure_leadership<C, P>(
        self,
        configure: C,
    ) -> NodeBuilderWithAll<EnsureLeadership<Self::Node, P, I>>
    where
        C: FnOnce(
            EnsureLeadershipBuilderBlank<Self::Node>,
        ) -> EnsureLeadershipBuilder<Self::Node, P>,
        P: Fn() -> LogEntryOf<<B as NodeBuilder>::Node> + 'static,
    {
        self.decorated_with(configure(EnsureLeadershipBuilderBlank::new()).build())
    }
}

pub struct EnsureLeadershipBuilderBlank<N: Node>(std::marker::PhantomData<N>);

impl<N: Node> EnsureLeadershipBuilderBlank<N> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    pub fn with_entry<P>(self, entry_producer: P) -> EnsureLeadershipBuilderWithEntry<N, P>
    where
        P: Fn() -> LogEntryOf<N>,
    {
        EnsureLeadershipBuilderWithEntry {
            entry_producer,
            _node: std::marker::PhantomData,
        }
    }
}

pub struct EnsureLeadershipBuilderWithEntry<N, P> {
    entry_producer: P,

    _node: std::marker::PhantomData<N>,
}

impl<N, P> EnsureLeadershipBuilderWithEntry<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    pub fn every(self, interval: Duration) -> EnsureLeadershipBuilder<N, P> {
        EnsureLeadershipBuilder {
            entry_producer: self.entry_producer,
            interval,

            _node: std::marker::PhantomData,
        }
    }
}

impl<N, P> EnsureLeadershipBuilder<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    fn build(self) -> EnsureLeadershipArgs<N, P> {
        EnsureLeadershipArgs {
            entry_producer: self.entry_producer,
            interval: self.interval,

            _node: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct EnsureLeadershipArgs<N, P> {
    entry_producer: P,
    interval: Duration,

    _node: std::marker::PhantomData<N>,
}

#[derive(Debug)]
pub struct EnsureLeadership<N, P, I>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    decorated: N,
    arguments: EnsureLeadershipArgs<N, P>,

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

impl<N, P, I> EnsureLeadership<N, P, I>
where
    N: Node + LeadershipAwareNode<I> + 'static,
    P: Fn() -> LogEntryOf<N> + 'static,
    I: 'static,
{
    fn new_timer(&self) -> Option<futures_timer::Delay> {
        let leadership = self.leadership();

        if leadership.first().map(|l| l.leader) == Some(self.id()) {
            None
        } else {
            Some(futures_timer::Delay::new(self.arguments.interval))
        }
    }

    fn ensure_leadership(&mut self) {
        let log_entry = (self.arguments.entry_producer)();

        let append = self
            .decorated
            .append(
                log_entry,
                AppendArgs {
                    retry_policy: Box::new(DoNotRetry),
                    ..Default::default()
                },
            )
            .map(|_| ())
            .boxed_local();

        self.appends.push(append);
    }
}

impl<N, P, I> Decoration for EnsureLeadership<N, P, I>
where
    N: Node + LeadershipAwareNode<I> + 'static,
    P: Fn() -> LogEntryOf<N> + 'static,
    I: 'static,
{
    type Arguments = EnsureLeadershipArgs<N, P>;
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

impl<N, F, I> Node for EnsureLeadership<N, F, I>
where
    N: Node + LeadershipAwareNode<I> + 'static,
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

    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<crate::Event<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>> {
        let event = self.decorated.poll_events(cx);

        if let Poll::Ready(event) = &event {
            match event {
                crate::Event::Init {
                    status: new_status, ..
                }
                | crate::Event::StatusChange { new_status, .. } => {
                    self.timer = match new_status {
                        NodeStatus::Disoriented => None,
                        _ => self.new_timer(),
                    };
                }

                crate::Event::Install { .. } | crate::Event::Apply { .. } => {
                    self.timer = self.new_timer();
                }

                _ => {}
            }
        }

        while let Some(timer) = &mut self.timer {
            if timer.poll_unpin(cx).is_pending() {
                break;
            }

            self.ensure_leadership();

            self.timer = self.new_timer();
        }

        let _ = self.appends.poll_next_unpin(cx);

        event
    }

    fn handle(
        &self,
    ) -> crate::node::NodeHandle<
        Self::State,
        crate::communicator::RoundNumOf<Self::Communicator>,
        crate::communicator::CoordNumOf<Self::Communicator>,
    > {
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
    ) -> futures::future::LocalBoxFuture<'static, Result<std::sync::Arc<Self::State>, ()>> {
        self.decorated.read_stale()
    }

    fn append<A: ApplicableTo<Self::State> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<RoundNumOf<Self>>,
    ) -> futures::future::LocalBoxFuture<'static, Result<CommitFor<Self, A>, AppendError>> {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}