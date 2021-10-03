use std::task::Poll;
use std::time::Duration;

use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::StreamExt;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::buffer::Buffer;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::error::ShutDownOr;
use crate::node::builder::NodeBuilder;
use crate::node::AbstainOf;
use crate::node::AppendResultFor;
use crate::node::CommunicatorOf;
use crate::node::CoordNumOf;
use crate::node::EventFor;
use crate::node::ImplAppendResultFor;
use crate::node::InvocationOf;
use crate::node::LogEntryOf;
use crate::node::NayOf;
use crate::node::NodeIdOf;
use crate::node::NodeImpl;
use crate::node::NodeStatus;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::StaticAppendResultFor;
use crate::node::YeaOf;
use crate::retry::RetryPolicy;
use crate::voting::Voter;
use crate::Node;
use crate::RoundNum;

pub trait EnsureLeadershipBuilderExt {
    type Node: Node + 'static;
    type Voter: Voter;
    type Buffer: Buffer<
        RoundNum = RoundNumOf<Self::Node>,
        CoordNum = CoordNumOf<Self::Node>,
        Entry = LogEntryOf<Self::Node>,
    >;

    fn ensure_leadership<C, P>(
        self,
        configure: C,
    ) -> NodeBuilder<EnsureLeadership<Self::Node, P>, Self::Voter, Self::Buffer>
    where
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

impl<N, V, B> EnsureLeadershipBuilderExt for NodeBuilder<N, V, B>
where
    N: NodeImpl + 'static,
    V: Voter<
        State = StateOf<N>,
        RoundNum = RoundNumOf<N>,
        CoordNum = CoordNumOf<N>,
        Abstain = AbstainOf<N>,
        Yea = YeaOf<N>,
        Nay = NayOf<N>,
    >,
    B: Buffer<RoundNum = RoundNumOf<N>, CoordNum = CoordNumOf<N>, Entry = LogEntryOf<N>>,
{
    type Node = N;
    type Voter = V;
    type Buffer = B;

    fn ensure_leadership<C, P>(
        self,
        configure: C,
    ) -> NodeBuilder<EnsureLeadership<Self::Node, P>, Self::Voter, Self::Buffer>
    where
        C: FnOnce(EnsureLeadershipBuilderBlank<N>) -> EnsureLeadershipBuilder<N, P>,
        P: Fn() -> LogEntryOf<N> + 'static,
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
pub struct EnsureLeadership<N, P>
where
    N: Node,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    decorated: N,
    arguments: EnsureLeadershipArgs<N, P>,

    timer: Option<futures_timer::Delay>,

    appends: futures::stream::FuturesUnordered<LocalBoxFuture<'static, ()>>,
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

impl<N, P> EnsureLeadership<N, P>
where
    N: Node + 'static,
    P: Fn() -> LogEntryOf<N> + 'static,
{
    fn ensure_leadership(&mut self) {
        let log_entry = (self.arguments.entry_producer)();

        let append = self
            .decorated
            .append_static(log_entry, ())
            .map(|_| ())
            .boxed_local();

        self.appends.push(append);
    }
}

impl<N, P> Decoration for EnsureLeadership<N, P>
where
    N: NodeImpl + 'static,
    P: Fn() -> LogEntryOf<N> + 'static,
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
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N, F> Node for EnsureLeadership<N, F>
where
    N: Node + 'static,
    F: Fn() -> LogEntryOf<N> + 'static,
{
    type Invocation = InvocationOf<N>;
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

    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> Poll<EventFor<Self>> {
        let event = self.decorated.poll_events(cx);

        if let Poll::Ready(event) = &event {
            match event {
                crate::Event::Init {
                    status: new_status, ..
                }
                | crate::Event::StatusChange { new_status, .. } => {
                    self.timer = match new_status {
                        NodeStatus::Disoriented => None,
                        _ => Some(futures_timer::Delay::new(self.arguments.interval)),
                    };
                }

                crate::Event::Install { .. } | crate::Event::Apply { .. } => {
                    self.timer = Some(futures_timer::Delay::new(self.arguments.interval));
                }

                _ => {}
            }
        }

        while let Some(timer) = &mut self.timer {
            if timer.poll_unpin(cx).is_pending() {
                break;
            }

            self.ensure_leadership();

            self.timer = Some(futures_timer::Delay::new(self.arguments.interval));
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
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale(
        &self,
    ) -> futures::future::LocalBoxFuture<'_, Result<std::sync::Arc<StateOf<Self>>, Disoriented>>
    {
        self.decorated.read_stale()
    }

    fn append<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> futures::future::LocalBoxFuture<'_, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
    {
        self.decorated.append(applicable, args)
    }

    fn append_static<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> futures::future::LocalBoxFuture<'static, StaticAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.decorated.append_static(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

impl<N, F> NodeImpl for EnsureLeadership<N, F>
where
    N: NodeImpl + 'static,
    F: Fn() -> LogEntryOf<N> + 'static,
{
    fn append_impl<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'static, ImplAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
    {
        self.decorated.append_impl(applicable, args)
    }
}
