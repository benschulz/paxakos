use std::collections::hash_map;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;
use std::task::Poll;

use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::StreamExt;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::node::builder::NodeBuilder;
use crate::node::AbstainOf;
use crate::node::AppendResultFor;
use crate::node::CommunicatorOf;
use crate::node::CoordNumOf;
use crate::node::EventFor;
use crate::node::EventOf;
use crate::node::InvocationOf;
use crate::node::LogEntryOf;
use crate::node::NayOf;
use crate::node::Node;
use crate::node::NodeIdOf;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::YeaOf;
use crate::voting::Voter;

type LeaseOf<N> = <EventOf<N> as AsLeaseEvent>::Lease;
type LeaseIdOf<N> = <LeaseOf<N> as Lease>::Id;

pub trait AsLeaseEvent {
    type Lease: Lease;

    fn as_lease_taken(&self) -> Option<&Self::Lease>;

    fn as_lease_released(&self) -> Option<<Self::Lease as Lease>::Id>;
}

pub trait Lease {
    type Id: Copy + Eq + std::hash::Hash + PartialEq;

    fn id(&self) -> Self::Id;

    /// The value returned need not be equal, even for two consecutive calls.
    fn timeout(&self) -> std::time::Instant;
}

pub trait HasLeases {
    type Lease: Lease;
    type Iter: Iterator<Item = Self::Lease>;

    fn leases(&self) -> Self::Iter;
}

pub trait ReleaserBuilderExt {
    type Node: Node;
    type Voter: Voter;

    fn release_leases<C, P>(
        self,
        configure: C,
    ) -> NodeBuilder<Releaser<Self::Node, P>, Self::Voter>
    where
        EventOf<Self::Node>: AsLeaseEvent,
        C: FnOnce(ReleaserBuilderBlank<Self::Node>) -> ReleaserBuilder<Self::Node, P>,
        P: Fn(LeaseIdOf<Self::Node>) -> LogEntryOf<Self::Node> + 'static;
}

impl<N, V> ReleaserBuilderExt for NodeBuilder<N, V>
where
    N: Node + 'static,
    EventOf<N>: AsLeaseEvent,
    V: Voter<
        State = StateOf<N>,
        RoundNum = RoundNumOf<N>,
        CoordNum = CoordNumOf<N>,
        Abstain = AbstainOf<N>,
        Yea = YeaOf<N>,
        Nay = NayOf<N>,
    >,
{
    type Node = N;
    type Voter = V;

    fn release_leases<C, P>(self, configure: C) -> NodeBuilder<Releaser<N, P>, V>
    where
        EventOf<N>: AsLeaseEvent,
        C: FnOnce(ReleaserBuilderBlank<N>) -> ReleaserBuilder<N, P>,
        P: Fn(LeaseIdOf<N>) -> LogEntryOf<N> + 'static,
    {
        let configured = configure(ReleaserBuilderBlank(std::marker::PhantomData));

        self.decorated_with(ReleaserArgs {
            _node: std::marker::PhantomData,
            _jitter: std::time::Duration::from_millis(0),
            entry_producer: configured.entry_producer,
        })
    }
}

pub struct ReleaserBuilderBlank<N>(std::marker::PhantomData<N>)
where
    N: Node,
    EventOf<N>: AsLeaseEvent;

impl<N> ReleaserBuilderBlank<N>
where
    N: Node,
    EventOf<N>: AsLeaseEvent,
{
    pub fn with<P>(self, entry_producer: P) -> ReleaserBuilder<N, P>
    where
        P: Fn(LeaseIdOf<N>) -> LogEntryOf<N>,
    {
        ReleaserBuilder {
            entry_producer,
            _node: std::marker::PhantomData,
        }
    }
}

pub struct ReleaserBuilder<N, P>
where
    N: Node,
    EventOf<N>: AsLeaseEvent,
    P: Fn(LeaseIdOf<N>) -> LogEntryOf<N>,
{
    entry_producer: P,

    _node: std::marker::PhantomData<N>,
}

pub struct ReleaserArgs<N, P>
where
    N: Node,
    EventOf<N>: AsLeaseEvent,
    P: Fn(LeaseIdOf<N>) -> LogEntryOf<N>,
{
    _node: std::marker::PhantomData<N>,
    // TODO jitter
    _jitter: std::time::Duration,
    entry_producer: P,
}

pub struct Releaser<N, P>
where
    N: Node,
    EventOf<N>: AsLeaseEvent,
    P: Fn(LeaseIdOf<N>) -> LogEntryOf<N>,
{
    decorated: N,
    arguments: ReleaserArgs<N, P>,

    queue: BinaryHeap<QueuedLease<LeaseIdOf<N>>>,
    timeouts: HashMap<LeaseIdOf<N>, usize>,
    next_timeout_id: usize,

    timer: Option<futures_timer::Delay>,

    appends: futures::stream::FuturesUnordered<LocalBoxFuture<'static, Option<LeaseIdOf<N>>>>,
}

impl<N, P> Decoration for Releaser<N, P>
where
    N: Node + 'static,
    EventOf<N>: AsLeaseEvent,
    P: Fn(LeaseIdOf<N>) -> LogEntryOf<N> + 'static,
{
    type Arguments = ReleaserArgs<N, P>;

    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
        Ok(Self {
            decorated,
            arguments,
            queue: BinaryHeap::new(),
            timeouts: HashMap::new(),
            next_timeout_id: 0,
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

impl<N, P> Node for Releaser<N, P>
where
    N: Node,
    EventOf<N>: AsLeaseEvent,
    P: Fn(LeaseIdOf<N>) -> LogEntryOf<N>,
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
        let e = self.decorated.poll_events(cx);

        // TODO queue leases on Init and Install

        if let Poll::Ready(crate::Event::Apply { result, .. }) = &e {
            if let Some(lease) = result.as_lease_taken() {
                let timeout_id = self.next_timeout_id;
                self.next_timeout_id += 1;

                self.timeouts.insert(lease.id(), timeout_id);
                self.queue.push(QueuedLease {
                    lease_id: lease.id(),
                    timeout_id,
                    timeout: lease.timeout(),
                })
            }

            if let Some(lease_id) = result.as_lease_released() {
                self.timeouts.remove(&lease_id);
            }
        }

        while let Poll::Ready(Some(r)) = self.appends.poll_next_unpin(cx) {
            if let Some(lease_id) = r {
                let timeout_id = self.next_timeout_id;
                self.next_timeout_id += 1;

                // TODO retry policy
                let new_timeout = std::time::Instant::now() + std::time::Duration::from_secs(5);
                self.timeouts.insert(lease_id, timeout_id);
                self.queue.push(QueuedLease {
                    lease_id,
                    timeout_id,
                    timeout: new_timeout,
                })
            }
        }

        loop {
            let now = std::time::Instant::now();

            while self.queue.peek().filter(|q| q.timeout <= now).is_some() {
                let queued = self.queue.pop().unwrap();

                if let hash_map::Entry::Occupied(e) = self.timeouts.entry(queued.lease_id) {
                    if *e.get() == queued.timeout_id {
                        let (id, _) = e.remove_entry();

                        let log_entry = (self.arguments.entry_producer)(id);
                        self.appends.push(
                            self.decorated
                                .append(log_entry, Default::default())
                                .map(move |r| r.map(|_| None).unwrap_or(Some(id)))
                                .boxed_local(),
                        );
                    }
                }
            }

            self.timer = self
                .queue
                .peek()
                .map(|q| futures_timer::Delay::new(q.timeout - std::time::Instant::now()));

            match self.timer.as_mut().map(|t| t.poll_unpin(cx)) {
                None | Some(Poll::Pending) => break,
                _ => {}
            }
        }

        e
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
    ) -> futures::future::LocalBoxFuture<'_, Result<Arc<StateOf<Self>>, Disoriented>> {
        self.decorated.read_stale()
    }

    fn append<A: ApplicableTo<StateOf<Self>> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<Self::Invocation>,
    ) -> futures::future::LocalBoxFuture<'static, AppendResultFor<Self, A>> {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

struct QueuedLease<I> {
    lease_id: I,
    timeout_id: usize,
    timeout: std::time::Instant,
}

impl<I> Eq for QueuedLease<I> {}

impl<I> PartialEq for QueuedLease<I> {
    fn eq(&self, other: &Self) -> bool {
        self.timeout.eq(&other.timeout)
    }
}

impl<I> Ord for QueuedLease<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timeout.cmp(&other.timeout).reverse()
    }
}

impl<I> PartialOrd for QueuedLease<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
