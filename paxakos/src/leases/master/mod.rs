pub mod communicator;
pub mod voter;

use std::collections::hash_map;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::task::Poll;

pub use communicator::LeaseRecordingCommunicator;
pub use communicator::ToLeaseDuration;
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use futures::StreamExt;
pub use voter::LeaseGrantingVoter;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::error::ShutDownOr;
use crate::node::AppendResultFor;
use crate::node::EventFor;
use crate::node::ImplAppendResultFor;
use crate::node::InvocationOf;
use crate::node::NodeIdOf;
use crate::node::NodeImpl;
use crate::node::NodeOf;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::StaticAppendResultFor;
use crate::node_builder::ExtensibleNodeBuilder;
use crate::retry::RetryPolicy;
use crate::Node;
use crate::NodeInfo;
use crate::State as _;

pub trait MasterLeasingNode<I>: Node {
    fn read_master_lax<F, T>(&self, f: F) -> LocalBoxFuture<'_, Option<T>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static;

    fn read_master_strict<F, T>(&self, f: F) -> LocalBoxFuture<'_, Option<T>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static;
}

pub trait MasterLeasesBuilderExt {
    type Node: Node + 'static;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static>;

    fn maintain_master_lease<C>(
        self,
        communicator_subscription: communicator::Subscription<NodeIdOf<Self::Node>>,
        voter_subscription: voter::Subscription<NodeIdOf<Self::Node>>,
        config: C,
    ) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static;
}

impl<B> MasterLeasesBuilderExt for B
where
    B: ExtensibleNodeBuilder,
    B::Node: NodeImpl + 'static,
{
    type Node = B::Node;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static> =
        B::DecoratedBuilder<MasterLeases<B::Node, C>>;

    fn maintain_master_lease<C>(
        self,
        communicator_subscription: communicator::Subscription<NodeIdOf<Self::Node>>,
        voter_subscription: voter::Subscription<NodeIdOf<Self::Node>>,
        config: C,
    ) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static,
    {
        self.decorated_with(Arguments {
            config,
            communicator_subscription,
            voter_subscription,
        })
    }
}

pub struct Arguments<C, I> {
    config: C,
    communicator_subscription: communicator::Subscription<I>,
    voter_subscription: voter::Subscription<I>,
}

pub trait State: crate::State {
    /// Duration of a master lease.
    fn lease_duration(&self) -> instant::Duration;

    /// Remaining duration of a lease this node gave out _in a previous run_ or
    /// `ZERO`.
    ///
    /// This value is concerned with the following scenario.
    ///
    /// 1. `lease_duration` is `d1`
    /// 2. Node A hands out a lease to node B.
    /// 3. Node A crashes.
    /// 4. `lease_duration` is set to `d2` < `d1`.
    /// 5. A node other than node A creates a snapshot (lease duration is `d2`).
    /// 6. Node A recovers using the snapshot.
    /// 7. Node A is asked to cast a vote by node C and must decide whether the
    ///    new lease would be in conflict with any it handed out before its
    ///    crash.
    ///
    /// If the time it takes to create, transfer and recover from a snapshot
    /// will always be greater than the `lease_duration`, then an implementation
    /// can safely return `ZERO`.
    fn previous_lease_duration(&self) -> instant::Duration;
}

pub trait Config {
    /// The node type that is decorated.
    type Node: Node;

    /// Initializes this configuration.
    #[allow(unused_variables)]
    fn init(&mut self, node: &Self::Node) {}

    /// Updates the configuration with the given event.
    #[allow(unused_variables)]
    fn update(&mut self, event: &EventFor<Self::Node>) {}

    /// Upper bound on the amount of time dilation that may be seen within a
    /// [State::lease_duration].
    ///
    /// This value can and in some cases should be node-specific. The
    /// implementation of [`Instant::now()`](std::time::Instant::now)
    /// dictates what a valid upper bound is. We're concerned with the following
    /// scenario.
    ///
    /// 1. `lease_duration` is `d`.
    /// 2. This node begins appending a log entry.
    /// 3. A majority of nodes accept the log entry and hand out leases.
    /// 4. This node receives the acceptances and considers itself master until
    ///    `t2 + d`.
    /// 5. This node attempts a master read and needs to decide wheter it's
    ///    still master.
    ///
    /// If this node's system (backing `Instant::now()`) dilates/stretches time
    /// between `t2` and `t5` then this dilation must be compensated for by this
    /// value, `dilation_margin`.
    fn dilation_margin(&self) -> instant::Duration;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Lease<I> {
    pub lessor: I,
    pub end: instant::Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueuedLease<I>(Lease<I>);

impl<I: Eq> Ord for QueuedLease<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.end.cmp(&other.0.end).reverse()
    }
}

impl<I: Eq> PartialOrd for QueuedLease<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct MasterLeases<N, C>
where
    N: Node,
    C: Config<Node = N>,
{
    decorated: N,
    config: C,

    communicator_subscription: communicator::Subscription<NodeIdOf<N>>,
    voter_subscription: voter::Subscription<NodeIdOf<N>>,

    leases_by_end: BinaryHeap<QueuedLease<NodeIdOf<N>>>,
    leases_by_lessor: HashMap<NodeIdOf<N>, Lease<NodeIdOf<N>>>,
}

impl<N, C> MasterLeases<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
{
    fn has_own_lease(&self, now: instant::Instant) -> bool {
        self.leases_by_lessor
            .get(&self.id())
            .map(|l| self.is_valid_at(l, now))
            .unwrap_or(false)
    }

    fn has_majority_of(&self, cluster: Vec<NodeOf<N>>, now: instant::Instant) -> bool {
        // TODO handle weights (once implemented)
        let leases = cluster
            .iter()
            .filter(|n| {
                self.leases_by_lessor
                    .get(&n.id())
                    .map(|l| self.is_valid_at(l, now))
                    .unwrap_or(false)
            })
            .count();

        leases > cluster.len() / 2
    }

    fn is_valid_at(&self, lease: &Lease<NodeIdOf<N>>, t: instant::Instant) -> bool {
        lease.end - self.config.dilation_margin() > t
    }
}

impl<N, C> Decoration for MasterLeases<N, C>
where
    N: NodeImpl + 'static,
    C: Config<Node = N> + 'static,
{
    type Arguments = Arguments<C, NodeIdOf<N>>;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(Self {
            decorated,
            config: arguments.config,

            communicator_subscription: arguments.communicator_subscription,
            voter_subscription: arguments.voter_subscription,

            leases_by_end: BinaryHeap::new(),
            leases_by_lessor: HashMap::new(),
        })
    }

    fn peek_into(decorated: &Self) -> &Self::Decorated {
        &decorated.decorated
    }

    fn unwrap(decorated: Self) -> Self::Decorated {
        decorated.decorated
    }
}

impl<N, C> Node for MasterLeases<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
{
    type Invocation = InvocationOf<N>;
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

        while let Poll::Ready(Some(lease)) = self.communicator_subscription.poll_next_unpin(cx) {
            self.leases_by_end.push(QueuedLease(lease));
            self.leases_by_lessor.insert(lease.lessor, lease);
        }

        let node_id = self.id();
        while let Poll::Ready(Some(lease)) = self.voter_subscription.poll_next_unpin(cx) {
            if lease.lessee == Some(node_id) {
                let lease = Lease {
                    lessor: node_id,
                    end: lease.end,
                };

                self.leases_by_end.push(QueuedLease(lease));
                self.leases_by_lessor.insert(node_id, lease);
            }
        }

        let now = instant::Instant::now();

        while let Some(&QueuedLease(lease)) = self.leases_by_end.peek().filter(|l| l.0.end <= now) {
            if let hash_map::Entry::Occupied(e) = self.leases_by_lessor.entry(lease.lessor) {
                if *e.get() == lease {
                    e.remove();
                }
            }

            self.leases_by_end.pop();
        }

        event
    }

    fn handle(&self) -> crate::node::HandleFor<Self> {
        self.decorated.handle()
    }

    fn prepare_snapshot(&self) -> LocalBoxFuture<'static, SnapshotFor<Self>> {
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

    fn read_stale<F, T>(&self, f: F) -> LocalBoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.decorated.read_stale(f)
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

impl<N, C> NodeImpl for MasterLeases<N, C>
where
    N: NodeImpl + 'static,
    C: Config<Node = N>,
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

    fn await_commit_of(
        &self,
        log_entry_id: crate::node::LogEntryIdOf<Self>,
    ) -> LocalBoxFuture<'static, Result<crate::node::CommitFor<Self>, crate::error::ShutDown>> {
        self.decorated.await_commit_of(log_entry_id)
    }

    fn eject(
        &self,
        reason: crate::node::EjectionOf<Self>,
    ) -> LocalBoxFuture<'static, Result<bool, crate::error::ShutDown>> {
        self.decorated.eject(reason)
    }
}

impl<D, I> MasterLeasingNode<(I,)> for D
where
    D: Decoration,
    <D as Decoration>::Decorated: MasterLeasingNode<I>,
{
    fn read_master_lax<F, T>(&self, f: F) -> LocalBoxFuture<'_, Option<T>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        Decoration::peek_into(self).read_master_lax(f)
    }

    fn read_master_strict<F, T>(&self, f: F) -> LocalBoxFuture<'_, Option<T>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        Decoration::peek_into(self).read_master_strict(f)
    }
}

impl<N, C> MasterLeasingNode<()> for MasterLeases<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
{
    fn read_master_lax<F, T>(&self, f: F) -> LocalBoxFuture<'_, Option<T>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        async move {
            let now = instant::Instant::now();

            if !self.has_own_lease(now) {
                return None;
            }

            let cluster = self
                .read_stale(|s| s.cluster_at(NonZeroUsize::new(1).unwrap()))
                .await
                .ok()?;
            let majority = self.has_majority_of(cluster, now);

            if majority {
                self.read_stale(f).await.ok()
            } else {
                None
            }
        }
        .boxed_local()
    }

    fn read_master_strict<F, T>(&self, f: F) -> LocalBoxFuture<'_, Option<T>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        async move {
            let now = instant::Instant::now();

            if !self.has_own_lease(now) {
                return None;
            }

            let clusters = self
                .read_stale(|s| {
                    (1..=crate::state::concurrency_of(s).into())
                        .map(|o| s.cluster_at(NonZeroUsize::new(o).unwrap()))
                        .collect::<Vec<_>>()
                })
                .await
                .ok()?;
            let majority = clusters.into_iter().all(|c| self.has_majority_of(c, now));

            if majority {
                self.read_stale(f).await.ok()
            } else {
                None
            }
        }
        .boxed_local()
    }
}
