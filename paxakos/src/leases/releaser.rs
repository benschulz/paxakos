use std::collections::hash_map;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::stream::StreamExt;

use crate::append::AppendArgs;
use crate::append::AppendError;
use crate::applicable::ApplicableTo;
use crate::decoration::Decoration;
use crate::error::Disoriented;
use crate::error::ShutDownOr;
use crate::node::AppendResultFor;
use crate::node::DelegatingNodeImpl;
use crate::node::EffectOf;
use crate::node::EventFor;
use crate::node::FrozenStateOf;
use crate::node::InvocationOf;
use crate::node::Node;
use crate::node::NodeIdOf;
use crate::node::NodeImpl;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node_builder::ExtensibleNodeBuilder;
use crate::retry::RetryPolicy;

type LeaseOf<N> = <EffectOf<N> as AsLeaseEffect>::Lease;
type LeaseIdOf<N> = <LeaseOf<N> as Lease>::Id;

/// Releaser configuration.
pub trait Config
where
    EffectOf<Self::Node>: AsLeaseEffect,
{
    /// The node type that is decorated.
    type Node: Node;

    /// The applicable that is used to release leases.
    type Applicable: ApplicableTo<StateOf<Self::Node>> + 'static;

    /// Type of retry policy to be used.
    ///
    /// See [`retry_policy`][Config::retry_policy].
    type RetryPolicy: RetryPolicy<
        Invocation = InvocationOf<Self::Node>,
        Error = AppendError<InvocationOf<Self::Node>>,
        StaticError = AppendError<InvocationOf<Self::Node>>,
    >;

    /// Type of currently active leases.
    type Leases<'a>: Iterator<Item = &'a LeaseOf<Self::Node>>
    where
        Self: 'a;

    /// Initializes this configuration.
    #[allow(unused_variables)]
    fn init(&mut self, node: &Self::Node) {}

    /// Updates the configuration with the given event.
    #[allow(unused_variables)]
    fn update(&mut self, event: &EventFor<Self::Node>) {}

    /// Returns the active leases for `state`.
    fn active_leases<'a>(&'a self, state: &'a FrozenStateOf<Self::Node>) -> Self::Leases<'a>;

    /// Prepares to release the lease with the given id.
    fn release(&self, lease_id: LeaseIdOf<Self::Node>) -> Self::Applicable;

    /// Creates a retry policy.
    fn retry_policy(&self) -> Self::RetryPolicy;
}

pub trait AsLeaseEffect {
    type Lease: Lease;

    fn as_lease_taken(&self) -> Option<&Self::Lease>;

    fn as_lease_released(&self) -> Option<<Self::Lease as Lease>::Id>;
}

pub trait Lease {
    type Id: Copy + Eq + std::hash::Hash + PartialEq + Send;

    fn id(&self) -> Self::Id;

    /// The value returned need not be equal, even for two consecutive calls.
    fn timeout(&self) -> instant::Instant;
}

pub trait HasLeases {
    type Lease: Lease;
    type Iter: Iterator<Item = Self::Lease>;

    fn leases(&self) -> Self::Iter;
}

pub trait ReleaserBuilderExt
where
    EffectOf<Self::Node>: AsLeaseEffect,
{
    type Node: Node;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static>;

    fn release_leases<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static;
}

impl<B> ReleaserBuilderExt for B
where
    B: ExtensibleNodeBuilder,
    B::Node: NodeImpl + 'static,
    EffectOf<B::Node>: AsLeaseEffect,
{
    type Node = B::Node;
    type DecoratedBuilder<C: Config<Node = Self::Node> + 'static> =
        B::DecoratedBuilder<Releaser<Self::Node, C>>;

    fn release_leases<C>(self, config: C) -> Self::DecoratedBuilder<C>
    where
        C: Config<Node = Self::Node> + 'static,
    {
        self.decorated_with(config)
    }
}

pub struct Releaser<N, C>
where
    N: Node,
    EffectOf<N>: AsLeaseEffect,
    C: Config<Node = N>,
{
    decorated: N,
    config: C,

    queue: BinaryHeap<QueuedLease<LeaseIdOf<N>>>,
    timeouts: HashMap<LeaseIdOf<N>, usize>,
    next_timeout_id: usize,

    timer: Option<futures_timer::Delay>,

    appends: futures::stream::FuturesUnordered<BoxFuture<'static, Option<LeaseIdOf<N>>>>,
}

impl<N, C> Releaser<N, C>
where
    N: Node,
    EffectOf<N>: AsLeaseEffect,
    C: Config<Node = N>,
{
    fn queue_lease(&mut self, lease_id: LeaseIdOf<N>, timeout: instant::Instant) {
        Self::queue_lease_split(
            &mut self.next_timeout_id,
            &mut self.timeouts,
            &mut self.queue,
            lease_id,
            timeout,
        )
    }

    fn queue_lease_split(
        next_timeout_id: &mut usize,
        timeouts: &mut HashMap<LeaseIdOf<N>, usize>,
        queue: &mut BinaryHeap<QueuedLease<LeaseIdOf<N>>>,
        lease_id: LeaseIdOf<N>,
        timeout: instant::Instant,
    ) {
        let timeout_id = *next_timeout_id;
        *next_timeout_id += 1;

        timeouts.insert(lease_id, timeout_id);
        queue.push(QueuedLease {
            lease_id,
            timeout_id,
            timeout,
        })
    }
}

impl<N, C> Decoration for Releaser<N, C>
where
    N: NodeImpl + 'static,
    EffectOf<N>: AsLeaseEffect,
    C: Config<Node = N> + 'static,
{
    type Arguments = C;

    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(Self {
            decorated,
            config: arguments,
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

impl<N, C> Node for Releaser<N, C>
where
    N: Node,
    EffectOf<N>: AsLeaseEffect,
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
        let e = self.decorated.poll_events(cx);

        if let Poll::Ready(e) = &e {
            match e {
                crate::Event::Init {
                    state: Some(state), ..
                }
                | crate::Event::Install {
                    state: Some(state), ..
                } => {
                    for lease in self.config.active_leases(state) {
                        Self::queue_lease_split(
                            &mut self.next_timeout_id,
                            &mut self.timeouts,
                            &mut self.queue,
                            lease.id(),
                            lease.timeout(),
                        );
                    }
                }

                crate::Event::Eject { .. } => {
                    self.queue.clear();
                    self.timeouts.clear();
                    self.timer = None;
                    self.appends.clear();
                }

                crate::Event::Apply { effect: result, .. } => {
                    if let Some(lease) = result.as_lease_taken() {
                        self.queue_lease(lease.id(), lease.timeout());
                    }

                    if let Some(lease_id) = result.as_lease_released() {
                        self.timeouts.remove(&lease_id);
                    }
                }

                _ => {}
            }
        }

        loop {
            let now = instant::Instant::now();

            while self.queue.peek().filter(|q| q.timeout <= now).is_some() {
                let queued = self.queue.pop().unwrap();

                if let hash_map::Entry::Occupied(e) = self.timeouts.entry(queued.lease_id) {
                    if *e.get() == queued.timeout_id {
                        let (id, _) = e.remove_entry();

                        let log_entry = self.config.release(id);
                        self.appends.push(
                            self.decorated
                                .append(log_entry, self.config.retry_policy())
                                .map(move |r| r.map(|_| None).unwrap_or(Some(id)))
                                .boxed(),
                        );
                    }
                }
            }

            while let Poll::Ready(Some(r)) = self.appends.poll_next_unpin(cx) {
                if let Some(lease_id) = r {
                    // TODO retry policy
                    let new_timeout = now + std::time::Duration::from_secs(5);
                    self.queue_lease(lease_id, new_timeout);
                }
            }

            self.timer = self
                .queue
                .peek()
                .map(|q| futures_timer::Delay::new(q.timeout - now));

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

    fn prepare_snapshot(&self) -> BoxFuture<'static, SnapshotFor<Self>> {
        self.decorated.prepare_snapshot()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.decorated.affirm_snapshot(snapshot)
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.decorated.install_snapshot(snapshot)
    }

    fn read_stale<F, T>(&self, f: F) -> BoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.decorated.read_stale(f)
    }

    fn read_stale_infallibly<F, T>(&self, f: F) -> BoxFuture<'_, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.decorated.read_stale_infallibly(f)
    }

    fn read_stale_scoped<'read, F, T>(&self, f: F) -> BoxFuture<'read, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.decorated.read_stale_scoped(f)
    }

    fn read_stale_scoped_infallibly<'read, F, T>(&self, f: F) -> BoxFuture<'read, T>
    where
        F: FnOnce(Option<&StateOf<Self>>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.decorated.read_stale_scoped_infallibly(f)
    }

    fn append<A, P, R>(
        &mut self,
        applicable: A,
        args: P,
    ) -> futures::future::BoxFuture<'static, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.decorated.append(applicable, args)
    }

    fn shut_down(self) -> Self::Shutdown {
        self.decorated.shut_down()
    }
}

impl<N, C> DelegatingNodeImpl for Releaser<N, C>
where
    N: NodeImpl,
    EffectOf<N>: AsLeaseEffect,
    C: Config<Node = N>,
{
    type Delegate = N;

    fn delegate(&mut self) -> &mut Self::Delegate {
        &mut self.decorated
    }
}

struct QueuedLease<I> {
    lease_id: I,
    timeout_id: usize,
    timeout: instant::Instant,
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
