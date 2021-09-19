pub mod communicator;
pub mod voter;

use std::collections::hash_map;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

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
use crate::node::AbstainOf;
use crate::node::AppendResultFor;
use crate::node::CommunicatorOf;
use crate::node::CoordNumOf;
use crate::node::EventFor;
use crate::node::InvocationOf;
use crate::node::NayOf;
use crate::node::NodeIdOf;
use crate::node::Participation;
use crate::node::RoundNumOf;
use crate::node::SnapshotFor;
use crate::node::StateOf;
use crate::node::YeaOf;
use crate::voting::Voter;
use crate::Node;
use crate::NodeBuilder;
use crate::NodeInfo;
use crate::State;

pub trait MasterLeaseMaintainingNode<I>: Node {
    fn read_master_lax(&self) -> LocalBoxFuture<'_, Option<Arc<StateOf<Self>>>>;

    fn read_master_strict(&self) -> LocalBoxFuture<'_, Option<Arc<StateOf<Self>>>>;
}

pub trait MaintainMasterLeaseBuilderExt {
    type Node: Node + 'static;
    type Voter: Voter;

    fn maintain_master_lease<C>(
        self,
        communicator_subscription: communicator::Subscription<NodeIdOf<Self::Node>>,
        voter_subscription: voter::Subscription<NodeIdOf<Self::Node>>,
        config: C,
    ) -> NodeBuilder<MaintainMasterLease<Self::Node, C>, Self::Voter>
    where
        C: Config<Node = Self::Node> + 'static;
}

impl<N, V> MaintainMasterLeaseBuilderExt for NodeBuilder<N, V>
where
    N: Node + 'static,
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

    fn maintain_master_lease<C>(
        self,
        communicator_subscription: communicator::Subscription<NodeIdOf<Self::Node>>,
        voter_subscription: voter::Subscription<NodeIdOf<Self::Node>>,
        config: C,
    ) -> NodeBuilder<MaintainMasterLease<Self::Node, C>, Self::Voter>
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

pub trait Config {
    type Node: Node;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Lease<I> {
    pub lessor: I,
    pub end: Instant,
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
pub struct MaintainMasterLease<N, C>
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

// TODO defensively subtract some duration to account for clock drift
impl<N, C> MaintainMasterLease<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
{
    fn has_own_lease(&self, now: Instant) -> bool {
        self.leases_by_lessor
            .get(&self.id())
            .map(|l| l.end > now)
            .unwrap_or(false)
    }

    fn has_majority_at_offset(
        &self,
        state: &StateOf<N>,
        offset: NonZeroUsize,
        now: Instant,
    ) -> bool {
        assert!(offset <= state.concurrency());

        let cluster = state.cluster_at(offset);

        // TODO handle weights (once implemented)
        let leases = cluster
            .iter()
            .filter(|n| {
                self.leases_by_lessor
                    .get(&n.id())
                    .map(|l| l.end > now)
                    .unwrap_or(false)
            })
            .count();

        leases > cluster.len() / 2
    }
}

impl<N, C> Decoration for MaintainMasterLease<N, C>
where
    N: Node + 'static,
    C: Config<Node = N> + 'static,
{
    type Arguments = Arguments<C, NodeIdOf<N>>;
    type Decorated = N;

    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, crate::error::SpawnError> {
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

impl<N, C> Node for MaintainMasterLease<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
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

        let now = Instant::now();

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

impl<D, I> MasterLeaseMaintainingNode<(I,)> for D
where
    D: Decoration
        + Node<
            Invocation = InvocationOf<<D as Decoration>::Decorated>,
            Communicator = CommunicatorOf<<D as Decoration>::Decorated>,
        >,
    <D as Decoration>::Decorated: MasterLeaseMaintainingNode<I>,
{
    fn read_master_lax(&self) -> LocalBoxFuture<'_, Option<Arc<StateOf<Self>>>> {
        Decoration::peek_into(self).read_master_lax()
    }

    fn read_master_strict(&self) -> LocalBoxFuture<'_, Option<Arc<StateOf<Self>>>> {
        Decoration::peek_into(self).read_master_strict()
    }
}

impl<N, C> MasterLeaseMaintainingNode<()> for MaintainMasterLease<N, C>
where
    N: Node + 'static,
    C: Config<Node = N>,
{
    fn read_master_lax(&self) -> LocalBoxFuture<'_, Option<Arc<StateOf<Self>>>> {
        async move {
            let now = Instant::now();

            if !self.has_own_lease(now) {
                return None;
            }

            let state = self.read_stale().await.ok()?;
            let majority = self.has_majority_at_offset(&*state, NonZeroUsize::new(1).unwrap(), now);

            majority.then(|| state)
        }
        .boxed_local()
    }

    fn read_master_strict(&self) -> LocalBoxFuture<'_, Option<Arc<StateOf<Self>>>> {
        async move {
            let now = Instant::now();

            if !self.has_own_lease(now) {
                return None;
            }

            let state = self.read_stale().await.ok()?;
            let majority = (1..=state.concurrency().into())
                .all(|o| self.has_majority_at_offset(&*state, NonZeroUsize::new(o).unwrap(), now));

            majority.then(|| state)
        }
        .boxed_local()
    }
}
