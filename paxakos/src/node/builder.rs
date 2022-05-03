//! Contains a set of types for constructing nodes.
//!
//! See [`node_builder`][crate::node_builder] or
//! [`node::builder`][crate::node::builder].
use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::future::TryFutureExt;

use crate::buffer::Buffer;
use crate::buffer::InMemoryBuffer;
use crate::communicator::Communicator;
use crate::decoration::Decoration;
use crate::error::SpawnError;
use crate::invocation::AbstainOf;
use crate::invocation::CommunicationErrorOf;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;
use crate::invocation::NodeIdOf;
use crate::invocation::NodeOf;
use crate::invocation::RoundNumOf;
use crate::invocation::SnapshotFor;
use crate::invocation::StateOf;
use crate::invocation::YeaOf;
use crate::node;
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
use crate::voting::IndiscriminateVoter;
use crate::voting::Voter;
use crate::Node;
use crate::Shell;
use crate::State;

use super::snapshot::Snapshot;
use super::CommunicatorOf;
use super::Core;
use super::IndiscriminateVoterFor;
use super::InvocationOf;
use super::NodeImpl;
use super::NodeKit;
use super::RequestHandlerFor;

/// Result returned by [`NodeBuilder::spawn`] or [`NodeBuilder::spawn_in`].
pub type SpawnResult<T> = std::result::Result<T, SpawnError>;

/// Blank node builder.
#[derive(Default)]
pub struct NodeBuilderBlank<I>(std::marker::PhantomData<I>);

impl<I: Invocation> NodeBuilderBlank<I> {
    /// Constructs a new blank builder.
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    /// Specifies identifier of the node to be built.
    pub fn for_node(self, node_id: NodeIdOf<I>) -> NodeBuilderWithNodeId<I> {
        NodeBuilderWithNodeId { node_id }
    }
}

/// Node builder with node id already set.
pub struct NodeBuilderWithNodeId<I: Invocation> {
    node_id: NodeIdOf<I>,
}

impl<I: Invocation> NodeBuilderWithNodeId<I> {
    /// Sets the communicator with which to communicate.
    pub fn communicating_via<C>(self, communicator: C) -> NodeBuilderWithNodeIdAndCommunicator<I, C>
    where
        C: Communicator<
            Node = NodeOf<I>,
            RoundNum = RoundNumOf<I>,
            CoordNum = CoordNumOf<I>,
            LogEntry = LogEntryOf<I>,
            Error = CommunicationErrorOf<I>,
            Yea = YeaOf<I>,
            Nay = NayOf<I>,
            Abstain = AbstainOf<I>,
        >,
    {
        NodeBuilderWithNodeIdAndCommunicator {
            node_id: self.node_id,
            communicator,
        }
    }
}

/// Node builder with node id and communicator already set.
pub struct NodeBuilderWithNodeIdAndCommunicator<I: Invocation, C: Communicator> {
    node_id: NodeIdOf<I>,
    communicator: C,
}

impl<I, C> NodeBuilderWithNodeIdAndCommunicator<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = NodeOf<I>,
        RoundNum = RoundNumOf<I>,
        CoordNum = CoordNumOf<I>,
        LogEntry = LogEntryOf<I>,
        Error = CommunicationErrorOf<I>,
        Yea = YeaOf<I>,
        Nay = NayOf<I>,
        Abstain = AbstainOf<I>,
    >,
{
    /// Starts the node without any state and in passive mode.
    pub fn without_state(self) -> NodeBuilder<Core<I, C>> {
        self.with_snapshot(Snapshot::stale_without_state())
    }

    /// Starts a new cluster with the given initial state.
    ///
    /// The round number will be [zero](num_traits::Zero).
    pub fn with_initial_state<S: Into<Option<StateOf<I>>>>(
        self,
        initial_state: S,
    ) -> NodeBuilder<Core<I, C>> {
        let snapshot = initial_state
            .into()
            .map(Snapshot::initial_with)
            .unwrap_or_else(Snapshot::initial_without_state);

        self.with_snapshot(snapshot)
    }

    /// Resume operation from the given snapshot.
    ///
    /// # Soundness
    ///
    /// It is assumed that the given snapshot was yielded from the [`Final`]
    /// event of a clean shutdown and that the node hasn't run in the meantime.
    /// As such the node will start in active participation mode. This is
    /// unsound if the assumptions are violated.
    ///
    /// Use [recovering_with] to have a failed node recover.
    ///
    /// [`Final`]: crate::event::ShutdownEvent::Final
    /// [recovering_with]: NodeBuilderWithNodeIdAndCommunicator::recovering_with
    pub fn resuming_from<S: Into<SnapshotFor<I>>>(self, snapshot: S) -> NodeBuilder<Core<I, C>> {
        self.with_snapshot(snapshot.into())
    }

    /// Resume operation from the given snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_with<S: Into<Option<SnapshotFor<I>>>>(
        self,
        snapshot: S,
    ) -> NodeBuilder<Core<I, C>> {
        let snapshot = snapshot
            .into()
            .unwrap_or_else(Snapshot::stale_without_state);

        self.with_snapshot(snapshot.into_stale())
    }

    /// Resume operation without a snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_without_state(self) -> NodeBuilder<Core<I, C>> {
        self.with_snapshot(Snapshot::stale_without_state())
    }

    /// Commence operation from the given snapshot.
    ///
    /// # Soundness
    ///
    /// This method assumes that the node is  (re-)joining the Paxos cluster.
    /// Use [recovering_with] to have a failed node recover.
    ///
    /// A node is considered to rejoin iff there is a previous round `r` such
    /// that this node
    ///  - was not considered a member of the cluster for `r` and
    ///  - it did not participate in any rounds since `r`.
    ///
    /// [recovering_with]: NodeBuilderWithNodeIdAndCommunicator::recovering_with
    pub fn joining_with<S: Into<Option<SnapshotFor<I>>>>(
        self,
        snapshot: S,
    ) -> NodeBuilder<Core<I, C>> {
        let snapshot = snapshot
            .into()
            .unwrap_or_else(Snapshot::initial_without_state);

        self.with_snapshot(snapshot.into_fresh())
    }

    /// Commence operation without a snapshot.
    ///
    /// # Soundness
    ///
    /// This method assumes that the node is  (re-)joining the Paxos cluster.
    /// Use [recovering_with] to have a failed node recover.
    ///
    /// A node is considered to rejoin iff there is a previous round `r` such
    /// that this node
    ///  - was not considered a member of the cluster for `r` and
    ///  - it did not participate in any rounds since `r`.
    ///
    /// [recovering_with]: NodeBuilderWithNodeIdAndCommunicator::recovering_with
    pub fn joining_without_state(self) -> NodeBuilder<Core<I, C>> {
        self.with_snapshot(Snapshot::initial_without_state())
    }

    #[doc(hidden)]
    fn with_snapshot(self, snapshot: SnapshotFor<I>) -> NodeBuilder<Core<I, C>> {
        NodeBuilder {
            kit: NodeKit::new(),
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot,
            voter: IndiscriminateVoter::new(),
            buffer: InMemoryBuffer::new(1024),
            finisher: Box::new(Ok),

            #[cfg(feature = "tracer")]
            tracer: None,
        }
    }
}

type Finisher<N> = dyn FnOnce(Core<InvocationOf<N>, CommunicatorOf<N>>) -> SpawnResult<N>;

/// Node builder with all essential information set.
pub struct NodeBuilder<
    N: Node,
    V = IndiscriminateVoterFor<N>,
    B = InMemoryBuffer<node::RoundNumOf<N>, node::CoordNumOf<N>, node::LogEntryOf<N>>,
> {
    kit: NodeKit<InvocationOf<N>>,
    node_id: node::NodeIdOf<N>,
    voter: V,
    communicator: CommunicatorOf<N>,
    snapshot: node::SnapshotFor<N>,
    buffer: B,
    finisher: Box<Finisher<N>>,

    #[cfg(feature = "tracer")]
    tracer: Option<Box<dyn Tracer<InvocationOf<N>>>>,
}

impl<N, V, B> NodeBuilder<N, V, B>
where
    N: NodeImpl + 'static,
    V: Voter<
        State = node::StateOf<N>,
        RoundNum = node::RoundNumOf<N>,
        CoordNum = node::CoordNumOf<N>,
        Abstain = node::AbstainOf<N>,
        Yea = node::YeaOf<N>,
        Nay = node::NayOf<N>,
    >,
    B: Buffer<
        RoundNum = node::RoundNumOf<N>,
        CoordNum = node::CoordNumOf<N>,
        Entry = node::LogEntryOf<N>,
    >,
{
    /// Sets the applied entry buffer.
    pub fn buffering_applied_entries_in<
        T: Buffer<
            RoundNum = node::RoundNumOf<N>,
            CoordNum = node::CoordNumOf<N>,
            Entry = node::LogEntryOf<N>,
        >,
    >(
        self,
        buffer: T,
    ) -> NodeBuilder<N, V, T> {
        // https://github.com/rust-lang/rust/issues/86555
        NodeBuilder {
            kit: self.kit,
            node_id: self.node_id,
            voter: self.voter,
            communicator: self.communicator,
            snapshot: self.snapshot,
            buffer,
            finisher: self.finisher,

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }

    /// Tracer to record events with.
    #[cfg(feature = "tracer")]
    pub fn traced_by<T: Into<Box<dyn Tracer<InvocationOf<N>>>>>(mut self, tracer: T) -> Self {
        self.tracer = Some(tracer.into());

        self
    }

    /// Sets the voting strategy to use.
    pub fn voting_with<T>(self, voter: T) -> NodeBuilder<N, T, B> {
        // https://github.com/rust-lang/rust/issues/86555
        NodeBuilder {
            kit: self.kit,
            node_id: self.node_id,
            voter,
            communicator: self.communicator,
            snapshot: self.snapshot,
            buffer: self.buffer,
            finisher: self.finisher,

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }

    /// Sets the node kit to use.
    pub fn using(mut self, kit: NodeKit<InvocationOf<N>>) -> Self {
        self.kit = kit;

        self
    }

    /// Spawns the node into context `()`.
    pub fn spawn(self) -> LocalBoxFuture<'static, SpawnResult<(RequestHandlerFor<N>, Shell<N>)>>
    where
        node::StateOf<N>: State<Context = ()>,
    {
        self.spawn_in(())
    }

    /// Spawns the node in the given context.
    pub fn spawn_in(
        self,
        context: node::ContextOf<N>,
    ) -> LocalBoxFuture<'static, SpawnResult<(RequestHandlerFor<N>, Shell<N>)>> {
        let finisher = self.finisher;

        let receiver = self.kit.receiver;

        Core::spawn(
            self.kit.state_keeper,
            self.kit.sender,
            self.node_id,
            self.communicator,
            super::SpawnArgs {
                context,
                node_id: self.node_id,
                voter: self.voter,
                snapshot: self.snapshot,
                buffer: self.buffer,
                #[cfg(feature = "tracer")]
                tracer: self.tracer,
            },
        )
        .map(Ok)
        .and_then(|(req_handler, core)| {
            futures::future::ready(
                finisher(core).map(|node| (req_handler, Shell::new(node, receiver))),
            )
        })
        .boxed_local()
    }
}

/// Declares the `decorated_with` method to add decorations to the node being
/// built.
pub trait ExtensibleNodeBuilder {
    /// Node type without decoration applied.
    type Node: NodeImpl;

    /// Type of this builder after decoration `D` is applied.
    type DecoratedBuilder<D: Decoration<Decorated = Self::Node> + 'static>;

    /// Adds a decoration to wrap around the resulting node.
    fn decorated_with<D>(
        self,
        arguments: <D as Decoration>::Arguments,
    ) -> Self::DecoratedBuilder<D>
    where
        D: Decoration<
                Decorated = Self::Node,
                Invocation = InvocationOf<Self::Node>,
                Communicator = CommunicatorOf<Self::Node>,
            > + 'static;
}

impl<N, V, B> ExtensibleNodeBuilder for NodeBuilder<N, V, B>
where
    N: NodeImpl + 'static,
    V: Voter<
        State = node::StateOf<N>,
        RoundNum = node::RoundNumOf<N>,
        CoordNum = node::CoordNumOf<N>,
        Abstain = node::AbstainOf<N>,
        Yea = node::YeaOf<N>,
        Nay = node::NayOf<N>,
    >,
    B: Buffer<
        RoundNum = node::RoundNumOf<N>,
        CoordNum = node::CoordNumOf<N>,
        Entry = node::LogEntryOf<N>,
    >,
{
    type Node = N;
    type DecoratedBuilder<D: Decoration<Decorated = Self::Node> + 'static> = NodeBuilder<D, V, B>;

    fn decorated_with<D>(self, arguments: <D as Decoration>::Arguments) -> Self::DecoratedBuilder<D>
    where
        D: Decoration<
                Decorated = N,
                Invocation = InvocationOf<N>,
                Communicator = CommunicatorOf<N>,
            > + 'static,
    {
        let finisher = self.finisher;

        NodeBuilder {
            kit: self.kit,
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot: self.snapshot,
            voter: self.voter,
            buffer: self.buffer,
            finisher: Box::new(move |x| ((finisher)(x)).and_then(|node| D::wrap(node, arguments))),

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }
}
