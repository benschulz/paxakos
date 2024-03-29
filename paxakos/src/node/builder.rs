//! Contains a set of types for constructing nodes.
//!
//! See [`node_builder`][crate::node_builder] or
//! [`node::builder`][crate::node::builder].
use crate::buffer::Buffer;
use crate::buffer::InMemoryBuffer;
use crate::communicator::Communicator;
use crate::decoration::Decoration;
use crate::error::SpawnError;
use crate::executor;
use crate::invocation::AbstainOf;
use crate::invocation::CommunicationErrorOf;
use crate::invocation::CoordNumOf;
use crate::invocation::FrozenStateOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;
use crate::invocation::NodeIdOf;
use crate::invocation::NodeOf;
use crate::invocation::RoundNumOf;
use crate::invocation::SnapshotFor;
use crate::invocation::YeaOf;
use crate::node;
use crate::node::inner::Snarc;
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
use crate::voting::IndiscriminateVoter;
use crate::voting::Voter;
use crate::Node;
use crate::RequestHandler;
use crate::Shell;
use crate::State;

use super::commits::Commits;
use super::inner::NodeInner;
use super::snapshot::Snapshot;
use super::state_keeper::StateKeeper;
use super::Core;
use super::IndiscriminateVoterFor;
use super::InvocationOf;
use super::NodeImpl;
use super::NodeKit;
use super::RequestHandlerFor;

/// Result returned by [`NodeBuilder::spawn`] or [`NodeBuilder::spawn_in`].
pub type ExecutorSpawnResult<N, E, V, B> =
    SpawnResult<N, executor::ErrorOf<E, node::InvocationOf<N>, V, B>>;

/// Result returned by [`NodeBuilder::spawn`] or [`NodeBuilder::spawn_in`].
pub type SpawnResult<N, E> = std::result::Result<(RequestHandlerFor<N>, Shell<N>), SpawnError<E>>;

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

/// Defines how to start a node w/r/t its state.
pub enum Starter<I: Invocation> {
    /// Start without a snapshot.
    ///
    /// Calling [`with(Starter::None)`][with] is equivalent to calling
    /// [`without_state()`][without_state].
    ///
    /// [with]: NodeBuilderWithNodeIdAndCommunicator::with
    /// [without_state]: NodeBuilderWithNodeIdAndCommunicator::without_state
    None,
    /// Resume from the given snapshot.
    ///
    /// Calling [`with(Starter::Resume(…))`][with] is equivalent to calling
    /// [`resuming_from(…)`][resuming_from].
    ///
    /// [with]: NodeBuilderWithNodeIdAndCommunicator::with
    /// [resuming_from]: NodeBuilderWithNodeIdAndCommunicator::resuming_from
    Resume(SnapshotFor<I>),
    /// Recover from the given snapshot.
    ///
    /// Calling [`with(Starter::Recover(…))`][with] is equivalent to calling
    /// [`recovering_with(…)`][recovering_with].
    ///
    /// [with]: NodeBuilderWithNodeIdAndCommunicator::with
    /// [recovering_with]: NodeBuilderWithNodeIdAndCommunicator::recovering_with
    Recover(SnapshotFor<I>),
}

impl<I: Invocation> From<Starter<I>> for SnapshotFor<I> {
    fn from(val: Starter<I>) -> Self {
        match val {
            Starter::None => Snapshot::stale_without_state(),
            Starter::Resume(s) => s,
            Starter::Recover(s) => s.into_stale(),
        }
    }
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
    /// Starts the node as specified by the given starter.
    pub fn with(
        self,
        starter: Starter<I>,
    ) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
        self.with_snapshot(starter.into())
    }

    /// Starts the node without any state and in passive mode.
    pub fn without_state(self) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
        self.with_snapshot(Snapshot::stale_without_state())
    }

    /// Starts a new cluster with the given initial state.
    ///
    /// The round number will be [zero](num_traits::Zero).
    pub fn with_initial_state<S: Into<Option<FrozenStateOf<I>>>>(
        self,
        initial_state: S,
    ) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
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
    pub fn resuming_from<S: Into<SnapshotFor<I>>>(
        self,
        snapshot: S,
    ) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
        self.with_snapshot(snapshot.into())
    }

    /// Resume operation from the given snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_with<S: Into<Option<SnapshotFor<I>>>>(
        self,
        snapshot: S,
    ) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
        let snapshot = snapshot
            .into()
            .unwrap_or_else(Snapshot::stale_without_state);

        self.with_snapshot(snapshot.into_stale())
    }

    /// Resume operation without a snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_without_state(
        self,
    ) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
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
    ) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
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
    pub fn joining_without_state(
        self,
    ) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
        self.with_snapshot(Snapshot::initial_without_state())
    }

    #[doc(hidden)]
    fn with_snapshot(
        self,
        snapshot: SnapshotFor<I>,
    ) -> NodeBuilder<Core<I, C>, impl Finisher<Node = Core<I, C>>> {
        NodeBuilder {
            kit: NodeKit::new(),
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot,
            voter: IndiscriminateVoter::new(),
            buffer: InMemoryBuffer::new(1024),
            context: (),
            finisher: CoreFinisher::new(),

            #[cfg(feature = "tracer")]
            tracer: None,
        }
    }
}

/// Used to construct the node when `spawn_in` is called.
///
/// As decorations are added via `ExtensibleNodeBuilder`, the builder keeps a
/// stack of the `Decoration::wrap` calls to make. This stack is encoded via
/// implementations of the `Finisher` trait.
pub trait Finisher: 'static {
    /// Type of node constructed by this finisher.
    type Node: Node;

    /// Type of communicator used by the constructed node.
    type Communicator: Communicator<
        Node = node::NodeOf<Self::Node>,
        RoundNum = node::RoundNumOf<Self::Node>,
        CoordNum = node::CoordNumOf<Self::Node>,
        LogEntry = node::LogEntryOf<Self::Node>,
        Error = node::CommunicationErrorOf<Self::Node>,
        Yea = node::YeaOf<Self::Node>,
        Nay = node::NayOf<Self::Node>,
        Abstain = node::AbstainOf<Self::Node>,
    >;

    /// Wraps the configured decorations around the given `core` node.
    fn finish(
        self,
        core: Core<InvocationOf<Self::Node>, Self::Communicator>,
    ) -> Result<Self::Node, Box<dyn std::error::Error + Send + Sync + 'static>>;
}

struct CoreFinisher<I, C>(std::marker::PhantomData<(I, C)>);

impl<I, C> CoreFinisher<I, C> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<I, C> Finisher for CoreFinisher<I, C>
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
    type Node = Core<I, C>;
    type Communicator = C;

    fn finish(
        self,
        core: Core<InvocationOf<Self::Node>, Self::Communicator>,
    ) -> Result<Core<I, C>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(core)
    }
}

#[doc(hidden)]
pub struct DecorationFinisher<D: Decoration, I> {
    arguments: <D as Decoration>::Arguments,
    inner: I,
}

impl<D: Decoration, I> DecorationFinisher<D, I> {
    fn wrap(inner: I, arguments: <D as Decoration>::Arguments) -> Self {
        DecorationFinisher { arguments, inner }
    }
}

impl<D, I> Finisher for DecorationFinisher<D, I>
where
    D: Decoration + 'static,
    I: Finisher<Node = D::Decorated> + 'static,
{
    type Node = D;
    type Communicator = <I as Finisher>::Communicator;

    fn finish(
        self,
        core: Core<InvocationOf<Self::Node>, Self::Communicator>,
    ) -> Result<D, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.inner
            .finish(core)
            .and_then(move |node| D::wrap(node, self.arguments))
    }
}

/// Node builder with all essential information set.
pub struct NodeBuilder<
    N: Node,
    F: Finisher<Node = N>,
    V = IndiscriminateVoterFor<N>,
    B = InMemoryBuffer<node::RoundNumOf<N>, node::CoordNumOf<N>, node::LogEntryOf<N>>,
    C = (),
> {
    kit: NodeKit<InvocationOf<N>>,
    node_id: node::NodeIdOf<N>,
    voter: V,
    communicator: <F as Finisher>::Communicator,
    snapshot: node::SnapshotFor<N>,
    buffer: B,
    context: C,
    finisher: F,

    #[cfg(feature = "tracer")]
    tracer: Option<Box<dyn Tracer<InvocationOf<N>>>>,
}

impl<N, F, V, B, C> NodeBuilder<N, F, V, B, C>
where
    N: NodeImpl + 'static,
    F: Finisher<Node = N>,
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
    ) -> NodeBuilder<N, F, V, T, C> {
        // https://github.com/rust-lang/rust/issues/86555
        NodeBuilder {
            kit: self.kit,
            node_id: self.node_id,
            voter: self.voter,
            communicator: self.communicator,
            snapshot: self.snapshot,
            buffer,
            context: self.context,
            finisher: self.finisher,

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }

    /// Sets the context the node will execute in.
    ///
    /// See [State::Context].
    pub fn in_context<T>(self, context: T) -> NodeBuilder<N, F, V, B, T> {
        // https://github.com/rust-lang/rust/issues/86555
        NodeBuilder {
            kit: self.kit,
            node_id: self.node_id,
            voter: self.voter,
            communicator: self.communicator,
            snapshot: self.snapshot,
            buffer: self.buffer,
            context,
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
    pub fn voting_with<T>(self, voter: T) -> NodeBuilder<N, F, T, B, C> {
        // https://github.com/rust-lang/rust/issues/86555
        NodeBuilder {
            kit: self.kit,
            node_id: self.node_id,
            voter,
            communicator: self.communicator,
            snapshot: self.snapshot,
            buffer: self.buffer,
            context: self.context,
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
    pub async fn spawn(self) -> SpawnResult<N, std::io::Error>
    where
        node::StateOf<N>: State<Context = C> + Send,
        C: Send,
    {
        self.spawn_using(executor::StdThread).await
    }

    /// Spawns the node in the given context.
    pub async fn spawn_using<E>(self, executor: E) -> ExecutorSpawnResult<N, E, V, B>
    where
        node::StateOf<N>: State<Context = C>,
        E: executor::Executor<node::InvocationOf<N>, V, B>,
    {
        let finisher = self.finisher;

        let receiver = self.kit.receiver;

        let state_keeper = self.kit.state_keeper.handle();

        let args = super::SpawnArgs {
            context: self.context,
            node_id: self.node_id,
            voter: self.voter,
            snapshot: self.snapshot,
            buffer: self.buffer,
            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        };

        let (initial_status, initial_participation, events, proof_of_life) =
            StateKeeper::spawn(executor, self.kit.state_keeper, args)
                .await
                .map_err(SpawnError::ExecutorError)?;

        let req_handler = RequestHandler::new(state_keeper.clone());
        let commits = Commits::new();

        let inner = NodeInner::new(
            self.node_id,
            self.communicator,
            state_keeper.clone(),
            commits,
        );

        let mut inner = Snarc::new(inner);

        let core = Core::new(
            inner.new_ref(),
            state_keeper,
            proof_of_life,
            events,
            initial_status,
            initial_participation,
            self.kit.sender,
        );

        let finished = inner.enter(|_| finisher.finish(core));

        finished
            .map(|node| (req_handler, Shell::new(node, inner.into_erased(), receiver)))
            .map_err(SpawnError::Decoration)
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
        D: Decoration<Decorated = Self::Node, Invocation = InvocationOf<Self::Node>> + 'static;
}

impl<N, F, V, B> ExtensibleNodeBuilder for NodeBuilder<N, F, V, B>
where
    N: NodeImpl + 'static,
    F: Finisher<Node = N>,
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
    type DecoratedBuilder<D: Decoration<Decorated = N> + 'static> =
        NodeBuilder<D, DecorationFinisher<D, F>, V, B>;

    fn decorated_with<D>(self, arguments: <D as Decoration>::Arguments) -> Self::DecoratedBuilder<D>
    where
        D: Decoration<Decorated = N, Invocation = InvocationOf<N>> + 'static,
    {
        NodeBuilder {
            kit: self.kit,
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot: self.snapshot,
            voter: self.voter,
            buffer: self.buffer,
            context: self.context,
            finisher: DecorationFinisher::wrap(self.finisher, arguments),

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }
}
