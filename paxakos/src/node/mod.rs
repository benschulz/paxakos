//! A [`Node`] is a member in a distributed paxakos cluster.

pub mod builder;
mod commits;
mod core;
mod handle;
mod info;
mod inner;
mod req_handler;
mod shell;
mod shutdown;
mod snapshot;
mod state_keeper;
mod status;

use std::sync::Arc;

use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::future::LocalBoxFuture;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::buffer::Buffer;
use crate::communicator::Communicator;
use crate::error::Disoriented;
use crate::error::ShutDown;
use crate::error::ShutDownOr;
use crate::invocation;
use crate::invocation::Invocation;
use crate::retry::RetryPolicy;
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
use crate::voting::IndiscriminateVoter;
use crate::Event;

pub use self::core::Core;
pub use builder::NodeBuilder;
pub use commits::Commit;
pub use handle::NodeHandle;
pub use info::NodeInfo;
pub use req_handler::RequestHandler;
pub use shell::Shell;
pub use shutdown::DefaultShutdown;
pub use shutdown::Shutdown;
pub use snapshot::Snapshot;
pub use status::NodeStatus;

use state_keeper::StateKeeperKit;

/// Shorthand to extract invocation's `Abstain` type out of `N`.
pub type AbstainOf<N> = invocation::AbstainOf<InvocationOf<N>>;
/// Shorthand to extract invocation's `CommunicationError` type out of `N`.
pub type CommunicationErrorOf<N> = invocation::CommunicationErrorOf<InvocationOf<N>>;
/// Shorthand to extract `Communicator` type out of `N`.
pub type CommunicatorOf<N> = <N as Node>::Communicator;
/// Shorthand to extract state's `Context` type out of `N`.
pub type ContextOf<N> = invocation::ContextOf<InvocationOf<N>>;
/// Shorthand to extract invocation's `CoordNum` type out of `N`.
pub type CoordNumOf<N> = invocation::CoordNumOf<InvocationOf<N>>;
/// Shorthand to extract invocation's `Event` type out of `N`.
pub type EventOf<N> = invocation::EventOf<InvocationOf<N>>;
/// Shorthand to extract `Invocation` type out of `N`.
pub type InvocationOf<N> = <N as Node>::Invocation;
/// Shorthand to extract state's `LogEntry` type out of `N`.
pub type LogEntryOf<N> = invocation::LogEntryOf<InvocationOf<N>>;
/// Shorthand to extract log entry `Id` type out of `N`.
pub type LogEntryIdOf<N> = invocation::LogEntryIdOf<InvocationOf<N>>;
/// Shorthand to extract invocation's `Nay` type out of `N`.
pub type NayOf<N> = invocation::NayOf<InvocationOf<N>>;
/// Shorthand to extract invocation's `Node` type (`impl NodeInfo`) out of `N`.
pub type NodeOf<N> = invocation::NodeOf<InvocationOf<N>>;
/// Shorthand to extract state's `Outcome` type out of `N`.
pub type NodeIdOf<N> = invocation::NodeIdOf<InvocationOf<N>>;
/// Shorthand to extract node (`impl NodeInfo`) `Id` type out of `N`.
pub type OutcomeOf<N> = invocation::OutcomeOf<InvocationOf<N>>;
/// Shorthand to extract invocation's `RoundNum` type out of `N`.
pub type RoundNumOf<N> = invocation::RoundNumOf<InvocationOf<N>>;
/// Shorthand to extract `Shutdown` type out of `N`.
pub type ShutdownOf<N> = <N as Node>::Shutdown;
/// Shorthand to extract invocation's `State` type out of `N`.
pub type StateOf<N> = invocation::StateOf<InvocationOf<N>>;
/// Shorthand to extract invocation's `Yea` type out of `N`.
pub type YeaOf<N> = invocation::YeaOf<InvocationOf<N>>;

/// Invokes `Acceptance` type constructor so as to be compatible with `N`.
pub type AcceptanceFor<N> = invocation::AcceptanceFor<InvocationOf<N>>;
/// Invokes `Result` type constructor so as to be compatible with `N`'s
/// `append(…) method`.
pub type AppendResultFor<N, A, R> = Result<CommitFor<N, A>, <R as RetryPolicy>::Error>;
/// Invokes `Result` type constructor so as to be compatible with `N`'s
/// `append_static(…) method`.
pub type StaticAppendResultFor<N, A, R> = Result<CommitFor<N, A>, <R as RetryPolicy>::StaticError>;
/// Invokes `Result` type constructor so as to be compatible with `N`'s
/// `append_impl(…) method`.
pub type ImplAppendResultFor<N, A, R> =
    Result<CommitFor<N, A>, ShutDownOr<<R as RetryPolicy>::Error>>;
/// Invokes `Commit` type constructor so as to be compatible with `N`.
pub type CommitFor<N, A = LogEntryOf<N>> = invocation::CommitFor<InvocationOf<N>, A>;
/// Invokes `Conflict` type constructor so as to be compatible with `N`.
pub type ConflictFor<N> = invocation::ConflictFor<InvocationOf<N>>;
/// Invokes `Event` type constructor so as to be compatible with `N`.
pub type EventFor<N> = Event<InvocationOf<N>>;
/// Invokes `NodeHandle` type constructor so as to be compatible with `N`.
pub type HandleFor<N> = NodeHandle<InvocationOf<N>>;
/// Invokes `IndiscriminateVoter` type constructor so as to be compatible with
/// `N`.
pub type IndiscriminateVoterFor<N> =
    IndiscriminateVoter<StateOf<N>, RoundNumOf<N>, CoordNumOf<N>, AbstainOf<N>, YeaOf<N>, NayOf<N>>;
/// Invokes `Promise` type constructor so as to be compatible with `N`.
pub type PromiseFor<N> = invocation::PromiseFor<InvocationOf<N>>;
/// Invokes `RequestHandler` type constructor so as to be compatible with `N`.
pub type RequestHandlerFor<N> = RequestHandler<InvocationOf<N>>;
/// Invokes `Snapshot` type constructor so as to be compatible with `N`.
pub type SnapshotFor<N> = invocation::SnapshotFor<InvocationOf<N>>;
/// Invokes `Vote` type constructor so as to be compatible with `N`.
pub type VoteFor<N> = invocation::VoteFor<InvocationOf<N>>;

/// Node that participates in a cluster.
pub trait Node: Sized {
    /// Parametrization of the paxakos algorithm.
    type Invocation: Invocation;

    /// Type of communicator this node uses.
    type Communicator: Communicator<
        Node = invocation::NodeOf<Self::Invocation>,
        RoundNum = invocation::RoundNumOf<Self::Invocation>,
        CoordNum = invocation::CoordNumOf<Self::Invocation>,
        LogEntry = invocation::LogEntryOf<Self::Invocation>,
        Error = invocation::CommunicationErrorOf<Self::Invocation>,
        Yea = invocation::YeaOf<Self::Invocation>,
        Nay = invocation::NayOf<Self::Invocation>,
        Abstain = invocation::AbstainOf<Self::Invocation>,
    >;

    /// Type that will perform graceful shutdown if requsted.
    type Shutdown: Shutdown<Invocation = Self::Invocation>;

    /// This node's identifier.
    fn id(&self) -> NodeIdOf<Self>;

    /// Node's current status.
    fn status(&self) -> NodeStatus;

    /// Node's current mode of participation.
    fn participation(&self) -> Participation<RoundNumOf<Self>>;

    /// Polls the node's event stream.
    ///
    /// It is important to poll the node's event stream because it implicitly
    /// drives the actions that keep the node up to date.
    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<EventFor<Self>>;

    /// Returns a future that polls this node for events until the next one is
    /// returned.
    fn next_event(&mut self) -> NextEvent<'_, Self> {
        NextEvent(self)
    }

    /// Returns a [handle][NodeHandle] for this node.
    ///
    /// A node handle can be freely sent between threads.
    fn handle(&self) -> NodeHandle<Self::Invocation>;

    /// Requests that snapshot of the node's current state be taken.
    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<'static, Result<SnapshotFor<Self>, crate::error::PrepareSnapshotError>>;

    /// Affirms that the given snapshot was written to persistent storage.
    ///
    /// Currently does nothing.
    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>>;

    /// Requests that given snapshot be installed.
    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>>;

    /// Reads the node's current state.
    ///
    /// As the name implies the state may be stale, i.e. other node's may have
    /// advanced the shared state without this node being aware.
    fn read_stale(&self) -> LocalBoxFuture<'_, Result<Arc<StateOf<Self>>, Disoriented>>;

    /// Appends `applicable` to the shared log.
    fn append<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'_, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>;

    /// Appends `applicable` to the shared log.
    fn append_static<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'static, StaticAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>;

    /// Begins a graceful shutdown of this node.
    fn shut_down(self) -> Self::Shutdown;
}

/// Exposes "plumbing" API relevant to decorations.
pub trait NodeImpl: Node {
    /// Appends `applicable` to the shared log.
    fn append_impl<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'static, ImplAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>;
}

/// Future returned by [`Node::next_event`].
pub struct NextEvent<'a, N: ?Sized>(&'a mut N);

impl<'a, N> std::future::Future for NextEvent<'a, N>
where
    N: Node,
{
    type Output = EventFor<N>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.poll_events(cx)
    }
}

/// Exposes seldomly used administrative operations.
pub trait Admin {
    /// Forces node out of passive participation.
    ///
    /// # Soundness
    ///
    /// Forcing a node out of passive mode may cause it to back out of
    /// obligations it took on previously. The only exception are nodes
    /// which are (re-)joining a cluster (see [joining_with]).
    ///
    /// [joining_with]:
    /// builder::NodeBuilderWithNodeIdAndCommunicator::joining_with
    fn force_active(&self) -> BoxFuture<'static, Result<bool, ShutDown>>;
}

pub(crate) struct SpawnArgs<I, V, B>
where
    I: Invocation,
    B: Buffer<
        RoundNum = invocation::RoundNumOf<I>,
        CoordNum = invocation::CoordNumOf<I>,
        Entry = invocation::LogEntryOf<I>,
    >,
{
    pub context: invocation::ContextOf<I>,
    pub node_id: invocation::NodeIdOf<I>,
    pub voter: V,
    pub snapshot: Option<invocation::SnapshotFor<I>>,
    pub force_passive: bool,
    pub buffer: B,
    #[cfg(feature = "tracer")]
    pub tracer: Option<Box<dyn Tracer<I>>>,
}

/// Reflects a [`Node`]'s possible modes of participation.
///
/// When a node shuts down, it returns a final snapshot. This snapshot can be
/// used to restart the node. When restarted in such a fashion, the node is
/// aware of any and all commitments it made previously.
///
/// Conversely, when a node crashes and restarts off some other snapshot, it is
/// unaware of any commitments it made prior to crashing. To prevent the node
/// from inadvertantly breaking any such commitments and introducing
/// inconsistencies, it is started in passive mode.
///
/// While in passive mode, a node does not vote and does not accept any entries.
/// It remains in this mode until all rounds it may have participated in before
/// have been settled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Participation<R> {
    /// The node will actively participate in any round.
    Active,
    /// The node will only participate actively in the given or later rounds.
    ///
    /// When the given round becomes the next round, i.e. an entry was applied
    /// for its preceding round, the node will transition into `Active`
    /// participation.
    PartiallyActive(R),
    /// The node is fully passive and won't participate in any round.
    ///
    /// The node will observe communications and determine a lower bound for the
    /// rounds in which it can participate without causing inconsistencies. Once
    /// the lower bound is found, an [`Event::Activate`] event will be emitted
    /// and the node transitions into `PartiallyActive` participation.
    Passive,
}

/// Allows getting a `NodeHandle` before the `Node` itself is built.
pub struct NodeKit<I: Invocation> {
    state_keeper: StateKeeperKit<I>,
    sender: mpsc::Sender<handle::RequestAndResponseSender<I>>,
    receiver: mpsc::Receiver<handle::RequestAndResponseSender<I>>,
}

impl<I: Invocation> NodeKit<I> {
    /// Constructs a new node kit.
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(16);

        Self {
            state_keeper: StateKeeperKit::new(),
            sender,
            receiver,
        }
    }

    /// Returns a handle for the node yet to be created.
    pub fn handle(&self) -> NodeHandle<I> {
        NodeHandle::new(self.sender.clone(), self.state_keeper.handle())
    }
}

impl<I: Invocation> Default for NodeKit<I> {
    fn default() -> Self {
        Self::new()
    }
}
