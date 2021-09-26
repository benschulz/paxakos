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
use crate::invocation;
use crate::invocation::Invocation;
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
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

pub type AbstainOf<N> = invocation::AbstainOf<InvocationOf<N>>;
pub type CommunicationErrorOf<N> = invocation::CommunicationErrorOf<InvocationOf<N>>;
pub type CommunicatorOf<N> = <N as Node>::Communicator;
pub type ContextOf<N> = invocation::ContextOf<InvocationOf<N>>;
pub type CoordNumOf<N> = invocation::CoordNumOf<InvocationOf<N>>;
pub type EventOf<N> = invocation::EventOf<InvocationOf<N>>;
pub type InvocationOf<N> = <N as Node>::Invocation;
pub type LogEntryOf<N> = invocation::LogEntryOf<InvocationOf<N>>;
pub type LogEntryIdOf<N> = invocation::LogEntryIdOf<InvocationOf<N>>;
pub type NayOf<N> = invocation::NayOf<InvocationOf<N>>;
pub type NodeOf<N> = invocation::NodeOf<InvocationOf<N>>;
pub type NodeIdOf<N> = invocation::NodeIdOf<InvocationOf<N>>;
pub type OutcomeOf<N> = invocation::OutcomeOf<InvocationOf<N>>;
pub type RoundNumOf<N> = invocation::RoundNumOf<InvocationOf<N>>;
pub type ShutdownOf<N> = <N as Node>::Shutdown;
pub type StateOf<N> = invocation::StateOf<InvocationOf<N>>;
pub type YeaOf<N> = invocation::YeaOf<InvocationOf<N>>;

pub type AcceptanceFor<N> = invocation::AcceptanceFor<InvocationOf<N>>;
pub type AppendResultFor<N, A = LogEntryOf<N>> = invocation::AppendResultFor<InvocationOf<N>, A>;
pub type CommitFor<N, A = LogEntryOf<N>> = invocation::CommitFor<InvocationOf<N>, A>;
pub type ConflictFor<N> = invocation::ConflictFor<InvocationOf<N>>;
pub type EventFor<N> = Event<InvocationOf<N>>;
pub type HandleFor<N> = NodeHandle<InvocationOf<N>>;
pub type PromiseFor<N> = invocation::PromiseFor<InvocationOf<N>>;
pub type RequestHandlerFor<N> = RequestHandler<InvocationOf<N>>;
pub type SnapshotFor<N> = invocation::SnapshotFor<InvocationOf<N>>;
pub type VoteFor<N> = invocation::VoteFor<InvocationOf<N>>;

pub trait Node: Sized {
    type Invocation: Invocation;
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
    type Shutdown: Shutdown<Invocation = Self::Invocation>;

    fn id(&self) -> NodeIdOf<Self>;

    fn status(&self) -> NodeStatus;

    fn participation(&self) -> Participation<RoundNumOf<Self>>;

    /// Polls the node's event stream.
    ///
    /// It is important to poll the node's event stream because it implicitly
    /// drives the actions that keep the node up to date.
    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<EventFor<Self>>;

    fn next_event(&mut self) -> NextEvent<'_, Self> {
        NextEvent(self)
    }

    fn handle(&self) -> NodeHandle<Self::Invocation>;

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<'static, Result<SnapshotFor<Self>, crate::error::PrepareSnapshotError>>;

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>>;

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>>;

    fn read_stale(&self) -> LocalBoxFuture<'_, Result<Arc<StateOf<Self>>, Disoriented>>;

    fn append<A: ApplicableTo<StateOf<Self>> + 'static, P: Into<AppendArgs<Self::Invocation>>>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'static, AppendResultFor<Self, A>>;

    fn shut_down(self) -> Self::Shutdown;
}

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
    /// builder::NodeBuilderWithNodeIdAndWorkingDirAndCommunicator::joining_with
    fn force_active(&self) -> BoxFuture<'static, Result<bool, ShutDown>>;
}

pub struct SpawnArgs<I, V, B>
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
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(16);

        Self {
            state_keeper: StateKeeperKit::new(),
            sender,
            receiver,
        }
    }

    pub fn handle(&self) -> NodeHandle<I> {
        NodeHandle::new(self.sender.clone(), self.state_keeper.handle())
    }
}

impl<I: Invocation> Default for NodeKit<I> {
    fn default() -> Self {
        Self::new()
    }
}
