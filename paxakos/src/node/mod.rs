pub mod builder;
mod commits;
mod handle;
mod info;
mod inner;
mod kernel;
mod req_handler;
mod shutdown;
mod snapshot;
mod state_keeper;
mod status;

use std::sync::Arc;

use futures::future::{BoxFuture, LocalBoxFuture};

use crate::append::{AppendArgs, AppendError};
use crate::applicable::{ApplicableTo, ProjectionOf};
use crate::communicator::Communicator;
use crate::log::LogKeeping;
use crate::state::ContextOf;
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
use crate::{CoordNum, Event, RoundNum, State};

pub use builder::NodeBuilder;
pub use commits::Commit;
pub use handle::NodeHandle;
pub use info::NodeInfo;
pub use kernel::NodeKernel;
pub use req_handler::{RequestHandler, RequestHandlerFor};
pub use shutdown::{DefaultShutdown, Shutdown};
pub use snapshot::{Snapshot, SnapshotFor};
pub use status::NodeStatus;

pub type StateOf<N> = <N as Node>::State;
pub type CommunicatorOf<N> = <N as Node>::Communicator;

pub type RoundNumOf<N> = crate::communicator::RoundNumOf<CommunicatorOf<N>>;
pub type CoordNumOf<N> = crate::communicator::CoordNumOf<CommunicatorOf<N>>;

pub type LogEntryOf<N> = crate::state::LogEntryOf<StateOf<N>>;
pub type LogEntryIdOf<N> = crate::state::LogEntryIdOf<StateOf<N>>;
pub type NodeOf<N> = crate::state::NodeOf<StateOf<N>>;
pub type NodeIdOf<N> = crate::state::NodeIdOf<StateOf<N>>;
pub type EventOf<N> = EventFor<StateOf<N>>;

pub type CommitFor<N, A> = Commit<StateOf<N>, RoundNumOf<N>, ProjectionOf<A, StateOf<N>>>;
pub type EventFor<N> = Event<StateOf<N>, RoundNumOf<N>, CoordNumOf<N>>;

pub fn builder() -> builder::NodeBuilderBlank {
    builder::NodeBuilderBlank::new()
}

pub trait Node: Sized {
    type State: State<
        LogEntry = <Self::Communicator as Communicator>::LogEntry,
        Node = <Self::Communicator as Communicator>::Node,
    >;
    type Communicator: Communicator;

    type Shutdown: Shutdown;

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

    fn handle(&self) -> NodeHandle<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>;

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<'static, Result<SnapshotFor<Self>, crate::error::PrepareSnapshotError>>;

    fn affirm_snapshot(
        &self,
        snapshot: Snapshot<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>>;

    fn install_snapshot(
        &self,
        snapshot: Snapshot<Self::State, RoundNumOf<Self>, CoordNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>>;

    fn read_stale(&self) -> LocalBoxFuture<'static, Result<Arc<Self::State>, ()>>;

    fn append<A: ApplicableTo<Self::State> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<RoundNumOf<Self>>,
    ) -> LocalBoxFuture<'static, Result<CommitFor<Self, A>, AppendError>>;

    fn shut_down(self) -> Self::Shutdown;
}

pub struct NextEvent<'a, N: ?Sized>(&'a mut N);

impl<'a, N> std::future::Future for NextEvent<'a, N>
where
    N: Node,
{
    type Output = Event<StateOf<N>, RoundNumOf<N>, CoordNumOf<N>>;

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
    fn force_active(&self) -> BoxFuture<'static, Result<bool, ()>>;
}

pub struct SpawnArgs<S: State, R: RoundNum, C: CoordNum> {
    pub context: ContextOf<S>,
    pub working_dir: Option<std::path::PathBuf>,
    pub snapshot: Option<Snapshot<S, R, C>>,
    pub participation: Participation<R>,
    pub log_keeping: LogKeeping,
    #[cfg(feature = "tracer")]
    pub tracer: Option<Box<dyn Tracer<R, C, crate::state::LogEntryIdOf<S>>>>,
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
