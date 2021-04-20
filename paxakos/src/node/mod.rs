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

    /// Polls the node's event stream.
    ///
    /// It is important to poll the node's event stream because it implicitly
    /// drives the actions that keep the node up to date.
    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<EventFor<Self>>;

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

/// Exposes seldomly used administrative operations.
pub trait Admin {
    /// Forces node out of passive participation.
    ///
    /// # Soundness
    ///
    /// Forcing a node out of passive mode may cause it to go back out of
    /// obligations it took on previously. The only exception are nodes which
    /// are (re-)joining a cluster (see [joining_with]).
    ///
    /// [joining_with]:
    /// builder::NodeBuilderWithNodeIdAndWorkingDirAndCommunicator::joining_with
    fn force_active(&self) -> BoxFuture<'static, Result<bool, ()>>;
}

pub struct SpawnArgs<S: State, R: RoundNum, C: CoordNum> {
    pub context: ContextOf<S>,
    pub working_dir: Option<std::path::PathBuf>,
    pub snapshot: Option<Snapshot<S, R, C>>,
    pub participation: Participaction,
    pub log_keeping: LogKeeping,
    #[cfg(feature = "tracer")]
    pub tracer: Option<Box<dyn Tracer<R, C, crate::state::LogEntryIdOf<S>>>>,
}

// TODO expose current mode in Node/NodeHandle
pub enum Participaction {
    Active,
    Passive,
}
