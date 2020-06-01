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
use crate::communicator::{Communicator, CoordNumOf, RoundNumOf};
use crate::state::LogEntryOf;
use crate::{Event, ShutdownEvent, State};

pub use builder::NodeBuilder;
pub use commits::Commit;
pub use handle::NodeHandle;
pub use info::NodeInfo;
pub use kernel::NodeKernel;
pub use req_handler::RequestHandler;
pub use shutdown::{DefaultShutdown, Shutdown};
pub use snapshot::Snapshot;
pub use status::NodeStatus;

pub type StateOf<N> = <N as Node>::State;
pub type CommunicatorOf<N> = <N as Node>::Communicator;

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

    fn status(&self) -> NodeStatus;

    /// Polls the node's event stream.
    ///
    /// It is important to poll the node's event stream because it implicitly
    /// drives the actions that keep the node up to date.
    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Event<Self::State, RoundNumOf<Self::Communicator>>>;

    fn handle(
        &self,
    ) -> NodeHandle<Self::State, RoundNumOf<Self::Communicator>, CoordNumOf<Self::Communicator>>;

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<
        'static,
        Result<
            Snapshot<Self::State, RoundNumOf<Self::Communicator>, CoordNumOf<Self::Communicator>>,
            crate::error::PrepareSnapshotError,
        >,
    >;

    fn affirm_snapshot(
        &self,
        snapshot: Snapshot<
            Self::State,
            RoundNumOf<Self::Communicator>,
            CoordNumOf<Self::Communicator>,
        >,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>>;

    fn install_snapshot(
        &self,
        snapshot: Snapshot<
            Self::State,
            RoundNumOf<Self::Communicator>,
            CoordNumOf<Self::Communicator>,
        >,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>>;

    fn read_stale(&self) -> LocalBoxFuture<'static, Result<Arc<Self::State>, ()>>;

    // TODO introduce trait ApplicableTo<S: State> which defines
    //  - `into_log_entry(self) -> Arc<<S as State>::LogEntry>`
    //  - `project_output(output: <S as State>::Output) -> Self::Projected` (to
    //    narrow the output type)
    fn append(
        &self,
        log_entry: impl Into<Arc<LogEntryOf<Self::State>>>,
        args: AppendArgs<RoundNumOf<Self::Communicator>>,
    ) -> LocalBoxFuture<'static, Result<Commit<Self::State>, AppendError>>;

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

pub enum Participaction {
    Active,
    Passive,
}
