use std::future::Future;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::FutureExt;
use futures::sink::SinkExt;

use crate::append::AppendArgs;
use crate::append::AppendError;
use crate::applicable::ApplicableTo;
use crate::error::ShutDown;
use crate::invocation::CommitFor;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::SnapshotFor;
use crate::invocation::StateOf;

use super::commits::Commit;
use super::state_keeper::StateKeeperHandle;
use super::Admin;
use super::NodeStatus;

pub type RequestAndResponseSender<I> =
    (NodeHandleRequest<I>, oneshot::Sender<NodeHandleResponse<I>>);

/// A remote handle for a paxakos [`Node`][crate::Node].
pub struct NodeHandle<I: Invocation> {
    sender: mpsc::Sender<RequestAndResponseSender<I>>,
    state_keeper: StateKeeperHandle<I>,
}

impl<I: Invocation> Clone for NodeHandle<I> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            state_keeper: self.state_keeper.clone(),
        }
    }
}

macro_rules! dispatch_node_handle_req {
    ($self:ident, $name:ident) => {{
        let req = NodeHandleRequest::$name;

        let mut sender = $self.sender.clone();
        let (s, r) = oneshot::channel();

        async move {
            let send_res = sender.send((req, s)).await;

            match (send_res, r.await) {
                (Ok(_), Ok(NodeHandleResponse::$name(r))) => Some(r),
                (Err(_), _) | (_, Err(_)) => None,
                _ => unreachable!(),
            }
        }
    }};

    ($self:ident, $name:ident, $args:tt) => {{
        let req = NodeHandleRequest::$name $args;

        let mut sender = $self.sender.clone();
        let (s, r) = oneshot::channel();

        async move {
            let send_res = sender.send((req, s)).await;

            match (send_res, r.await) {
                (Ok(_), Ok(NodeHandleResponse::$name(r))) => Some(r),
                (Err(_), _) | (_, Err(_)) => None,
                _ => unreachable!(),
            }
        }
    }};
}

impl<I: Invocation> NodeHandle<I> {
    pub(crate) fn new(
        sender: mpsc::Sender<RequestAndResponseSender<I>>,
        state_keeper: StateKeeperHandle<I>,
    ) -> Self {
        Self {
            sender,
            state_keeper,
        }
    }

    /// Node's current status.
    pub fn status(&self) -> impl Future<Output = Result<NodeStatus, ShutDown>> {
        dispatch_node_handle_req!(self, Status).map(|r| r.ok_or(ShutDown))
    }

    /// Requests that snapshot of the node's current state be taken.
    pub fn prepare_snapshot(
        &self,
    ) -> impl Future<Output = Result<SnapshotFor<I>, crate::error::PrepareSnapshotError>> {
        self.state_keeper.prepare_snapshot()
    }

    /// Affirms that the given snapshot was written to persistent storage.
    ///
    /// Currently does nothing.
    pub fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<I>,
    ) -> impl Future<Output = Result<(), crate::error::AffirmSnapshotError>> {
        self.state_keeper.affirm_snapshot(snapshot)
    }

    /// Requests that given snapshot be installed.
    pub fn install_snapshot(
        &self,
        snapshot: SnapshotFor<I>,
    ) -> impl Future<Output = Result<(), crate::error::InstallSnapshotError>> {
        self.state_keeper.install_snapshot(snapshot)
    }

    /// Reads the node's current state.
    ///
    /// As the name implies the state may be stale, i.e. other node's may have
    /// advanced the shared state without this node being aware.
    pub fn read_stale(
        &self,
    ) -> impl Future<Output = Result<Arc<StateOf<I>>, crate::error::ReadStaleError>> {
        self.state_keeper.try_read_stale()
    }

    /// Appends `applicable` to the shared log.
    pub fn append<A: ApplicableTo<StateOf<I>>, P: Into<AppendArgs<I>>>(
        &self,
        applicable: A,
        args: P,
    ) -> impl Future<Output = Result<CommitFor<I, A>, AppendError<I>>> {
        dispatch_node_handle_req!(self, Append, {
            log_entry: applicable.into_log_entry(),
            args: args.into(),
        })
        .map(|r| {
            r.ok_or(AppendError::ShutDown)
                .and_then(|r| r)
                .map(Commit::projected)
        })
    }
}

impl<I: Invocation> Admin for NodeHandle<I> {
    fn force_active(&self) -> futures::future::BoxFuture<'static, Result<bool, ShutDown>> {
        self.state_keeper.force_active().boxed()
    }
}

impl<I: Invocation> std::fmt::Debug for NodeHandle<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "paxakos::NodeHandle")
    }
}

pub enum NodeHandleRequest<I: Invocation> {
    Status,

    Append {
        log_entry: Arc<LogEntryOf<I>>,
        args: AppendArgs<I>,
    },
}

#[derive(Debug)]
pub enum NodeHandleResponse<I: Invocation> {
    Status(NodeStatus),

    Append(Result<CommitFor<I>, AppendError<I>>),
}
