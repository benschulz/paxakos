use std::future::Future;
use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::future::FutureExt;
use futures::sink::SinkExt;

use crate::append::{AppendArgs, AppendError};
use crate::state::{LogEntryOf, State};
use crate::{CoordNum, RoundNum};

use super::commits::Commit;
use super::snapshot::Snapshot;
use super::state_keeper::StateKeeperHandle;
use super::{Admin, NodeStatus};

// macros
use crate::dispatch_node_handle_req;

/// A remote handle for a paxakos [`Node`][crate::Node].
#[derive(Clone)]
pub struct NodeHandle<S, R, C>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
{
    sender: mpsc::Sender<(
        NodeHandleRequest<S, R>,
        oneshot::Sender<NodeHandleResponse<S>>,
    )>,
    state_keeper: StateKeeperHandle<S, R, C>,
}

impl<S, R, C> NodeHandle<S, R, C>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
{
    pub(crate) fn new(
        sender: mpsc::Sender<(
            NodeHandleRequest<S, R>,
            oneshot::Sender<NodeHandleResponse<S>>,
        )>,
        state_keeper: StateKeeperHandle<S, R, C>,
    ) -> Self {
        Self {
            sender,
            state_keeper,
        }
    }

    pub fn status(&self) -> impl Future<Output = Result<NodeStatus, ()>> {
        dispatch_node_handle_req!(self, Status).map(|r| r.ok_or(()))
    }

    pub fn prepare_snapshot(
        &self,
    ) -> impl Future<Output = Result<Snapshot<S, R, C>, crate::error::PrepareSnapshotError>> {
        self.state_keeper.prepare_snapshot()
    }

    pub fn affirm_snapshot(
        &self,
        snapshot: Snapshot<S, R, C>,
    ) -> impl Future<Output = Result<(), crate::error::AffirmSnapshotError>> {
        self.state_keeper.affirm_snapshot(snapshot)
    }

    pub fn install_snapshot(
        &self,
        snapshot: Snapshot<S, R, C>,
    ) -> impl Future<Output = Result<(), crate::error::InstallSnapshotError>> {
        self.state_keeper.install_snapshot(snapshot)
    }

    pub fn read_stale(&self) -> impl Future<Output = Result<Arc<S>, crate::error::ReadStaleError>> {
        self.state_keeper.try_read_stale()
    }

    pub fn append<I: Into<Arc<LogEntryOf<S>>>>(
        &self,
        log_entry: I,
        args: AppendArgs<R>,
    ) -> impl Future<Output = Result<Commit<S>, AppendError>> {
        dispatch_node_handle_req!(self, Append, {
            log_entry: log_entry.into(),
            args,
        })
        .map(|r| r.ok_or(AppendError::ShutDown).and_then(|r| r))
    }
}

impl<S, R, C> Admin for NodeHandle<S, R, C>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
{
    fn force_active(&self) -> futures::future::BoxFuture<'static, Result<bool, ()>> {
        self.state_keeper.force_active().boxed()
    }
}

impl<S, R, C> std::fmt::Debug for NodeHandle<S, R, C>
where
    S: State,
    R: RoundNum,
    C: CoordNum,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "paxakos::NodeHandle")
    }
}

#[derive(Debug)]
pub enum NodeHandleRequest<S: State, R: RoundNum> {
    Status,

    Append {
        log_entry: Arc<LogEntryOf<S>>,
        args: AppendArgs<R>,
    },
}

#[derive(Debug)]
pub enum NodeHandleResponse<S: State> {
    Status(NodeStatus),

    Append(Result<Commit<S>, AppendError>),
}

mod macros {
    #[doc(hidden)]
    #[macro_export]
    macro_rules! dispatch_node_handle_req {
        ($self:ident, $name:ident) => {{
            let req = NodeHandleRequest::$name;

            let mut sender = $self.sender.clone();
            let (s, r) = oneshot::channel();

            async move {
                match (sender.send((req, s)).await, r.await) {
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
                match (sender.send((req, s)).await, r.await) {
                    (Ok(_), Ok(NodeHandleResponse::$name(r))) => Some(r),
                    (Err(_), _) | (_, Err(_)) => None,
                    _ => unreachable!(),
                }
            }
        }};
    }
}
