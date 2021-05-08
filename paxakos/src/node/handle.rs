use std::future::Future;
use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::future::FutureExt;
use futures::sink::SinkExt;

use crate::append::{AppendArgs, AppendError};
use crate::applicable::{ApplicableTo, ProjectionOf};
use crate::communicator::{Communicator, CoordNumOf, LogEntryOf, RoundNumOf};
use crate::error::ShutDown;
use crate::state::State;

use super::commits::Commit;
use super::snapshot::Snapshot;
use super::state_keeper::StateKeeperHandle;
use super::{Admin, NodeStatus};

// macros
use crate::dispatch_node_handle_req;

pub type RequestAndResponseSender<S, C> = (
    NodeHandleRequest<C>,
    oneshot::Sender<NodeHandleResponse<S, C>>,
);

/// A remote handle for a paxakos [`Node`][crate::Node].
pub struct NodeHandle<S, C>
where
    S: State,
    C: Communicator,
{
    sender: mpsc::Sender<RequestAndResponseSender<S, C>>,
    state_keeper: StateKeeperHandle<S, C>,
}

impl<S, C> Clone for NodeHandle<S, C>
where
    S: State,
    C: Communicator,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            state_keeper: self.state_keeper.clone(),
        }
    }
}

impl<S, C> NodeHandle<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    pub(crate) fn new(
        sender: mpsc::Sender<RequestAndResponseSender<S, C>>,
        state_keeper: StateKeeperHandle<S, C>,
    ) -> Self {
        Self {
            sender,
            state_keeper,
        }
    }

    pub fn status(&self) -> impl Future<Output = Result<NodeStatus, ShutDown>> {
        dispatch_node_handle_req!(self, Status).map(|r| r.ok_or(ShutDown))
    }

    pub fn prepare_snapshot(
        &self,
    ) -> impl Future<
        Output = Result<
            Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
            crate::error::PrepareSnapshotError,
        >,
    > {
        self.state_keeper.prepare_snapshot()
    }

    pub fn affirm_snapshot(
        &self,
        snapshot: Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
    ) -> impl Future<Output = Result<(), crate::error::AffirmSnapshotError>> {
        self.state_keeper.affirm_snapshot(snapshot)
    }

    pub fn install_snapshot(
        &self,
        snapshot: Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
    ) -> impl Future<Output = Result<(), crate::error::InstallSnapshotError>> {
        self.state_keeper.install_snapshot(snapshot)
    }

    pub fn read_stale(&self) -> impl Future<Output = Result<Arc<S>, crate::error::ReadStaleError>> {
        self.state_keeper.try_read_stale()
    }

    pub fn append<A: ApplicableTo<S>>(
        &self,
        applicable: A,
        args: AppendArgs<C>,
    ) -> impl Future<Output = Result<Commit<S, RoundNumOf<C>, ProjectionOf<A, S>>, AppendError>>
    {
        dispatch_node_handle_req!(self, Append, {
            log_entry: applicable.into_log_entry(),
            args,
        })
        .map(|r| {
            r.ok_or(AppendError::ShutDown)
                .and_then(|r| r)
                .map(Commit::projected)
        })
    }
}

impl<S, C> Admin for NodeHandle<S, C>
where
    S: State,
    C: Communicator,
{
    fn force_active(&self) -> futures::future::BoxFuture<'static, Result<bool, ShutDown>> {
        self.state_keeper.force_active().boxed()
    }
}

impl<S, C> std::fmt::Debug for NodeHandle<S, C>
where
    S: State,
    C: Communicator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "paxakos::NodeHandle")
    }
}

#[derive(Debug)]
pub enum NodeHandleRequest<C: Communicator> {
    Status,

    Append {
        log_entry: Arc<LogEntryOf<C>>,
        args: AppendArgs<C>,
    },
}

#[derive(Debug)]
pub enum NodeHandleResponse<S: State, C: Communicator> {
    Status(NodeStatus),

    Append(Result<Commit<S, RoundNumOf<C>>, AppendError>),
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
}
