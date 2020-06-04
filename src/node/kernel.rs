use std::rc::Rc;
use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};

use crate::append::{AppendArgs, AppendError};
use crate::communicator::{Communicator, CoordNumOf, RoundNumOf};
use crate::event::{Event, ShutdownEvent};
use crate::log::LogKeeping;
#[cfg(feature = "tracer")]
use crate::state::LogEntryIdOf;
use crate::state::{ContextOf, LogEntryOf, NodeIdOf, State};
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;

use super::commits::{Commit, Commits};
use super::handle::{NodeHandleRequest, NodeHandleResponse};
use super::inner::NodeInner;
use super::shutdown::DefaultShutdown;
use super::snapshot::Snapshot;
use super::state_keeper::{EventStream, ProofOfLife, StateKeeper, StateKeeperHandle};
use super::{Node, NodeHandle, NodeStatus, Participaction, RequestHandler};

/// The default [`Node`][crate::Node] implementation.
// TODO a better name may be needed…
pub struct NodeKernel<S, C>
where
    S: State,
    C: Communicator,
{
    inner: Rc<NodeInner<S, C>>,
    state_keeper: StateKeeperHandle<S, RoundNumOf<C>, CoordNumOf<C>>,
    proof_of_life: ProofOfLife,
    commits: Commits,
    events: EventStream<S, RoundNumOf<C>>,
    status: NodeStatus,
    handle_send: mpsc::Sender<(
        NodeHandleRequest<S, RoundNumOf<C>>,
        oneshot::Sender<NodeHandleResponse<S>>,
    )>,
    handle_recv: mpsc::Receiver<(
        NodeHandleRequest<S, RoundNumOf<C>>,
        oneshot::Sender<NodeHandleResponse<S>>,
    )>,
    handle_appends: FuturesUnordered<LocalBoxFuture<'static, ()>>,
}

impl<S, C> Node for NodeKernel<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    type State = S;
    type Communicator = C;

    type Shutdown = DefaultShutdown<S, C>;

    fn status(&self) -> NodeStatus {
        self.status
    }

    /// Polls the node's status, yielding `Ready` on every change.
    ///
    /// It is important to poll the node's event stream because it implicitly
    /// drives the actions that keep the node up to date.
    fn poll_events(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Event<S, RoundNumOf<C>>> {
        self.poll_handle_reqs(cx);

        while let std::task::Poll::Ready(Some(())) = self.handle_appends.poll_next_unpin(cx) {
            // keep going
        }

        while let std::task::Poll::Ready(Some(())) = self.commits.poll_next(cx) {
            // keep going
        }

        let result = self
            .events
            .poll_next_unpin(cx)
            .map(|e| e.expect("Event stream ended"))
            .map(|e| match e {
                ShutdownEvent::Regular(e) => e,
                ShutdownEvent::Last { .. } => unreachable!(),
            });

        if let std::task::Poll::Ready(Event::StatusChange { new_status, .. }) = result {
            self.status = new_status;
        }

        result
    }

    fn handle(&self) -> NodeHandle<S, RoundNumOf<C>, CoordNumOf<C>> {
        NodeHandle::new(self.handle_send.clone(), self.state_keeper.clone())
    }

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<
        'static,
        Result<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>, crate::error::PrepareSnapshotError>,
    > {
        self.state_keeper.prepare_snapshot().boxed_local()
    }

    fn affirm_snapshot(
        &self,
        snapshot: Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.state_keeper.affirm_snapshot(snapshot).boxed_local()
    }

    fn install_snapshot(
        &self,
        snapshot: Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.state_keeper.install_snapshot(snapshot).boxed_local()
    }

    fn append(
        &self,
        log_entry: impl Into<Arc<LogEntryOf<S>>>,
        args: AppendArgs<RoundNumOf<C>>,
    ) -> LocalBoxFuture<'static, Result<Commit<S>, AppendError>> {
        Rc::clone(&self.inner)
            .append(log_entry.into(), args)
            .boxed_local()
    }

    fn read_stale(&self) -> LocalBoxFuture<'static, Result<Arc<S>, ()>> {
        self.state_keeper
            .read_stale(&self.proof_of_life)
            .boxed_local()
    }

    fn shut_down(self) -> Self::Shutdown {
        let state_keeper = self.state_keeper;
        let proof_of_life = self.proof_of_life;
        let events = self.events;
        let commits = self.commits;

        let trigger = state_keeper.shut_down(proof_of_life).fuse().boxed_local();

        DefaultShutdown::new(trigger, events, commits)
    }
}

impl<S, C> NodeKernel<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    pub(crate) async fn new(
        context: ContextOf<S>,
        working_dir: Option<std::path::PathBuf>,
        id: NodeIdOf<S>,
        communicator: C,
        snapshot: Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>,
        participation: Participaction,
        log_keeping: LogKeeping,
        #[cfg(feature = "tracer")] tracer: Option<
            Box<dyn Tracer<RoundNumOf<C>, CoordNumOf<C>, LogEntryIdOf<S>>>,
        >,
    ) -> Result<
        (
            RequestHandler<S, RoundNumOf<C>, CoordNumOf<C>>,
            NodeKernel<S, C>,
        ),
        crate::error::SpawnError,
    > {
        let (initial_status, events, state_keeper, proof_of_life) = StateKeeper::spawn(
            context,
            working_dir,
            snapshot,
            participation,
            log_keeping,
            #[cfg(feature = "tracer")]
            tracer,
        )
        .await?;

        let req_handler = RequestHandler::new(state_keeper.clone());
        let commits = Commits::new();

        let inner = NodeInner::new(id, communicator, state_keeper.clone(), commits.clone());

        let (handle_send, handle_recv) = mpsc::channel(32);

        let inner = Rc::new(inner);

        let node = NodeKernel {
            inner,
            state_keeper,
            proof_of_life,
            commits,
            events,
            status: initial_status,
            handle_send,
            handle_recv,
            handle_appends: FuturesUnordered::new(),
        };

        Ok((req_handler, node))
    }

    fn poll_handle_reqs(&mut self, cx: &mut std::task::Context<'_>) {
        while let std::task::Poll::Ready(Some((req, send))) = self.handle_recv.poll_next_unpin(cx) {
            self.process_handle_req(req, send);
        }
    }

    fn process_handle_req(
        &mut self,
        req: NodeHandleRequest<S, RoundNumOf<C>>,
        send: oneshot::Sender<NodeHandleResponse<S>>,
    ) {
        match req {
            NodeHandleRequest::Status => {
                let _ = send.send(NodeHandleResponse::Status(self.status()));
            }
            NodeHandleRequest::Append { log_entry, args } => self.handle_appends.push(
                self.append(log_entry, args)
                    .map(|r| {
                        let _ = send.send(NodeHandleResponse::Append(r));
                    })
                    .boxed_local(),
            ),
        }
    }
}
