use std::rc::Rc;
use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};
use num_traits::One;

use crate::append::{AppendArgs, AppendError};
use crate::applicable::ApplicableTo;
use crate::communicator::{Communicator, CoordNumOf, JustificationOf, RoundNumOf};
use crate::error::Disoriented;
use crate::event::{Event, ShutdownEvent};
use crate::state::{LogEntryOf, NodeIdOf, State};
use crate::voting::Voter;

use super::handle::{NodeHandleRequest, NodeHandleResponse};
use super::inner::NodeInner;
use super::shutdown::DefaultShutdown;
use super::snapshot::{Snapshot, SnapshotFor};
use super::state_keeper::{EventStream, ProofOfLife, StateKeeper, StateKeeperHandle};
use super::{commits::Commits, AppendResultFor};
use super::{CommitFor, EventFor, Node, NodeHandle, NodeStatus, Participation, RequestHandler};

/// The default [`Node`][crate::Node] implementation.
// TODO a better name may be needed…
pub struct NodeKernel<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    inner: Rc<NodeInner<S, C>>,
    state_keeper: StateKeeperHandle<S, C>,
    proof_of_life: ProofOfLife,
    commits: Commits,
    events: EventStream<S, C>,
    status: NodeStatus,
    participation: Participation<RoundNumOf<C>>,
    handle_send: mpsc::Sender<super::handle::RequestAndResponseSender<S, C>>,
    handle_recv: mpsc::Receiver<super::handle::RequestAndResponseSender<S, C>>,
    handle_appends: FuturesUnordered<HandleAppend<S, C>>,
}

impl<S, C> Node for NodeKernel<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    type State = S;
    type Communicator = C;

    type Shutdown = DefaultShutdown<S, C>;

    fn id(&self) -> NodeIdOf<S> {
        self.inner.id()
    }

    fn status(&self) -> NodeStatus {
        self.status
    }

    fn participation(&self) -> Participation<RoundNumOf<C>> {
        self.participation
    }

    /// Polls the node's status, yielding `Ready` on every change.
    ///
    /// It is important to poll the node's event stream because it implicitly
    /// drives the actions that keep the node up to date.
    fn poll_events(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<EventFor<Self>> {
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

        if let std::task::Poll::Ready(Event::Install { .. }) = result {
            self.participation = Participation::Passive;
        }

        if let std::task::Poll::Ready(Event::Activate(r)) = result {
            self.participation = Participation::PartiallyActive(r);
        }

        if let Participation::PartiallyActive(r) = self.participation {
            if let std::task::Poll::Ready(Event::Apply { round, .. }) = result {
                if round + One::one() >= r {
                    self.participation = Participation::Active;
                }
            }
        }

        result
    }

    fn handle(&self) -> NodeHandle<S, C> {
        NodeHandle::new(self.handle_send.clone(), self.state_keeper.clone())
    }

    fn prepare_snapshot(
        &self,
    ) -> LocalBoxFuture<'static, Result<SnapshotFor<Self>, crate::error::PrepareSnapshotError>>
    {
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

    fn append<A: ApplicableTo<Self::State> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<C>,
    ) -> LocalBoxFuture<'static, Result<CommitFor<Self, A>, AppendError<C>>> {
        Rc::clone(&self.inner)
            .append(applicable, args)
            .boxed_local()
    }

    fn read_stale(&self) -> LocalBoxFuture<'static, Result<Arc<S>, Disoriented>> {
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
    pub(crate) async fn spawn<V>(
        id: NodeIdOf<S>,
        communicator: C,
        args: super::SpawnArgs<S, C, V>,
    ) -> Result<(RequestHandler<S, C>, NodeKernel<S, C>), crate::error::SpawnError>
    where
        V: Voter<
            State = S,
            RoundNum = RoundNumOf<C>,
            CoordNum = CoordNumOf<C>,
            Justification = JustificationOf<C>,
        >,
    {
        let (initial_status, initial_participation, events, state_keeper, proof_of_life) =
            StateKeeper::spawn(args).await?;

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
            participation: initial_participation,
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
        req: NodeHandleRequest<C>,
        send: oneshot::Sender<NodeHandleResponse<S, C>>,
    ) {
        match req {
            NodeHandleRequest::Status => {
                let _ = send.send(NodeHandleResponse::Status(self.status()));
            }
            NodeHandleRequest::Append { log_entry, args } => {
                self.handle_appends.push(HandleAppend {
                    append: self.append(log_entry, args),
                    send: Some(send),
                })
            }
        }
    }
}

struct HandleAppend<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    append: LocalBoxFuture<'static, AppendResultFor<NodeKernel<S, C>, LogEntryOf<S>>>,
    send: Option<oneshot::Sender<NodeHandleResponse<S, C>>>,
}

impl<S, C> std::future::Future for HandleAppend<S, C>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
{
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.send.as_ref().unwrap().is_canceled() {
            return std::task::Poll::Ready(());
        }

        self.append.poll_unpin(cx).map(|r| {
            let send = self.send.take().unwrap();
            let _ = send.send(NodeHandleResponse::Append(r));
        })
    }
}
