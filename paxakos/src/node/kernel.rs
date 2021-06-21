use std::rc::Rc;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use num_traits::One;

use crate::append::AppendArgs;
use crate::append::AppendError;
use crate::applicable::ApplicableTo;
use crate::communicator::AbstainOf;
use crate::communicator::Communicator;
use crate::communicator::CoordNumOf;
use crate::communicator::NayOf;
use crate::communicator::RoundNumOf;
use crate::communicator::YeaOf;
use crate::error::Disoriented;
use crate::event::Event;
use crate::event::ShutdownEvent;
use crate::invocation;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::NodeIdOf;
use crate::invocation::StateOf;
use crate::voting::Voter;

use super::commits::Commits;
use super::handle::NodeHandleRequest;
use super::handle::NodeHandleResponse;
use super::inner::NodeInner;
use super::shutdown::DefaultShutdown;
use super::state_keeper::EventStream;
use super::state_keeper::ProofOfLife;
use super::state_keeper::StateKeeper;
use super::state_keeper::StateKeeperHandle;
use super::AppendResultFor;
use super::CommitFor;
use super::EventFor;
use super::Node;
use super::NodeHandle;
use super::NodeStatus;
use super::Participation;
use super::RequestHandler;
use super::SnapshotFor;

/// The default [`Node`][crate::Node] implementation.
// TODO a better name may be needed…
pub struct NodeKernel<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = invocation::NodeOf<I>,
        RoundNum = invocation::RoundNumOf<I>,
        CoordNum = invocation::CoordNumOf<I>,
        LogEntry = invocation::LogEntryOf<I>,
        Error = invocation::CommunicationErrorOf<I>,
        Yea = invocation::YeaOf<I>,
        Nay = invocation::NayOf<I>,
        Abstain = invocation::AbstainOf<I>,
    >,
{
    inner: Rc<NodeInner<I, C>>,
    state_keeper: StateKeeperHandle<I>,
    proof_of_life: ProofOfLife,
    commits: Commits,
    events: EventStream<I>,
    status: NodeStatus,
    participation: Participation<RoundNumOf<C>>,
    handle_send: mpsc::Sender<super::handle::RequestAndResponseSender<I>>,
    handle_recv: mpsc::Receiver<super::handle::RequestAndResponseSender<I>>,
    handle_appends: FuturesUnordered<HandleAppend<I, C>>,
}

impl<I, C> Node for NodeKernel<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = invocation::NodeOf<I>,
        RoundNum = invocation::RoundNumOf<I>,
        CoordNum = invocation::CoordNumOf<I>,
        LogEntry = invocation::LogEntryOf<I>,
        Error = invocation::CommunicationErrorOf<I>,
        Yea = invocation::YeaOf<I>,
        Nay = invocation::NayOf<I>,
        Abstain = invocation::AbstainOf<I>,
    >,
{
    type Invocation = I;
    type Communicator = C;

    type Shutdown = DefaultShutdown<I>;

    fn id(&self) -> NodeIdOf<I> {
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

    fn handle(&self) -> NodeHandle<I> {
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
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.state_keeper.affirm_snapshot(snapshot).boxed_local()
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> LocalBoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.state_keeper.install_snapshot(snapshot).boxed_local()
    }

    fn append<A: ApplicableTo<StateOf<I>> + 'static>(
        &self,
        applicable: A,
        args: AppendArgs<I>,
    ) -> LocalBoxFuture<'static, Result<CommitFor<Self, A>, AppendError<I>>> {
        Rc::clone(&self.inner)
            .append(applicable, args)
            .boxed_local()
    }

    fn read_stale(&self) -> LocalBoxFuture<'static, Result<Arc<StateOf<I>>, Disoriented>> {
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

impl<I, C> NodeKernel<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = invocation::NodeOf<I>,
        RoundNum = invocation::RoundNumOf<I>,
        CoordNum = invocation::CoordNumOf<I>,
        LogEntry = invocation::LogEntryOf<I>,
        Error = invocation::CommunicationErrorOf<I>,
        Yea = invocation::YeaOf<I>,
        Nay = invocation::NayOf<I>,
        Abstain = invocation::AbstainOf<I>,
    >,
{
    pub(crate) async fn spawn<V>(
        id: NodeIdOf<I>,
        communicator: C,
        args: super::SpawnArgs<I, V>,
    ) -> Result<(RequestHandler<I>, NodeKernel<I, C>), crate::error::SpawnError>
    where
        V: Voter<
            State = StateOf<I>,
            RoundNum = RoundNumOf<C>,
            CoordNum = CoordNumOf<C>,
            Abstain = AbstainOf<C>,
            Yea = YeaOf<C>,
            Nay = NayOf<C>,
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
        req: NodeHandleRequest<I>,
        send: oneshot::Sender<NodeHandleResponse<I>>,
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

struct HandleAppend<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = invocation::NodeOf<I>,
        RoundNum = invocation::RoundNumOf<I>,
        CoordNum = invocation::CoordNumOf<I>,
        LogEntry = invocation::LogEntryOf<I>,
        Error = invocation::CommunicationErrorOf<I>,
        Yea = invocation::YeaOf<I>,
        Nay = invocation::NayOf<I>,
        Abstain = invocation::AbstainOf<I>,
    >,
{
    append: LocalBoxFuture<'static, AppendResultFor<NodeKernel<I, C>, LogEntryOf<I>>>,
    send: Option<oneshot::Sender<NodeHandleResponse<I>>>,
}

impl<I, C> std::future::Future for HandleAppend<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = invocation::NodeOf<I>,
        RoundNum = invocation::RoundNumOf<I>,
        CoordNum = invocation::CoordNumOf<I>,
        LogEntry = invocation::LogEntryOf<I>,
        Error = invocation::CommunicationErrorOf<I>,
        Yea = invocation::YeaOf<I>,
        Nay = invocation::NayOf<I>,
        Abstain = invocation::AbstainOf<I>,
    >,
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
