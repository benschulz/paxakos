use std::rc::Rc;

use futures::channel::mpsc;
use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::StreamExt;
use futures::TryFutureExt;
use num_traits::One;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::buffer::Buffer;
use crate::communicator::AbstainOf;
use crate::communicator::Communicator;
use crate::communicator::CoordNumOf;
use crate::communicator::NayOf;
use crate::communicator::RoundNumOf;
use crate::communicator::YeaOf;
use crate::error::Disoriented;
use crate::error::ShutDown;
use crate::error::ShutDownOr;
use crate::event::Event;
use crate::event::ShutdownEvent;
use crate::executor;
use crate::invocation;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::NodeIdOf;
use crate::invocation::StateOf;
use crate::retry::RetryPolicy;
use crate::voting::Voter;
use crate::Commit;

use super::commits::Commits;
use super::inner::NodeInner;
use super::shutdown::DefaultShutdown;
use super::state_keeper::EventStream;
use super::state_keeper::ProofOfLife;
use super::state_keeper::StateKeeper;
use super::state_keeper::StateKeeperHandle;
use super::state_keeper::StateKeeperKit;
use super::AppendResultFor;
use super::EventFor;
use super::ImplAppendResultFor;
use super::Node;
use super::NodeHandle;
use super::NodeImpl;
use super::NodeStatus;
use super::Participation;
use super::RequestHandler;
use super::SnapshotFor;
use super::StaticAppendResultFor;

/// The core [`Node`][crate::Node] implementation.
///
/// This `Node` implemenation does most of the heavy lifting w/r/t the consensus
/// protocol. It has a counterpart [`Shell`][crate::Shell].
pub struct Core<I, C>
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
    handle_sender: mpsc::Sender<super::handle::RequestAndResponseSender<I>>,
}

impl<I, C> Node for Core<I, C>
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
        while let std::task::Poll::Ready(Some(())) = self.commits.poll_next(cx) {
            // keep going
        }

        let result = self
            .events
            .poll_next_unpin(cx)
            .map(|e| e.expect("Event stream ended"))
            .map(|e| match e {
                ShutdownEvent::Regular(e) => e,
                ShutdownEvent::Final { .. } => unreachable!(),
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
        NodeHandle::new(self.handle_sender.clone(), self.state_keeper.clone())
    }

    fn prepare_snapshot(&self) -> LocalBoxFuture<'static, SnapshotFor<Self>> {
        self.state_keeper
            .prepare_snapshot()
            .map(ShutDown::rule_out)
            .boxed_local()
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

    fn append<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'_, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self::Invocation>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
    {
        self.append_impl(applicable, args)
            .map_err(|e| e.expect_other())
            .boxed_local()
    }

    fn append_static<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'static, StaticAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self::Invocation>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.append_impl(applicable, args)
            .map_err(|e| e.into())
            .boxed_local()
    }

    fn read_stale<F, T>(&self, f: F) -> LocalBoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self::Invocation>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.state_keeper
            .read_stale(&self.proof_of_life, f)
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

impl<I, C> NodeImpl for Core<I, C>
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
    fn append_impl<A, P, R>(
        &self,
        applicable: A,
        args: P,
    ) -> LocalBoxFuture<'static, ImplAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<super::StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
    {
        Rc::clone(&self.inner)
            .append(applicable, args.into())
            .boxed_local()
    }

    fn await_commit_of(
        &self,
        log_entry_id: super::LogEntryIdOf<Self>,
    ) -> LocalBoxFuture<'static, Result<super::CommitFor<Self>, crate::error::ShutDown>> {
        let commit = self.state_keeper.await_commit_of(log_entry_id);

        async move {
            match commit.await {
                Ok(receiver) => match receiver.await {
                    Ok((round_num, outcome)) => Ok(Commit::ready(round_num, outcome)),
                    Err(_) => Err(ShutDown),
                },
                Err(err) => Err(err),
            }
        }
        .boxed_local()
    }
}

impl<I, C> Core<I, C>
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
    pub(crate) async fn spawn<V, B, E>(
        state_keeper_kit: StateKeeperKit<I>,
        handle_sender: mpsc::Sender<super::handle::RequestAndResponseSender<I>>,
        id: NodeIdOf<I>,
        communicator: C,
        args: super::SpawnArgs<I, V, B, E>,
    ) -> Result<(RequestHandler<I>, Core<I, C>), executor::ErrorOf<E>>
    where
        V: Voter<
            State = StateOf<I>,
            RoundNum = RoundNumOf<C>,
            CoordNum = CoordNumOf<C>,
            Abstain = AbstainOf<C>,
            Yea = YeaOf<C>,
            Nay = NayOf<C>,
        >,
        B: Buffer<RoundNum = RoundNumOf<C>, CoordNum = CoordNumOf<C>, Entry = LogEntryOf<I>>,
        E: crate::executor::Executor,
    {
        let state_keeper = state_keeper_kit.handle();

        let (initial_status, initial_participation, events, proof_of_life) =
            StateKeeper::spawn(state_keeper_kit, args).await?;

        let req_handler = RequestHandler::new(state_keeper.clone());
        let commits = Commits::new();

        let inner = NodeInner::new(id, communicator, state_keeper.clone(), commits.clone());

        let inner = Rc::new(inner);

        let node = Core {
            inner,
            state_keeper,
            proof_of_life,
            commits,
            events,
            status: initial_status,
            participation: initial_participation,
            handle_sender,
        };

        Ok((req_handler, node))
    }
}
