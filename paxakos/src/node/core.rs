use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::TryFutureExt;
use num_traits::One;

use crate::append::AppendArgs;
use crate::applicable::ApplicableTo;
use crate::communicator::Communicator;
use crate::communicator::RoundNumOf;
use crate::error::Disoriented;
use crate::error::PollError;
use crate::error::ShutDown;
use crate::error::ShutDownOr;
use crate::event::Event;
use crate::event::ShutdownEvent;
use crate::invocation;
use crate::invocation::Invocation;
use crate::invocation::NodeIdOf;
use crate::invocation::StateOf;
use crate::node::inner::SnarcRef;
use crate::retry::RetryPolicy;
use crate::Commit;

use super::inner::NodeInner;
use super::shutdown::DefaultShutdown;
use super::state_keeper::EventStream;
use super::state_keeper::ProofOfLife;
use super::state_keeper::StateKeeperHandle;
use super::AppendResultFor;
use super::EventFor;
use super::ImplAppendResultFor;
use super::Node;
use super::NodeHandle;
use super::NodeImpl;
use super::NodeStatus;
use super::Participation;
use super::SnapshotFor;

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
    inner: SnarcRef<NodeInner<I, C>>,
    state_keeper: StateKeeperHandle<I>,
    proof_of_life: ProofOfLife,
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
        self.inner.expect().id()
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
        while let std::task::Poll::Ready(Some(())) = self.inner.expect().commits.poll_next(cx) {
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

    fn prepare_snapshot(&self) -> BoxFuture<'static, SnapshotFor<Self>> {
        self.state_keeper
            .prepare_snapshot()
            .map(ShutDown::rule_out)
            .boxed()
    }

    fn affirm_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), crate::error::AffirmSnapshotError>> {
        self.state_keeper.affirm_snapshot(snapshot).boxed()
    }

    fn install_snapshot(
        &self,
        snapshot: SnapshotFor<Self>,
    ) -> BoxFuture<'static, Result<(), crate::error::InstallSnapshotError>> {
        self.state_keeper.install_snapshot(snapshot).boxed()
    }

    fn append<A, P, R>(
        &mut self,
        applicable: A,
        args: P,
    ) -> BoxFuture<'static, AppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<StateOf<Self::Invocation>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
        R::StaticError: From<ShutDownOr<R::Error>>,
    {
        self.append_impl(applicable, args)
            .map_err(|e| e.into())
            .boxed()
    }

    fn read_stale<F, T>(&self, f: F) -> BoxFuture<'_, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self::Invocation>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.state_keeper.read_stale(&self.proof_of_life, f).boxed()
    }

    fn read_stale_infallibly<F, T>(&self, f: F) -> BoxFuture<'_, T>
    where
        F: FnOnce(Option<&StateOf<Self::Invocation>>) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.state_keeper
            .read_stale_infallibly(&self.proof_of_life, |r| f(r.ok()))
            .boxed()
    }

    fn read_stale_scoped<'read, F, T>(&self, f: F) -> BoxFuture<'read, Result<T, Disoriented>>
    where
        F: FnOnce(&StateOf<Self::Invocation>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.state_keeper
            .read_stale_scoped(&self.proof_of_life, f)
            .boxed()
    }

    fn read_stale_scoped_infallibly<'read, F, T>(&self, f: F) -> BoxFuture<'read, T>
    where
        F: FnOnce(Option<&StateOf<Self::Invocation>>) -> T + Send + 'read,
        T: Send + 'static,
    {
        self.state_keeper
            .read_stale_scoped_infallibly(&self.proof_of_life, |r| f(r.ok()))
            .boxed()
    }

    fn shut_down(self) -> Self::Shutdown {
        let state_keeper = self.state_keeper;
        let proof_of_life = self.proof_of_life;
        let events = self.events;

        let trigger = state_keeper.shut_down(proof_of_life).fuse().boxed();

        DefaultShutdown::new(trigger, events)
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
        &mut self,
        applicable: A,
        args: P,
    ) -> BoxFuture<'static, ImplAppendResultFor<Self, A, R>>
    where
        A: ApplicableTo<super::StateOf<Self>> + 'static,
        P: Into<AppendArgs<Self::Invocation, R>>,
        R: RetryPolicy<Invocation = Self::Invocation>,
    {
        NodeInner::append(SnarcRef::clone(&self.inner), applicable, args.into()).boxed()
    }

    fn await_commit_of(
        &mut self,
        log_entry_id: super::LogEntryIdOf<Self>,
    ) -> BoxFuture<'static, Result<super::CommitFor<Self>, crate::error::ShutDown>> {
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
        .boxed()
    }

    fn eject(
        &mut self,
        reason: crate::node::EjectionOf<Self>,
    ) -> BoxFuture<'static, Result<bool, crate::error::ShutDown>> {
        self.state_keeper.eject(reason).boxed()
    }

    fn poll(
        &mut self,
        round_num: super::RoundNumOf<Self>,
        additional_nodes: Vec<super::NodeOf<Self>>,
    ) -> BoxFuture<'static, Result<bool, PollError<I>>> {
        NodeInner::poll(SnarcRef::clone(&self.inner), round_num, additional_nodes).boxed()
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inner: SnarcRef<NodeInner<I, C>>,
        state_keeper: StateKeeperHandle<I>,
        proof_of_life: ProofOfLife,
        events: EventStream<I>,
        status: NodeStatus,
        participation: Participation<RoundNumOf<C>>,
        handle_sender: mpsc::Sender<super::handle::RequestAndResponseSender<I>>,
    ) -> Self {
        Self {
            inner,
            state_keeper,
            proof_of_life,
            events,
            status,
            participation,
            handle_sender,
        }
    }
}
