use futures::future::FutureExt;
use futures::future::LocalBoxFuture;
use futures::future::TryFutureExt;

use crate::communicator::Communicator;
use crate::decoration::Decoration;
use crate::error::SpawnError;
use crate::invocation::AbstainOf;
use crate::invocation::CommunicationErrorOf;
use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryOf;
use crate::invocation::NayOf;
use crate::invocation::NodeIdOf;
use crate::invocation::NodeOf;
use crate::invocation::RoundNumOf;
use crate::invocation::SnapshotFor;
use crate::invocation::StateOf;
use crate::invocation::YeaOf;
use crate::log::LogKeeping;
use crate::node;
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
use crate::voting::IndiscriminateVoter;
use crate::voting::IndiscriminateVoterFor;
use crate::voting::Voter;
use crate::Node;
use crate::State;

use super::snapshot::Snapshot;
use super::CommunicatorOf;
use super::InvocationOf;
use super::NodeKernel;
use super::NodeKit;
use super::RequestHandlerFor;

#[derive(Default)]
pub struct NodeBuilderBlank<I>(std::marker::PhantomData<I>);

impl<I: Invocation> NodeBuilderBlank<I> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    pub fn for_node(self, node_id: NodeIdOf<I>) -> NodeBuilderWithNodeId<I> {
        NodeBuilderWithNodeId { node_id }
    }
}

pub struct NodeBuilderWithNodeId<I: Invocation> {
    node_id: NodeIdOf<I>,
}

impl<I: Invocation> NodeBuilderWithNodeId<I> {
    pub fn working_in(
        self,
        dir: impl AsRef<std::path::Path>,
    ) -> NodeBuilderWithNodeIdAndWorkingDir<I> {
        NodeBuilderWithNodeIdAndWorkingDir {
            working_dir: Some(dir.as_ref().to_path_buf()),
            node_id: self.node_id,
        }
    }

    pub fn working_ephemerally(self) -> NodeBuilderWithNodeIdAndWorkingDir<I> {
        NodeBuilderWithNodeIdAndWorkingDir {
            working_dir: None,
            node_id: self.node_id,
        }
    }
}

pub struct NodeBuilderWithNodeIdAndWorkingDir<I: Invocation> {
    working_dir: Option<std::path::PathBuf>,
    node_id: NodeIdOf<I>,
}

impl<I: Invocation> NodeBuilderWithNodeIdAndWorkingDir<I> {
    pub fn communicating_via<C>(
        self,
        communicator: C,
    ) -> NodeBuilderWithNodeIdAndWorkingDirAndCommunicator<I, C>
    where
        C: Communicator<
            Node = NodeOf<I>,
            RoundNum = RoundNumOf<I>,
            CoordNum = CoordNumOf<I>,
            LogEntry = LogEntryOf<I>,
            Error = CommunicationErrorOf<I>,
            Yea = YeaOf<I>,
            Nay = NayOf<I>,
            Abstain = AbstainOf<I>,
        >,
    {
        NodeBuilderWithNodeIdAndWorkingDirAndCommunicator {
            working_dir: self.working_dir,
            node_id: self.node_id,
            communicator,
        }
    }
}

pub struct NodeBuilderWithNodeIdAndWorkingDirAndCommunicator<I: Invocation, C: Communicator> {
    working_dir: Option<std::path::PathBuf>,
    node_id: NodeIdOf<I>,
    communicator: C,
}

impl<I, C> NodeBuilderWithNodeIdAndWorkingDirAndCommunicator<I, C>
where
    I: Invocation,
    C: Communicator<
        Node = NodeOf<I>,
        RoundNum = RoundNumOf<I>,
        CoordNum = CoordNumOf<I>,
        LogEntry = LogEntryOf<I>,
        Error = CommunicationErrorOf<I>,
        Yea = YeaOf<I>,
        Nay = NayOf<I>,
        Abstain = AbstainOf<I>,
    >,
{
    /// Starts the node without any state and in passive mode.
    pub fn without_state(self) -> NodeBuilder<NodeKernel<I, C>> {
        self.with_snapshot_and_passivity(None, false)
    }

    /// Starts a new cluster with the given initial state.
    ///
    /// The round number will be [zero](num_traits::Zero).
    pub fn with_initial_state<S: Into<Option<StateOf<I>>>>(
        self,
        initial_state: S,
    ) -> NodeBuilder<NodeKernel<I, C>> {
        self.with_snapshot_and_passivity(initial_state.into().map(Snapshot::initial), false)
    }

    /// Resume operation from the given snapshot.
    ///
    /// # Soundness
    ///
    /// It is assumed that the given snapshot was yielded from the [`Last`]
    /// event of a clean shutdown and that the node hasn't run in the meantime.
    /// As such the node will start in active participation mode. This is
    /// unsound if the assumptions are violated.
    ///
    /// Use [recovering_with] to have a failed node recover.
    ///
    /// [`Last`]: crate::event::ShutdownEvent::Last
    /// [recovering_with]:
    /// NodeBuilderWithNodeIdAndWorkingDirAndCommunicator::recovering_with
    pub fn resuming_from<S: Into<Option<SnapshotFor<I>>>>(
        self,
        snapshot: S,
    ) -> NodeBuilder<NodeKernel<I, C>> {
        self.with_snapshot_and_passivity(snapshot, false)
    }

    /// Resume operation from the given snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_with<S: Into<Option<SnapshotFor<I>>>>(
        self,
        snapshot: S,
    ) -> NodeBuilder<NodeKernel<I, C>> {
        self.with_snapshot_and_passivity(snapshot, true)
    }

    /// Resume operation without a snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_without_state(self) -> NodeBuilder<NodeKernel<I, C>> {
        self.with_snapshot_and_passivity(None, true)
    }

    /// Commence operation from the given snapshot.
    ///
    /// # Soundness
    ///
    /// This method assumes that the node is  (re-)joining the Paxos cluster.
    /// Use [recovering_with] to have a failed node recover.
    ///
    /// A node is considered to rejoin iff there is a previous round `r` such
    /// that this node
    ///  - was not considered a member of the cluster for `r` and
    ///  - it did not participate in any rounds since `r`.
    ///
    /// [recovering_with]:
    /// NodeBuilderWithNodeIdAndWorkingDirAndCommunicator::recovering_with
    pub fn joining_with<S: Into<Option<SnapshotFor<I>>>>(
        self,
        snapshot: S,
    ) -> NodeBuilder<NodeKernel<I, C>> {
        self.with_snapshot_and_passivity(snapshot, false)
    }

    /// Commence operation without a snapshot.
    ///
    /// # Soundness
    ///
    /// This method assumes that the node is  (re-)joining the Paxos cluster.
    /// Use [recovering_without] to have a failed node recover.
    ///
    /// A node is considered to rejoin iff there is a previous round `r` such
    /// that this node
    ///  - was not considered a member of the cluster for `r` and
    ///  - it did not participate in any rounds since `r`.
    ///
    /// [recovering_without]:
    /// NodeBuilderWithNodeIdAndWorkingDirAndCommunicator::recovering_without
    pub fn joining_without_state(self) -> NodeBuilder<NodeKernel<I, C>> {
        self.with_snapshot_and_passivity(None, false)
    }

    #[doc(hidden)]
    pub fn with_snapshot_and_passivity<S: Into<Option<SnapshotFor<I>>>>(
        self,
        snapshot: S,
        force_passive: bool,
    ) -> NodeBuilder<NodeKernel<I, C>> {
        let snapshot = snapshot.into();

        NodeBuilder {
            kit: NodeKit::new(),
            working_dir: self.working_dir,
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot,
            force_passive,
            voter: IndiscriminateVoter::new(),
            log_keeping: Default::default(),
            finisher: Box::new(Ok),

            #[cfg(feature = "tracer")]
            tracer: None,
        }
    }
}

type Finisher<N> =
    dyn FnOnce(NodeKernel<InvocationOf<N>, CommunicatorOf<N>>) -> Result<N, SpawnError>;

pub struct NodeBuilder<N: Node, V = IndiscriminateVoterFor<N>> {
    kit: NodeKit<InvocationOf<N>>,
    working_dir: Option<std::path::PathBuf>,
    node_id: node::NodeIdOf<N>,
    voter: V,
    communicator: CommunicatorOf<N>,
    snapshot: Option<node::SnapshotFor<N>>,
    force_passive: bool,
    log_keeping: LogKeeping,
    finisher: Box<Finisher<N>>,

    #[cfg(feature = "tracer")]
    tracer: Option<Box<dyn Tracer<InvocationOf<N>>>>,
}

impl<N, V> NodeBuilder<N, V>
where
    N: Node + 'static,
    V: Voter<
        State = node::StateOf<N>,
        RoundNum = node::RoundNumOf<N>,
        CoordNum = node::CoordNumOf<N>,
        Abstain = node::AbstainOf<N>,
        Yea = node::YeaOf<N>,
        Nay = node::NayOf<N>,
    >,
{
    pub fn limiting_applied_entry_logs_to(mut self, limit: usize) -> Self {
        self.log_keeping.logs_kept = limit;

        self
    }

    pub fn limiting_entries_in_applied_entry_logs_to(mut self, limit: usize) -> Self {
        assert!(limit > 0);

        self.log_keeping.entry_limit = limit;

        self
    }

    #[cfg(feature = "tracer")]
    pub fn traced_by<T: Into<Box<dyn Tracer<InvocationOf<N>>>>>(mut self, tracer: T) -> Self {
        self.tracer = Some(tracer.into());

        self
    }

    pub fn voting_with<T>(self, voter: T) -> NodeBuilder<N, T> {
        NodeBuilder {
            kit: self.kit,
            working_dir: self.working_dir,
            node_id: self.node_id,
            voter,
            communicator: self.communicator,
            snapshot: self.snapshot,
            force_passive: self.force_passive,
            log_keeping: self.log_keeping,
            finisher: self.finisher,

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }

    pub fn using(mut self, kit: NodeKit<InvocationOf<N>>) -> Self {
        self.kit = kit;

        self
    }

    pub fn decorated_with<D>(self, arguments: <D as Decoration>::Arguments) -> NodeBuilder<D, V>
    where
        D: Decoration<
            Decorated = N,
            Invocation = InvocationOf<N>,
            Communicator = CommunicatorOf<N>,
        >,
    {
        let finisher = self.finisher;

        NodeBuilder {
            kit: self.kit,
            working_dir: self.working_dir,
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot: self.snapshot,
            force_passive: self.force_passive,
            voter: self.voter,
            log_keeping: self.log_keeping,
            finisher: Box::new(move |x| ((finisher)(x)).and_then(|node| D::wrap(node, arguments))),

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }

    pub fn spawn(self) -> LocalBoxFuture<'static, Result<(RequestHandlerFor<N>, N), SpawnError>>
    where
        node::StateOf<N>: State<Context = ()>,
    {
        self.spawn_in(())
    }

    pub fn spawn_in(
        self,
        context: node::ContextOf<N>,
    ) -> LocalBoxFuture<'static, Result<(RequestHandlerFor<N>, N), SpawnError>> {
        let finisher = self.finisher;

        NodeKernel::spawn(
            self.kit,
            self.node_id,
            self.communicator,
            super::SpawnArgs {
                context,
                working_dir: self.working_dir,
                node_id: self.node_id,
                voter: self.voter,
                snapshot: self.snapshot,
                force_passive: self.force_passive,
                log_keeping: self.log_keeping,
                #[cfg(feature = "tracer")]
                tracer: self.tracer,
            },
        )
        .and_then(|(req_handler, kernel)| {
            futures::future::ready(finisher(kernel).map(|node| (req_handler, node)))
        })
        .boxed_local()
    }
}
