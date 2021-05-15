use futures::future::{FutureExt, LocalBoxFuture, TryFutureExt};

use crate::communicator::{Communicator, CoordNumOf, RoundNumOf};
use crate::deco::Decoration;
use crate::error::SpawnError;
use crate::log::LogKeeping;
use crate::node::{self, JustificationOf, Participation, RequestHandlerFor};
use crate::state::{ContextOf, NodeIdOf};
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
use crate::voting::{IndiscriminateVoter, IndiscriminateVoterFor, Voter};
use crate::{Identifier, Node, State};

use super::snapshot::{Snapshot, SnapshotFor};
use super::{CommunicatorOf, NodeKernel, StateOf};

#[derive(Default)]
pub struct NodeBuilderBlank;

impl NodeBuilderBlank {
    pub fn new() -> Self {
        Self
    }

    pub fn for_node<I: Identifier>(self, node_id: I) -> NodeBuilderWithNodeId<I> {
        NodeBuilderWithNodeId { node_id }
    }
}

pub struct NodeBuilderWithNodeId<I: Identifier> {
    node_id: I,
}

impl<I: Identifier> NodeBuilderWithNodeId<I> {
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

pub struct NodeBuilderWithNodeIdAndWorkingDir<I: Identifier> {
    working_dir: Option<std::path::PathBuf>,
    node_id: I,
}

impl<I: Identifier> NodeBuilderWithNodeIdAndWorkingDir<I> {
    pub fn communicating_via<C>(
        self,
        communicator: C,
    ) -> NodeBuilderWithNodeIdAndWorkingDirAndCommunicator<C>
    where
        C: Communicator,
        <C as Communicator>::Node: crate::NodeInfo<Id = I>,
    {
        NodeBuilderWithNodeIdAndWorkingDirAndCommunicator {
            working_dir: self.working_dir,
            node_id: self.node_id,
            communicator,
        }
    }
}

pub struct NodeBuilderWithNodeIdAndWorkingDirAndCommunicator<C: Communicator> {
    working_dir: Option<std::path::PathBuf>,
    node_id: <<C as Communicator>::Node as crate::NodeInfo>::Id,
    communicator: C,
}

impl<C: Communicator> NodeBuilderWithNodeIdAndWorkingDirAndCommunicator<C> {
    /// Starts the node without any state.
    pub fn without<S>(self) -> NodeBuilder<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(None, Participation::Passive)
    }

    /// Starts a new cluster with the given initial state.
    ///
    /// The round number will be [zero](num_traits::Zero).
    pub fn with_initial_state<S>(
        self,
        initial_state: impl Into<Option<S>>,
    ) -> NodeBuilder<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(
            initial_state.into().map(Snapshot::initial),
            Participation::Active,
        )
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
    pub fn resuming_from<S>(
        self,
        snapshot: impl Into<Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>>,
    ) -> NodeBuilder<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(snapshot.into(), Participation::Active)
    }

    /// Resume operation from the given snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_with<S>(
        self,
        snapshot: impl Into<Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>>,
    ) -> NodeBuilder<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(snapshot.into(), Participation::Passive)
    }

    /// Resume operation without a snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_without<S>(self) -> NodeBuilder<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(None, Participation::Passive)
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
    pub fn joining_with<S>(
        self,
        snapshot: impl Into<Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>>,
    ) -> NodeBuilder<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(snapshot.into(), Participation::Active)
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
    pub fn joining_without<S>(self) -> NodeBuilder<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(None, Participation::Active)
    }

    fn with_snapshot_and_participation<S>(
        self,
        snapshot: Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>,
        participation: Participation<RoundNumOf<C>>,
    ) -> NodeBuilder<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        NodeBuilder {
            working_dir: self.working_dir,
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot,
            participation,
            voter: IndiscriminateVoter::new(),
            log_keeping: Default::default(),
            finisher: Box::new(Ok),

            #[cfg(feature = "tracer")]
            tracer: None,
        }
    }
}

type Finisher<N> = dyn FnOnce(NodeKernel<StateOf<N>, CommunicatorOf<N>>) -> Result<N, SpawnError>;

pub struct NodeBuilder<N: Node, V = IndiscriminateVoterFor<N>> {
    working_dir: Option<std::path::PathBuf>,
    node_id: NodeIdOf<StateOf<N>>,
    voter: V,
    communicator: CommunicatorOf<N>,
    snapshot: Option<SnapshotFor<N>>,
    participation: Participation<node::RoundNumOf<N>>,
    log_keeping: LogKeeping,
    finisher: Box<Finisher<N>>,

    #[cfg(feature = "tracer")]
    tracer: Option<Box<dyn Tracer<CommunicatorOf<N>>>>,
}

impl<S, C, V> NodeBuilder<NodeKernel<S, C>, V>
where
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    C: Communicator,
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
    pub fn traced_by(mut self, tracer: impl Into<Box<dyn Tracer<C>>>) -> Self {
        self.tracer = Some(tracer.into());

        self
    }

    pub fn voting_with<T>(self, voter: T) -> NodeBuilder<NodeKernel<S, C>, T> {
        NodeBuilder {
            working_dir: self.working_dir,
            node_id: self.node_id,
            voter,
            communicator: self.communicator,
            snapshot: self.snapshot,
            participation: self.participation,
            log_keeping: self.log_keeping,
            finisher: self.finisher,

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }
}

impl<N, V> NodeBuilder<N, V>
where
    N: Node + 'static,
    V: Voter<
        State = StateOf<N>,
        RoundNum = node::RoundNumOf<N>,
        CoordNum = node::CoordNumOf<N>,
        Justification = JustificationOf<N>,
    >,
{
    pub fn decorated_with<D>(self, arguments: <D as Decoration>::Arguments) -> NodeBuilder<D, V>
    where
        D: Decoration<Decorated = N, State = StateOf<N>, Communicator = CommunicatorOf<N>>,
    {
        let finisher = self.finisher;

        NodeBuilder {
            working_dir: self.working_dir,
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot: self.snapshot,
            participation: self.participation,
            voter: self.voter,
            log_keeping: self.log_keeping,
            finisher: Box::new(move |x| ((finisher)(x)).and_then(|node| D::wrap(node, arguments))),

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }

    pub fn spawn(self) -> LocalBoxFuture<'static, Result<(RequestHandlerFor<N>, N), SpawnError>>
    where
        StateOf<N>: State<Context = ()>,
    {
        self.spawn_in(())
    }

    pub fn spawn_in(
        self,
        context: ContextOf<StateOf<N>>,
    ) -> LocalBoxFuture<'static, Result<(RequestHandlerFor<N>, N), SpawnError>> {
        let finisher = self.finisher;

        NodeKernel::spawn(
            self.node_id,
            self.communicator,
            super::SpawnArgs {
                context,
                working_dir: self.working_dir,
                node_id: self.node_id,
                voter: self.voter,
                snapshot: self.snapshot,
                participation: self.participation,
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
