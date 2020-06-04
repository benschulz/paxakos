use std::future::Future;

use futures::future::{FutureExt, LocalBoxFuture, TryFutureExt};

use crate::communicator::{Communicator, CoordNumOf, RoundNumOf};
use crate::deco::Decoration;
use crate::error::SpawnError;
use crate::log::LogKeeping;
use crate::node::Participaction;
#[cfg(feature = "tracer")]
use crate::state::LogEntryIdOf;
use crate::state::{ContextOf, NodeIdOf};
#[cfg(feature = "tracer")]
use crate::tracer::Tracer;
use crate::{Identifier, Node, State};

use super::snapshot::Snapshot;
use super::{CommunicatorOf, NodeKernel, RequestHandler, StateOf};

/// Builder to spawn a `Node`.
///
/// This API is badly desgined. Please have a look at and follow the
/// documentation of [`node_builder()`](crate::node_builder()).
// TODO This trait seems pointless, rename -WithAll to NodeBuilder?
//      Overall this seems a bit messy with all the structs. Is there a
//      better approach?
pub trait NodeBuilder: Sized {
    type Node: 'static + Node;
    type Future: Future<
        Output = Result<
            (
                RequestHandler<
                    StateOf<Self::Node>,
                    RoundNumOf<CommunicatorOf<Self::Node>>,
                    CoordNumOf<CommunicatorOf<Self::Node>>,
                >,
                Self::Node,
            ),
            SpawnError,
        >,
    >;

    fn decorated_with<D>(self, arguments: <D as Decoration>::Arguments) -> NodeBuilderWithAll<D>
    where
        D: Decoration<
            Decorated = Self::Node,
            State = StateOf<Self::Node>,
            Communicator = CommunicatorOf<Self::Node>,
        >;

    fn spawn(self) -> Self::Future
    where
        <Self::Node as Node>::State: State<Context = ()>,
    {
        self.spawn_in(())
    }

    fn spawn_in(self, context: ContextOf<StateOf<Self::Node>>) -> Self::Future;
}

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
    pub fn without_state<S>(self) -> NodeBuilderWithAll<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(None, Participaction::Passive)
    }

    /// Starts a new cluster with the given initial state.
    ///
    /// The round number will be [zero](num_traits::Zero).
    pub fn with_initial_state<S>(
        self,
        initial_state: impl Into<Option<S>>,
    ) -> NodeBuilderWithAll<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(
            initial_state.into().map(|s| Snapshot::initial(s)),
            Participaction::Active,
        )
    }

    /// Resume operation from the given snapshot.
    ///
    /// # Soundness
    ///
    /// Assuming a working directory is used, the node will read its obligation
    /// log and immediately transition into active participation. This is sound
    /// iff
    ///  - the given snapshot was taken by the same node,
    ///  - the same working directory was used when the snapshot was taken and
    ///  - the working directory is still valid, i.e. the node did not run using
    ///    a different working directory in the meantime.
    ///
    /// Use [recovering_with] to have a failed node recover.
    ///
    /// [recovering_with]:
    /// NodeBuilderWithNodeIdAndWorkingDirAndCommunicator::recovering_with
    pub fn resuming_from<S>(
        self,
        snapshot: impl Into<Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>>,
    ) -> NodeBuilderWithAll<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        // TODO this seems a bit fragile
        let participation = match self.working_dir {
            Some(_) => Participaction::Active,
            None => Participaction::Passive,
        };

        self.with_snapshot_and_participation(snapshot.into(), participation)
    }

    /// Resume operation from the given snapshot.
    ///
    /// The node will participate passively until it can be certain that it is
    /// not breaking any previous commitments.
    pub fn recovering_with<S>(
        self,
        snapshot: impl Into<Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>>,
    ) -> NodeBuilderWithAll<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(snapshot.into(), Participaction::Passive)
    }

    /// Resume operation from the given snapshot.
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
    ) -> NodeBuilderWithAll<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        self.with_snapshot_and_participation(snapshot.into(), Participaction::Active)
    }

    fn with_snapshot_and_participation<S>(
        self,
        snapshot: Option<Snapshot<S, RoundNumOf<C>, CoordNumOf<C>>>,
        participation: Participaction,
    ) -> NodeBuilderWithAll<NodeKernel<S, C>>
    where
        S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
    {
        NodeBuilderWithAll {
            working_dir: self.working_dir,
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot,
            participation,
            log_keeping: Default::default(),
            finisher: Box::new(|x| Ok(x)),

            #[cfg(feature = "tracer")]
            tracer: None,
        }
    }
}

pub struct NodeBuilderWithAll<N: Node> {
    working_dir: Option<std::path::PathBuf>,
    node_id: NodeIdOf<StateOf<N>>,
    communicator: CommunicatorOf<N>,
    snapshot:
        Option<Snapshot<StateOf<N>, RoundNumOf<CommunicatorOf<N>>, CoordNumOf<CommunicatorOf<N>>>>,
    participation: Participaction,
    log_keeping: LogKeeping,
    finisher: Box<dyn FnOnce(NodeKernel<StateOf<N>, CommunicatorOf<N>>) -> Result<N, SpawnError>>,

    #[cfg(feature = "tracer")]
    tracer: Option<
        Box<
            dyn Tracer<
                RoundNumOf<CommunicatorOf<N>>,
                CoordNumOf<CommunicatorOf<N>>,
                LogEntryIdOf<StateOf<N>>,
            >,
        >,
    >,
}

impl<S, C> NodeBuilderWithAll<NodeKernel<S, C>>
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
    pub fn traced_by(
        mut self,
        tracer: impl Into<Box<dyn Tracer<RoundNumOf<C>, CoordNumOf<C>, LogEntryIdOf<S>>>>,
    ) -> Self {
        self.tracer = Some(tracer.into());

        self
    }
}

impl<N: 'static + Node> NodeBuilder for NodeBuilderWithAll<N> {
    type Node = N;
    type Future = LocalBoxFuture<
        'static,
        Result<
            (
                RequestHandler<
                    StateOf<N>,
                    RoundNumOf<CommunicatorOf<N>>,
                    CoordNumOf<CommunicatorOf<N>>,
                >,
                N,
            ),
            SpawnError,
        >,
    >;

    fn decorated_with<D>(self, arguments: <D as Decoration>::Arguments) -> NodeBuilderWithAll<D>
    where
        D: Decoration<Decorated = N, State = StateOf<N>, Communicator = CommunicatorOf<N>>,
    {
        let finisher = self.finisher;

        NodeBuilderWithAll {
            working_dir: self.working_dir,
            node_id: self.node_id,
            communicator: self.communicator,
            snapshot: self.snapshot,
            participation: self.participation,
            log_keeping: self.log_keeping,
            finisher: Box::new(move |x| ((finisher)(x)).and_then(|node| D::wrap(node, arguments))),

            #[cfg(feature = "tracer")]
            tracer: self.tracer,
        }
    }

    fn spawn_in(self, context: ContextOf<StateOf<N>>) -> Self::Future {
        let finisher = self.finisher;

        NodeKernel::new(
            context,
            self.working_dir,
            self.node_id,
            self.communicator,
            self.snapshot,
            self.participation,
            self.log_keeping,
            #[cfg(feature = "tracer")]
            self.tracer,
        )
        .and_then(|(req_handler, kernel)| {
            futures::future::ready(finisher(kernel).map(|node| (req_handler, node)))
        })
        .boxed_local()
    }
}
