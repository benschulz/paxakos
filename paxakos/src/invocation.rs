use crate::append::AppendError;
use crate::applicable::ProjectionOf;
use crate::communicator::Acceptance;
use crate::communicator::Vote;
use crate::node::Snapshot;
use crate::node_builder::NodeBuilderBlank;
use crate::state;
use crate::state::State;
use crate::Commit;
use crate::Conflict;
use crate::CoordNum;
use crate::LogEntry;
use crate::NodeInfo;
use crate::Promise;
use crate::RoundNum;

pub type AbstainOf<I> = <I as Invocation>::Abstain;
pub type CommunicationErrorOf<I> = <I as Invocation>::CommunicationError;
pub type ContextOf<I> = state::ContextOf<StateOf<I>>;
pub type CoordNumOf<I> = <I as Invocation>::CoordNum;
pub type EventOf<I> = state::EventOf<StateOf<I>>;
pub type LogEntryOf<I> = state::LogEntryOf<StateOf<I>>;
pub type LogEntryIdOf<I> = <LogEntryOf<I> as LogEntry>::Id;
pub type NayOf<I> = <I as Invocation>::Nay;
pub type NodeOf<I> = state::NodeOf<StateOf<I>>;
pub type NodeIdOf<I> = <NodeOf<I> as NodeInfo>::Id;
pub type OutcomeOf<I> = state::OutcomeOf<StateOf<I>>;
pub type RoundNumOf<I> = <I as Invocation>::RoundNum;
pub type StateOf<I> = <I as Invocation>::State;
pub type YeaOf<I> = <I as Invocation>::Yea;

pub type AcceptanceFor<I> = Acceptance<CoordNumOf<I>, LogEntryOf<I>, YeaOf<I>, NayOf<I>>;
pub type AppendResultFor<I, A = LogEntryOf<I>> = Result<CommitFor<I, A>, AppendError<I>>;
pub type CommitFor<I, A = LogEntryOf<I>> =
    Commit<StateOf<I>, RoundNumOf<I>, ProjectionOf<A, StateOf<I>>>;
pub type ConflictFor<I> = Conflict<CoordNumOf<I>, LogEntryOf<I>>;
pub type PromiseFor<I> = Promise<RoundNumOf<I>, CoordNumOf<I>, LogEntryOf<I>>;
pub type SnapshotFor<I> = Snapshot<StateOf<I>, RoundNumOf<I>, CoordNumOf<I>>;
pub type VoteFor<I> = Vote<RoundNumOf<I>, CoordNumOf<I>, LogEntryOf<I>, AbstainOf<I>>;

/// A set of type arguments to invoke Paxakos with.
///
/// Idiomatic Rust code will usually have separate type parameters for every
/// type. This has several advantages, for instance type bounds need only be
/// placed on implementations which require them. However, as a generic
/// implementation of a fairly complex algorithm, Paxakos would require rather a
/// lot of individual type parameters. Therefore Paxakos foregoes the idiomatic
/// approach and accepts a single `Invocation` argument in most places.
pub trait Invocation: Sized + 'static {
    type RoundNum: RoundNum;
    type CoordNum: CoordNum;

    type State: State;

    type Yea: std::fmt::Debug + Send + Sync + 'static;
    type Nay: std::fmt::Debug + Send + Sync + 'static;
    type Abstain: std::fmt::Debug + Send + Sync + 'static;

    type CommunicationError: std::fmt::Debug + Send + Sync + 'static;

    fn node_builder() -> NodeBuilderBlank<Self> {
        NodeBuilderBlank::new()
    }
}
