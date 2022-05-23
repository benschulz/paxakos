//! Defines the [`Invocation`] trait.

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

/// Shorthand to extract `Abstain` type out of `I`.
pub type AbstainOf<I> = <I as Invocation>::Abstain;
/// Shorthand to extract `CommunicationError` type out of `I`.
pub type CommunicationErrorOf<I> = <I as Invocation>::CommunicationError;
/// Shorthand to extract state's `Context` type out of `I`.
pub type ContextOf<I> = state::ContextOf<StateOf<I>>;
/// Shorthand to extract `CoordNum` type out of `I`.
pub type CoordNumOf<I> = <I as Invocation>::CoordNum;
/// Shorthand to extract state's `Event` type out of `I`.
pub type EffectOf<I> = state::EffectOf<StateOf<I>>;
/// Shorthand to extract `Ejection` type out of `I`.
pub type EjectionOf<I> = <I as Invocation>::Ejection;
/// Shorthand to extract state's `Frozen` type out of `I`.
pub type FrozenStateOf<I> = state::FrozenOf<StateOf<I>>;
/// Shorthand to extract state's `LogEntry` type out of `I`.
pub type LogEntryOf<I> = state::LogEntryOf<StateOf<I>>;
/// Shorthand to extract log entry `Id` type out of `I`.
pub type LogEntryIdOf<I> = <LogEntryOf<I> as LogEntry>::Id;
/// Shorthand to extract `Nay` type out of `I`.
pub type NayOf<I> = <I as Invocation>::Nay;
/// Shorthand to extract state's `Node` type (`impl NodeInfo`) out of `I`.
pub type NodeOf<I> = state::NodeOf<StateOf<I>>;
/// Shorthand to extract node (`impl NodeInfo`) `Id` type out of `I`.
pub type NodeIdOf<I> = <NodeOf<I> as NodeInfo>::Id;
/// Shorthand to extract state's `Outcome` type out of `I`.
pub type OutcomeOf<I> = state::OutcomeOf<StateOf<I>>;
/// Shorthand to extract `RoundNum` type out of `I`.
pub type RoundNumOf<I> = <I as Invocation>::RoundNum;
/// Shorthand to extract `State` type out of `I`.
pub type StateOf<I> = <I as Invocation>::State;
/// Shorthand to extract `Yea` type out of `I`.
pub type YeaOf<I> = <I as Invocation>::Yea;

/// Invokes `Acceptance` type constructor so as to be compatible with `I`.
pub type AcceptanceFor<I> = Acceptance<CoordNumOf<I>, LogEntryOf<I>, YeaOf<I>, NayOf<I>>;
/// Invokes `Commit` type constructor so as to be compatible with `I`.
pub type CommitFor<I, A = LogEntryOf<I>> =
    Commit<StateOf<I>, RoundNumOf<I>, ProjectionOf<A, StateOf<I>>>;
/// Invokes `Conflict` type constructor so as to be compatible with `I`.
pub type ConflictFor<I> = Conflict<CoordNumOf<I>, LogEntryOf<I>>;
/// Invokes `Promise` type constructor so as to be compatible with `I`.
pub type PromiseFor<I> = Promise<RoundNumOf<I>, CoordNumOf<I>, LogEntryOf<I>>;
/// Invokes `Snapshot` type constructor so as to be compatible with `I`.
pub type SnapshotFor<I> = Snapshot<StateOf<I>, RoundNumOf<I>, CoordNumOf<I>>;
/// Invokes `Vote` type constructor so as to be compatible with `I`.
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
    /// Round number type.
    type RoundNum: RoundNum;
    /// Coordination number type.
    type CoordNum: CoordNum;

    /// State type.
    type State: State;

    /// Additional data sent along with 'yea' votes.
    type Yea: std::fmt::Debug + Send + Sync + 'static;
    /// Additional data sent along with 'nay' votes.
    type Nay: std::fmt::Debug + Send + Sync + 'static;
    /// Additional data sent along with abstentions.
    type Abstain: std::fmt::Debug + Send + Sync + 'static;

    /// Reason the node's state may be ejected.
    type Ejection: From<state::ErrorOf<Self::State>> + std::fmt::Debug + Send + Sync + 'static;

    /// Communication error type.
    type CommunicationError: std::fmt::Debug + Send + Sync + 'static;

    /// Constructs a blank node builder for this set of type arguments.
    fn node_builder() -> NodeBuilderBlank<Self> {
        NodeBuilderBlank::new()
    }
}
