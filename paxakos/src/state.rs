//! Defines the [`State`] trait and convenience type aliases.

use std::fmt::Debug;

use crate::LogEntry;
use crate::NodeInfo;

/// Type alias to extract a state's [`Context`][State::Context] type.
pub type ContextOf<S> = <S as State>::Context;

/// Type alias to extract a state's [`Error`][State::Error] type.
pub type ErrorOf<S> = <S as State>::Error;

/// Type alias to extract a state's [`Frozen`][State::Frozen] type.
pub type FrozenOf<S> = <S as State>::Frozen;

/// Type alias to extract a state's [`LogEntry`][State::LogEntry] type.
pub type LogEntryOf<S> = <S as State>::LogEntry;

/// Type alias to extract [`Id`][LogEntry::Id] type of a state's
/// [`LogEntry`][State::LogEntry] type.
pub type LogEntryIdOf<S> = <LogEntryOf<S> as LogEntry>::Id;

/// Type alias to extract a state's [`Node`][State::Node] type.
pub type NodeOf<S> = <S as State>::Node;

/// Type alias to extract [`Id`][NodeInfo::Id] type of a state's
/// [`Node`][State::Node] type.
pub type NodeIdOf<S> = <NodeOf<S> as NodeInfo>::Id;

/// Type alias to extract a state's [`Outcome`][State::Outcome] type.
pub type OutcomeOf<S> = <S as State>::Outcome;

/// Type alias to extract a state's [`Effect`][State::Effect] type.
pub type EffectOf<S> = <S as State>::Effect;

/// Distributed state to which log entries are applied.
pub trait State: 'static + Debug + Send + Sized {
    /// Frozen representation of this state type.
    type Frozen: Frozen<Self> + Send + Sync + Debug;

    /// Type of log entry that can be applied to this state.
    type LogEntry: LogEntry;

    /// An execution context.
    ///
    /// The execution context commonly provides access to a working directory or
    /// other state that is node and instance specific.
    type Context: Debug + Send;

    /// Result of applying a log entry to the state.
    ///
    /// This result is what those actively appending to the log receive.
    type Outcome: 'static + Clone + Debug + Send + Unpin;

    /// Result of applying a log entry to the state.
    ///
    /// This result is emitted as an [`Apply` event][Apply] event.
    ///
    /// [Apply]: crate::event::Event::Apply
    type Effect: 'static + Send + Debug;

    /// Type of error that may occur when applying a log entry.
    type Error: Send + Sync + 'static;

    /// Node descriptor type, see [`NodeInfo`].
    type Node: NodeInfo;

    /// Applies the given log entry to this state object.
    ///
    /// Implementations do not have to account for out-of-order application. Log
    /// entries are applied in a consistent order accross the cluster.
    ///
    /// Note: The distributed log may contain duplicates of the same event, i.e.
    /// entries `e1` and `e2` may be appended to the log for rounds `r1` and
    /// `r2` such that `e1.id() == e2.id()` and `r1 != r2`. Implementations that
    /// choose to ignore the application of the second entry must take care to
    /// do so in a fashion that is consistent across the cluster.
    fn apply(
        &mut self,
        log_entry: &Self::LogEntry,
        context: &mut Self::Context,
    ) -> Result<(Self::Outcome, Self::Effect), Self::Error>;

    /// Applies the given log entry to this state object.
    ///
    /// This method may be implemented as an optimization. It is called instead
    /// of [apply](State::apply) when there are no observers for the result
    /// object.
    ///
    /// *Careful*: Implementations must be equivalent to the default
    /// implementation in their effects.
    fn apply_unobserved(
        &mut self,
        log_entry: &Self::LogEntry,
        context: &mut Self::Context,
    ) -> Result<Self::Effect, Self::Error> {
        self.apply(log_entry, context)
            .map(|(_outcome, effect)| effect)
    }

    /// Returns the current level of concurrency.
    ///
    /// Implementations must return `Some` when the argument is `Some`.
    ///
    /// This function's contract admits two kinds of implementation. The first
    /// kind returns a constant value independent of the argument. The second
    /// kind returns `None` when the argument is `None` and `Some(value)`
    /// otherwise. The latter kind allows for the level of concurrency to change
    /// dynamically yet remain consistent across the cluster.
    ///
    /// The default implementation always returns `Some(1)`.
    #[allow(unused_variables)]
    fn concurrency(this: Option<&Self>) -> Option<std::num::NonZeroUsize> {
        Some(std::num::NonZeroUsize::new(1).unwrap())
    }

    /// Returns the set of nodes that make up the cluster at the given round
    /// offset.
    ///
    /// The round offset is guaranteed to be less than or equal to the current
    /// level of concurrency. As such there must always be a known set of nodes
    /// that make up the cluster for the given round.
    ///
    /// The order of the returned set must be determisistic, meaning it must be
    /// consistent across the entire cluster.
    fn cluster_at(&self, round_offset: std::num::NonZeroUsize) -> Vec<Self::Node>;

    /// Creates a frozen copy of this state object to create a snapshot with.
    fn freeze(&self) -> Self::Frozen;
}

/// A frozen value that may be thawed.
pub trait Frozen<S> {
    /// Thaws this value.
    fn thaw(&self) -> S;
}

impl<S: Clone> Frozen<S> for S {
    fn thaw(&self) -> S {
        self.clone()
    }
}

/// Determines the level of concurrency given `state`.
///
/// This is a convenience function that delegates to [`State::concurrency`] and
/// then unwraps the returned value.
pub fn concurrency_of<S: State>(state: &S) -> std::num::NonZeroUsize {
    S::concurrency(Some(state)).expect("State::concurrency(Some(...)) returned None")
}
