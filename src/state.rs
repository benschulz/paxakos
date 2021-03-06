use std::fmt::Debug;

use async_trait::async_trait;
use futures::io::AsyncRead;

use crate::{LogEntry, NodeInfo};

/// Type alias to extract a state's [`Context`][State::Context] type.
pub type ContextOf<S> = <S as State>::Context;

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

/// Type alias to extract a state's [`Event`][State::Event] type.
pub type EventOf<S> = <S as State>::Event;

/// Distributed state to which log entries are applied.
#[async_trait(?Send)]
pub trait State: 'static + Clone + Debug + Send + Sized + Sync {
    type Reader: std::io::Read;

    type LogEntry: LogEntry;

    /// An execution context.
    ///
    /// The execution context commonly provides access to a working directory or
    /// other state that is node and instance specific.
    type Context: Debug + Send;

    /// Result of applying a log entry to the state.
    ///
    /// This result is what those actively appending to the log receive.
    type Outcome: 'static + Clone + Debug + Send + Sync + Unpin;

    /// Result of applying a log entry to the state.
    ///
    /// This result is emitted as an [Apply event][Apply] event.
    ///
    /// [Apply]: crate::event::Event::Apply
    type Event: 'static + Send + Debug;

    type Node: NodeInfo;

    /// Deserializes state that was previously serialized using `to_reader()`.
    ///
    /// While implementations need not detect arbitrary data corruption, they
    /// must not panic.
    async fn from_reader<R: AsyncRead + Unpin>(read: R) -> Result<Self, crate::error::BoxError>;

    /// Number of bytes the result of `to_reader()` will emit.
    fn size(&self) -> usize;

    /// Serializes the state to enable snapshots.
    ///
    /// `State::from_reader(s.to_reader())` must yield an equivalent state.
    fn to_reader(&self) -> Self::Reader;

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
    ) -> (Self::Outcome, Self::Event);

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
    ) -> Self::Event {
        self.apply(log_entry, context).1
    }

    /// Returns whether an entry with `log_entry_id` was applied to this state.
    ///
    /// This method is called before a node attempts to append an entry to the
    /// log. When `false` is returned the node proceeds in its attempt.
    /// Otherwise the attempt is abandoned with an appropriate error.
    ///
    /// Careful: Please note that this is strictly an optimization. There is no
    /// way to prevent an event from being applied multiple times unless the
    /// concurrency level is reduced to one.
    // TODO this method is no longer used, consider removing it entirely
    fn contains(&self, _log_entry_id: <Self::LogEntry as LogEntry>::Id) -> Result<bool, ()> {
        Ok(false)
    }

    /// Returns the current level of concurrency, defaults to one.
    fn concurrency(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(1).unwrap()
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
}
