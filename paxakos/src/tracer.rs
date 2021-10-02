//! Defines the [`Tracer`] trait.

use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryIdOf;
use crate::invocation::RoundNumOf;

/// Records events as they are observed by a node.
///
/// Tracing events isn't useful under normal operation. It can aid in
/// understanding when and how inconsistencies were introduced. As such, some
/// tests set a tracer so that they can emit a trace if they fail.
///
/// See [`NodeBuilder::traced_by`][crate::NodeBuilder::traced_by].
pub trait Tracer<I: Invocation>: Send {
    /// A promise was made.
    fn record_promise(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        promise: Vec<(RoundNumOf<I>, CoordNumOf<I>, LogEntryIdOf<I>)>,
    );

    /// A proposal was accepted.
    fn record_accept(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        log_entry_id: LogEntryIdOf<I>,
    );

    /// A log entry was committed.
    fn record_commit(&mut self, round_num: RoundNumOf<I>, log_entry_id: LogEntryIdOf<I>);
}
