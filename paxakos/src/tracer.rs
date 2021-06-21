use crate::invocation::CoordNumOf;
use crate::invocation::Invocation;
use crate::invocation::LogEntryIdOf;
use crate::invocation::RoundNumOf;

pub trait Tracer<I: Invocation>: Send {
    fn record_promise(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        promise: Vec<(RoundNumOf<I>, CoordNumOf<I>, LogEntryIdOf<I>)>,
    );

    fn record_accept(
        &mut self,
        round_num: RoundNumOf<I>,
        coord_num: CoordNumOf<I>,
        log_entry_id: LogEntryIdOf<I>,
    );

    fn record_commit(&mut self, round_num: RoundNumOf<I>, log_entry_id: LogEntryIdOf<I>);
}
