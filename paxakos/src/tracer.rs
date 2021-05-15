use crate::communicator::{CoordNumOf, LogEntryIdOf, RoundNumOf};

pub trait Tracer<C>: Send
where
    C: crate::communicator::Communicator,
{
    fn record_promise(
        &mut self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        promise: Vec<(RoundNumOf<C>, CoordNumOf<C>, LogEntryIdOf<C>)>,
    );

    fn record_accept(
        &mut self,
        round_num: RoundNumOf<C>,
        coord_num: CoordNumOf<C>,
        log_entry_id: LogEntryIdOf<C>,
    );

    fn record_commit(&mut self, round_num: RoundNumOf<C>, log_entry_id: LogEntryIdOf<C>);
}
