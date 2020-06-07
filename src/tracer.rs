use crate::communicator::{CoordNumOf, LogEntryIdOf, RoundNumOf};

pub type TracerFor<C> = dyn Tracer<RoundNumOf<C>, CoordNumOf<C>, LogEntryIdOf<C>>;

pub trait Tracer<R, C, I>: std::fmt::Debug + Send
where
    R: crate::RoundNum,
    C: crate::CoordNum,
    I: crate::Identifier,
{
    fn record_promise(&mut self, round_num: R, coord_num: C, promise: Vec<(R, C, I)>);

    fn record_accept(&mut self, round_num: R, coord_num: C, log_entry_id: I);

    fn record_commit(&mut self, round_num: R, log_entry_id: I);
}
