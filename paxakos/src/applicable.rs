use std::sync::Arc;

use crate::log::LogEntry;
use crate::state::LogEntryOf;
use crate::state::OutcomeOf;
use crate::state::State;

pub type ProjectedOf<A, S> = <ProjectionOf<A, S> as Projection<OutcomeOf<S>>>::Projected;
pub type ProjectionOf<A, S> = <A as ApplicableTo<S>>::Projection;

pub trait ApplicableTo<S: State> {
    type Projection: Projection<OutcomeOf<S>>;

    fn into_log_entry(self) -> Arc<LogEntryOf<S>>;
}

impl<S: State<LogEntry = E>, E: LogEntry> ApplicableTo<S> for E {
    type Projection = Identity;

    fn into_log_entry(self) -> Arc<<S as State>::LogEntry> {
        Arc::new(self)
    }
}

impl<S: State<LogEntry = E>, E: LogEntry> ApplicableTo<S> for Arc<E> {
    type Projection = Identity;

    fn into_log_entry(self) -> Arc<<S as State>::LogEntry> {
        self
    }
}

pub trait Projection<T>: Send + Unpin {
    type Projected;

    fn project(val: T) -> Self::Projected;
}

#[derive(Debug)]
pub struct Identity;

impl<T> Projection<T> for Identity {
    type Projected = T;

    fn project(val: T) -> Self::Projected {
        val
    }
}
