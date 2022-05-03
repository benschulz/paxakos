//! Defines the [`ApplicableTo`] trait and related types.

use std::sync::Arc;

use crate::log_entry::LogEntry;
use crate::state::LogEntryOf;
use crate::state::OutcomeOf;
use crate::state::State;

/// Shorthand to extract `Projected` type out of `A as ApplicableTo<S>`.
pub type ProjectedOf<A, S> = <ProjectionOf<A, S> as Projection<OutcomeOf<S>>>::Projected;
/// Shorthand to extract `Projection` type out of `A as ApplicableTo<S>`.
pub type ProjectionOf<A, S> = <A as ApplicableTo<S>>::Projection;

/// Describes values that may be [applied][State::apply] to type `S`.
///
/// For any given `State` implementation there is usually a wide range of
/// possible operations. These are encoded by its corresponding `LogEntry` type,
/// which commonly contains (or is) an enum value whose variants correspond to
/// the state's operations. Different operations usually have different
/// outcome types, which is why [State::Outcome] is also commonly an enum.
///
/// This presents an ergonomics challenge. Imagine `enum MyLogEntry { A, B }`
/// and `enum MyOutcome { A(i64), B(bool) }`. When appending a `MyLogEntry::A`
/// we'd like to get back an `i64` rather than a `MyOutcome`. This can be
/// achieved as follows.
///
/// ```
/// # use std::sync::Arc;
/// #
/// # #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
/// # enum MyLogEntry {
/// #     A,
/// #     B,
/// # }
/// #
/// # #[derive(Clone, Debug)]
/// # enum MyOutcome {
/// #     A(i64),
/// #     B(bool),
/// # }
/// #
/// # #[derive(Clone, Debug)]
/// # struct MyState;
/// #
/// # impl paxakos::LogEntry for MyLogEntry {
/// #     type Id = ();
/// #
/// #     fn id(&self) -> Self::Id {
/// #         unimplemented!()
/// #     }
/// # }
/// # impl paxakos::State for MyState {
/// #     type Frozen = Self;
/// #
/// #     type LogEntry = MyLogEntry;
/// #
/// #     type Context = ();
/// #
/// #     type Outcome = MyOutcome;
/// #
/// #     type Effect = ();
/// #
/// #     type Node = ();
/// #
/// #     fn apply(
/// #         &mut self,
/// #         _log_entry: &Self::LogEntry,
/// #         _context: &mut Self::Context,
/// #     ) -> (Self::Outcome, Self::Effect) {
/// #         unimplemented!()
/// #     }
/// #
/// #     fn cluster_at(&self, _round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
/// #         unimplemented!()
/// #     }
/// #
/// #     fn freeze(&self) -> Self::Frozen {
/// #         unimplemented!()
/// #     }
/// # }
/// #
/// struct A;
///
/// impl paxakos::applicable::ApplicableTo<MyState> for A {
///     type Projection = ProjectionA;
///
///     fn into_log_entry(self) -> Arc<MyLogEntry> {
///         Arc::new(MyLogEntry::A)
///     }
/// }
///
/// struct ProjectionA;
///
/// impl paxakos::applicable::Projection<MyOutcome> for ProjectionA {
///     type Projected = i64;
///
///     fn project(val: MyOutcome) -> Self::Projected {
///         match val {
///             MyOutcome::A(i) => i,
///             _ => panic!("unexpected: {:?}", val)
///         }
///     }
/// }
/// ```
pub trait ApplicableTo<S: State> {
    /// Projection type, usually a zero-sized type.
    type Projection: Projection<OutcomeOf<S>>;

    /// Turns this applicable value into a log entry.
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

/// A projection from `T` to `Self::Projected`.
pub trait Projection<T>: Send + Unpin {
    /// The projected/image type.
    type Projected;

    /// Project `val`.
    fn project(val: T) -> Self::Projected;
}

/// The identity projection.
#[derive(Debug)]
pub struct Identity;

impl<T> Projection<T> for Identity {
    type Projected = T;

    fn project(val: T) -> Self::Projected {
        val
    }
}
