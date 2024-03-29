//! Paxakos is a pure Rust implementation of a distributed consensus algorithm
//! based on Leslie Lamport's [Paxos][wikipedia]. It enables distributed systems
//! to consistently modify shared state across their network, even in the
//! presence of failures.
//!
//! <details>
//! <summary>Check out the <a href="https://benschulz.github.io/paxakos/playground/?#">playground example</a>.</summary>
//! <iframe src="https://benschulz.github.io/paxakos/playground/?#" style="width: 100%; height: 800px;"></iframe>
//! </details>
//!
//! [wikipedia]: https://en.wikipedia.org/wiki/Paxos_(computer_science)
//!
//! [![crates.io][crates.io-badge]][crates.io-url]
//! [![docs.rs][docs.rs-badge]][docs.rs-url]
//! [![GPLv3 licensed][gpl-badge]][gpl-url]
//!
//! [crates.io-badge]: https://img.shields.io/crates/v/paxakos.svg
//! [crates.io-url]: https://crates.io/crates/paxakos
//! [docs.rs-badge]: https://docs.rs/paxakos/badge.svg
//! [docs.rs-url]: https://docs.rs/paxakos
//! [gpl-badge]: https://img.shields.io/badge/license-GPLv3-blue.svg
//! [gpl-url]: https://github.com/benschulz/paxakos/blob/master/LICENSE
//!
//! # Usage
//!
//! In order to use Paxakos, the traits [`LogEntry`], [`State`], [`NodeInfo`]
//! and [`Communicator`][Communicator] need to be implemented. The first two
//! describe what state will be replicated across the network and which
//! operations on it are available. The latter describe the nodes in your
//! network and how to communicate between them.
//!
//! Below are two _partial_ implementations of `LogEntry` and `State`. The other
//! two traits are more abstract and won't be illustrated here. Please refer to
//! the examples to get a fuller picture.
//!
//! ```
//! use std::collections::HashSet;
//! use std::convert::Infallible;
//!
//! use paxakos::{LogEntry, State};
//! use uuid::Uuid;
//!
//! #[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
//! pub enum CalcOp {
//!     Add(f64, Uuid),
//!     Div(f64, Uuid),
//!     Mul(f64, Uuid),
//!     Sub(f64, Uuid),
//! }
//!
//! impl LogEntry for CalcOp {
//!     type Id = Uuid;
//!
//!     fn id(&self) -> Self::Id {
//!         match self {
//!             CalcOp::Add(_, id)
//!             | CalcOp::Div(_, id)
//!             | CalcOp::Mul(_, id)
//!             | CalcOp::Sub(_, id) => {
//!                 *id
//!             }
//!         }
//!     }
//! }
//!
//! #[derive(Clone, Debug)]
//! pub struct CalcState {
//!     applied: HashSet<Uuid>,
//!     value: f64,
//! }
//!
//! impl State for CalcState {
//! #   type Frozen = Self;
//! #
//!     type Context = ();
//!
//!     type LogEntry = CalcOp;
//!     type Outcome = f64;
//!     type Effect = f64;
//!     type Error = Infallible;
//!
//! #   type Node = ();
//! #   
//! #   fn cluster_at(&self, round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
//! #       vec![()]
//! #   }
//! #   
//!     fn apply(
//!         &mut self,
//!         log_entry: &Self::LogEntry,
//!         _context: &mut (),
//!     ) -> Result<(Self::Outcome, Self::Effect), Self::Error> {
//!         if self.applied.insert(log_entry.id()) {
//!             match log_entry {
//!                 CalcOp::Add(v, _) => {
//!                     self.value += v;
//!                 }
//!                 CalcOp::Div(v, _) => {
//!                     self.value /= v;
//!                 }
//!                 CalcOp::Mul(v, _) => {
//!                     self.value *= v;
//!                 }
//!                 CalcOp::Sub(v, _) => {
//!                     self.value -= v;
//!                 }
//!             }
//!         }
//!
//!         Ok((self.value, self.value))
//!     }
//!
//!     fn freeze(&self, _context: &mut Self::Context) -> Self::Frozen {
//!         self.clone()
//!     }
//! }
//! ```
//!
//! # Motivation
//!
//! Rust is a great language with which to implement efficient and truly
//! reliable, fault-tolerant services. And while there already are several Rust
//! implementations of consensus algorithms, they are either rudimentary or
//! incomplete or their API was not approachable enough for this author.
//!
//! # Priorities
//!
//! The project's priorities are as follows.
//!
//! 1. **Correctness**
//!
//!    The highest priority is _correctness_, which, in the context of
//!    consensus, requires _stability_, _consistency_ and _liveness_.
//!
//!    - **Stability** implies that once a node learns that a log entry `a` has
//!      been appended to the distributed log, it will never learn that a
//!      different entry `b` belongs in its place.
//!    - **Consistency** means that all nodes in the Paxakos network agree on
//!      the contents of their shared log. While nodes may temporarily fall
//!      behind, i.e. their log may be shorter than other nodes', their logs
//!      must be otherwise equivalent.
//!    - **Liveness** means that the system won't get stuck, i.e. progress is
//!      always eventually made (assuming a there is no contention/some degree
//!      of cooperation).
//!
//! 2. **Simplicity**
//!
//!    Paxakos aims to be simple by providing few orthogonal primitives. To be
//!    clear, the goal is not to arbitrarily limit complexity. The goal is to
//!    have unentangled primitves; providing the same features with the least
//!    amount of complexity possible.
//!
//! 3. **Ergonomics**
//!
//!    Using Paxakos should be as pleasant as possible without sacrificing
//!    correctness or simplicity. The biggest challenge in this area are, at
//!    present, build times. If you have other concerns, please open an issue.
//!
//! # Features
//!
//! Paxakos is a Multi-Paxos implementation. It supports membership changes,
//! concurrency, as well as taking and installing snapshots.
//!
//! ## Membership Changes
//!
//! The `State` trait exposes the [`cluster_at`][State::cluster_at] method. By
//! implementing it, _arbitrary_ membership changes may be made. No restrictions
//! are imposed and it is up to users and implementors to make sure that any
//! transition is safe.
//!
//! ## Concurrency
//!
//! Multi-Paxos allows for any number of rounds to be settled concurrently. This
//! can be exploited by implementing `State`'s [concurrency][State::concurrency]
//! method.
//!
//! Please note that when concurrency is enabled, gaps may appear in the log.
//! These must be closed before entries after them can be applied to the state.
//! This is typically done by appending no-op entries. A utility for doing so
//! automatically is provided, but its API is far from stable.
//!
//! ## Snapshots
//!
//! A node may take a snapshot of its current state. The result is a combination
//! of the application specific `State` as well as pakakos specifc information.
//! These snapshots may be used for graceful shutdown and restart, to bootstrap
//! nodes which have joined the cluster or to catch up nodes that have fallen
//! behind.
//!
//! ## Decorations
//!
//! Implementations of the [`Decoration`][crate::decoration::Decoration] trait
//! can provide reusable functionality than _can_ but need not be used. Paxakos
//! comes with several decorations (see below).
//!
//! ## Heartbeats (`heartbeats` flag)
//!
//! The heartbeats decoration sends a heartbeat message at regular intervals.
//!
//! ## Autofill (`autofill` flag)
//!
//! From time to time gaps will appear in the distributed log. For example due
//! to [concurrency](#Concurrency) or dropped messages. When that happens, the
//! `autofill` decoration will fill the gap, implicitly catching the node up or
//! making sure that queued log entries may be applied.
//!
//! ## Track Leadership (`track-leadership` flag)
//!
//! The leadership tracking decoration infers which nodes are leading the
//! cluster.
//!
//! ## Ensure Leadership (experimental, `ensure-leadership` flag)
//!
//! Similar to heartbeats, the ensure leadership decoration will append a log
//! entry after none has been for a certain amount of time. However the goal of
//! this decoration is to ensure there is a leader.
//!
//! ## Automatic Delegation (experimental, `delegation` flag)
//!
//! The `Delegate` decoration allows automatic delegation of appends, ordinarily
//! to the leader node.
//!
//! ## Automatic Catch-Up (experimental, `catch-up` flag)
//!
//! When a node is started or just installed a snapshot it is likely lagging.
//! The `CatchUp` decoration will poll other nodes until the node is caught up.
//!
//! ## Verification of Consistency (experimental, `verify` flag)
//!
//! The `Verify` decoration will periodically check that all nodes are
//! consistent with each other. Nodes that have become inconsistent with the
//! majority eject their state to reduce the chance of propagation.
//!
//! ## Leases
//!
//! A cluster will often have shared resources which must be locked before they
//! may be accessed. To account for node failures, locks time out unless they
//! are refreshed. Such locks are commonly referred to as "leases".
//!
//! ### Leaser (experimental, `leaser` flag)
//!
//! `Leaser` makes taking, refreshing and releasing a lease as convenient as
//! calling [`take_lease`][crate::leases::leaser::Leaser::take_lease]. The lease
//! is refreshed as long as the returned value is held onto.
//!
//! ### Releaser (experimental, `releaser` flag)
//!
//! The releaser decoration makes sure that leases that have timed out are
//! cleared away, releasing the underlying resource.
//!
//! ## Master Leases (experimental, `master-leases` flag)
//!
//! Master leases are a mechanism to allow passive local reads. A comprehensive
//! description may be found in [Paxos Made Live][paxos-made-live].
//!
//! [paxos-made-live]: https://dl.acm.org/doi/10.1145/1281100.1281103
//!
//! # Protocol
//!
//! This section describes the Paxakos protocol. It is, for the most part, a
//! typical _Multi-Paxos_ protocol. Multi-Paxos generalizes Paxos to be run for
//! multiple rounds, where each round represents a slot in the distributed log.
//! Nodes may propose log entries to place in those slots. The liveness property
//! guarantees that the cluster will, for each round, eventually converge on one
//! of the proposed entries.
//!
//! A central component of the protocol are _coordination numbers_. These are
//! usually called "proposal numbers". However, because they are used throughout
//! the protocol, Paxakos uses the more abstract term.
//!
//! Appending an entry to the distributed log takes the following five steps.
//!
//! 1. Prepare `(RoundNum, CoordNum)`
//!
//!    In order for a node to append an entry to the distributed log, it must
//!    first become leader for the given round. If it already believes itself
//!    leader for the round, it will skip to step 3.
//!
//!    To become leader for a round the node will send a prepare message
//!    containing the round number and a coordination number. The coordination
//!    number is chosen so that it is
//!
//!     - higher than any previously encountered coordination number and
//!     - guaranteed not to be used by another node.
//!
//!    The former is important for liveness. The latter is required for
//!    stability and consistency and is achieved by exploiting the deterministic
//!    order of nodes returned by [`cluster_at`][State::cluster_at].
//!
//! 2. Vote
//!
//!    When a node receives a prepare request, it checks that it hasn't accepted
//!    a previous such request with a coordination number that's higher than the
//!    given one. If it has, it will reply with a conflict. If it hasn't, the
//!    node will either abstain, i.e., choose not to give its vote, or it sends
//!    back a _promise_ not to accept any more _proposals_ with a coordination
//!    number that's less the given one.
//!
//!    1. Promise `(Vec<(RoundNum, CoordNum, LogEntry)>)`
//!
//!       The promise is a set of triples, each consisting of a round number, a
//!       coordination number and a log entry. It can be thought to mean "I
//!       acknowledge your bid to become leader and give you my vote. However,
//!       in return you must respect these previous commitments I've made."
//!
//!    2. Conflict `(CoordNum, Option<(CoordNum, LogEntry)>)`
//!
//!       A rejection is sent with the greatest observed coordination number so
//!       far. For the special case that the round has already converged and the
//!       node still has it available, it will send it along as well.
//!
//!    3. Abstention `A`
//!
//!       The node chose to abstain. By default nodes will never abstain. This
//!       can be changed by providing a custom `Voter` implementation. The
//!       argument type `A` is defined by `Communicator::Abstain` and
//!       `Voter::Abstain`.
//!
//! 3. Propose `(RoundNum, CoordNum, LogEntry)`
//!
//!    When a node sent a `prepare(r, c)` request and received a quorum or more
//!    of promises in return (counting its own), it will believe itself to be
//!    leader for all rounds `r..`. It may now start proposing log entries for
//!    any of these rounds, using `c` as the coordination number.
//!
//!    The only restriction is that it must respect the promises it has
//!    received. If multiple promises contain a triple with the same round
//!    number, the one with the greatest coordination number wins. (Triples with
//!    the same round and coordination number will have the same log entry as
//!    well.)
//!
//! 4. Vote
//!
//!    When a node receives a proposal, it will check whether it has made any
//!    conflicting promises. If it has, it responds with a conflict. If it
//!    hasn't, it may choose to accept or reject the proposal and reply
//!    accordingly.
//!
//!    1. Acceptance `Y` / Rejection `N`
//!
//!       By default nodes accept all proposals with `Y = ()`. This can be
//!       changed by providing a custom `Voter` implementation. The argument
//!       types `Y` and `N` are defined by `Communicator::Yea`,
//!       `Communicator::Nay`, `Voter::Yea` and `Voter::Nay`.
//!
//!    2. Conflict `(CoordNum, Option<(CoordNum, LogEntry)>)`
//!
//!       See 2.2.
//!
//! 5. Commit `(RoundNum, CoordNum, LogEntry::Id)` /
//!    CommitById `(RoundNum, CoordNum, LogEntry)`
//!
//!    After having received a quorum of acceptances, the round has converged on
//!    the proposed entry. The leader node will commit the entry locally and
//!    inform other nodes as well. Nodes who sent an acceptance will only be
//!    sent the log entry's id, others will receive the full entry.
//!
//! # Project Status
//!
//! This section examines different aspects of paxakos, how far along they are
//! and what's missing or in need of improvement.
//!
//! ## ☀️ Consensus Implementation ☀️
//!
//! The core algorithm of paxakos is well-tested and has been exercised a lot.
//! There is reason to be confident in its correctness.
//!
//! ## ⛅ Passive Mode ⛅
//!
//! [Passive mode][crate::node::Participation] is implemented and superficially
//! tested. Thorough testing is still needed.
//!
//! ## ⛅ Serialization ⛅
//!
//! Snapshot serialization is serde based and still maturing.
//!
//! ## ⛅ API Stability ⛅
//!
//! The API has been fairly stable and there is no reason to expect big changes.
//! The decorations may be reworked so that their configuration *can* be moved
//! into the `State`.
//!
//! ## ⛅ Efficiency ⛅
//!
//! Paxakos supports concurrency and has a (for now rudimentary) implementation
//! of master leases. Assuming a scheme to delegate to the current master, this
//! combination is highly efficient.
//!
//! # Future Direction
//!
//! This is a side project. I will work on the following as my time allows (in
//! no particular order).
//!
//!  - Tests
//!  - Adding comments and documentation
//!  - Rounding off existing decorations
//!  - Implement decoration for read delegation
//!  - Find way to detect and react to when a node was left behind
//!  - Update playground
//!    - Allow configuration of `CatchUp` and `Verify`
//!    - Visualize `CatchUp`
//!    - Add master-lease decoration
//!    - Add delegate decoration
//!
//! [Communicator]: crate::communicator::Communicator
//! [LogEntry]: crate::log::LogEntry
//! [NodeInfo]: crate::node::NodeInfo
//! [State]: crate::state::State
//! [State::cluster_at]: crate::state::State::cluster_at

//
// Lint configuration
#![warn(rust_2018_idioms)]
#![warn(clippy::wildcard_imports)]
#![warn(missing_docs)]

pub mod append;
pub mod applicable;
#[cfg(feature = "autofill")]
pub mod autofill;
pub mod buffer;
#[cfg(feature = "catch-up")]
pub mod catch_up;
pub mod cluster;
pub mod communicator;
pub mod decoration;
#[cfg(feature = "delegation")]
pub mod delegate;
pub mod error;
pub mod event;
pub mod executor;
#[cfg(feature = "heartbeats")]
pub mod heartbeats;
pub mod invocation;
pub mod leadership;
#[cfg(feature = "leases")]
pub mod leases;
mod log_entry;
pub mod node;
#[cfg(feature = "prototyping")]
pub mod prototyping;
pub mod retry;
pub mod state;
#[cfg(feature = "tracer")]
pub mod tracer;
mod util;
#[cfg(feature = "verify")]
pub mod verify;
pub mod voting;

use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

// TODO move these three into the communicator module
pub use error::AcceptError;
pub use error::CommitError;
pub use error::PrepareError;
pub use event::Event;
pub use event::ShutdownEvent;
#[doc(inline)]
pub use invocation::Invocation;
#[doc(inline)]
pub use log_entry::LogEntry;
#[doc(inline)]
pub use node::builder as node_builder;
pub use node::Commit;
pub use node::Core;
pub use node::Node;
pub use node::NodeBuilder;
pub use node::NodeHandle;
pub use node::NodeInfo;
pub use node::NodeKit;
pub use node::NodeStatus;
pub use node::RequestHandler;
pub use node::Shell;
pub use node::Shutdown;
#[doc(inline)]
pub use state::State;

/// Trait bound of both [`CoordNum`] as well as [`RoundNum`].
pub trait Number:
    'static
    + num_traits::Bounded
    + num_traits::Num
    + Copy
    + Send
    + Sync
    + Unpin
    + Ord
    + Hash
    + Debug
    + std::fmt::Display
    + Into<u128>
    + TryFrom<u128>
    + TryFrom<usize>
    + TryInto<usize>
where
    Self: serde::Serialize,
    for<'de> Self: serde::Deserialize<'de>,
{
}

impl<T> Number for T where
    T: 'static
        + num_traits::Bounded
        + num_traits::Num
        + Copy
        + Send
        + Sync
        + Unpin
        + Ord
        + Hash
        + Debug
        + std::fmt::Display
        + Into<u128>
        + TryFrom<u128>
        + TryFrom<usize>
        + TryInto<usize>
        + serde::de::DeserializeOwned
        + serde::Serialize
{
}

/// A round number.
///
/// Please refer to the [description of the protocol](crate#protocol).
pub trait RoundNum: Number {}

impl<T: Number> RoundNum for T {}

/// A coordination number.
///
/// Please refer to the [description of the protocol](crate#protocol).
pub trait CoordNum: Number {}

impl<T: Number> CoordNum for T {}

/// Trait bound of [log entry ids][crate::LogEntry::Id] and [node
/// ids][crate::NodeInfo::Id].
pub trait Identifier: 'static + Copy + Debug + Eq + Hash + Send + Sync + Unpin {}

impl<T: 'static + Copy + Debug + Eq + Hash + Ord + Send + Sync + Unpin> Identifier for T {}

/// A condition for a [Promise].
///
/// When a node _promises_, i.e. votes for a candidate, it might require the
/// candidate to honor commitments the node had previously made. This struct
/// represents such a requirement.
///
/// A `Condition` is a triple of round number, coordination number and log
/// entry. At [the protocol level](crate#protocol), this triple corresponds to a
/// proposal that the node previously accepted.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Condition<R, C, E> {
    /// The condition's round number.
    pub round_num: R,
    /// The condition's coordination number.
    pub coord_num: C,
    /// The condition's log entry.
    pub log_entry: Arc<E>,
}

impl<R, C, E> From<Condition<R, C, E>> for (R, C, Arc<E>) {
    fn from(val: Condition<R, C, E>) -> Self {
        (val.round_num, val.coord_num, val.log_entry)
    }
}

impl<R, C, E> From<(R, C, Arc<E>)> for Condition<R, C, E> {
    fn from(condition: (R, C, Arc<E>)) -> Self {
        let (round_num, coord_num, log_entry) = condition;

        Self {
            round_num,
            coord_num,
            log_entry,
        }
    }
}

/// A promise not to accept certain proposals anymore.
///
/// Please refer to the [description of the protocol](crate#protocol).
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Promise<R, C, E>(Vec<Condition<R, C, E>>);

impl<R: RoundNum, C, E> From<Vec<Condition<R, C, E>>> for Promise<R, C, E> {
    fn from(mut conditions: Vec<Condition<R, C, E>>) -> Self {
        conditions.sort_by_key(|c| c.round_num);

        Self(conditions)
    }
}

impl<R, C, E> From<Promise<R, C, E>> for Vec<Condition<R, C, E>> {
    fn from(val: Promise<R, C, E>) -> Self {
        val.0
    }
}

impl<R, C, E> IntoIterator for Promise<R, C, E> {
    type Item = Condition<R, C, E>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<R: RoundNum, C: CoordNum, E> Promise<R, C, E> {
    /// The conditions this promise is predicated on.
    pub fn conditions(&self) -> &[Condition<R, C, E>] {
        &self.0
    }

    pub(crate) fn empty() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn log_entry_for(&self, round_num: R) -> Option<Arc<E>> {
        // TODO exploit order
        self.0
            .iter()
            .find(|c| c.round_num == round_num)
            .map(|c| Arc::clone(&c.log_entry))
    }

    pub(crate) fn merge_with(self, other: Self) -> Self {
        let a = self.0;
        let b = other.0;

        let mut max = Vec::with_capacity(a.len() + b.len());

        let mut a = a.into_iter();
        let mut b = b.into_iter();

        let mut a_next = a.next().map(Into::into);
        let mut b_next = b.next().map(Into::into);

        loop {
            match (a_next, b_next) {
                (Some((ra, ca, ea)), Some((rb, cb, eb))) => match ra.cmp(&rb) {
                    std::cmp::Ordering::Equal => {
                        if ca > cb {
                            max.push(Condition::from((ra, ca, ea)));
                        } else {
                            max.push(Condition::from((rb, cb, eb)));
                        }

                        a_next = a.next().map(Into::into);
                        b_next = b.next().map(Into::into);
                    }
                    std::cmp::Ordering::Less => {
                        max.push(Condition::from((ra, ca, ea)));
                        a_next = a.next().map(Into::into);
                        b_next = Some((rb, cb, eb));
                    }
                    std::cmp::Ordering::Greater => {
                        max.push(Condition::from((rb, cb, eb)));
                        b_next = b.next().map(Into::into);
                        a_next = Some((ra, ca, ea));
                    }
                },
                (Some((ra, ca, ea)), None) => {
                    max.push(Condition::from((ra, ca, ea)));
                    max.extend(a);
                    return Promise(max);
                }
                (None, Some((rb, cb, eb))) => {
                    max.push(Condition::from((rb, cb, eb)));
                    max.extend(b);
                    return Promise(max);
                }
                (None, None) => {
                    return Promise(max);
                }
            }
        }
    }
}

/// Conflict to a prepare request or a proposal.
///
/// Please refer to the [description of the protocol](crate#protocol).
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Conflict<C, E> {
    /// Another node had itself elected leader.
    Supplanted {
        /// The coordination number the other node used to become leader.
        coord_num: C,
    },
    /// The given round has already converged.
    Converged {
        /// The greatest coordination number the node that sent the `Converged`
        /// message had observed.
        coord_num: C,
        /// The coordination number that was used to settle the round and the
        /// log entry which was committed.
        ///
        /// May be `None` if the node that sent the `Converged` message no
        /// longer had this information in its [applied entry
        /// buffer][crate::buffer::Buffer].
        log_entry: Option<(C, Arc<E>)>,
    },
}

impl<C: CoordNum, E> Conflict<C, E> {
    pub(crate) fn coord_num(&self) -> C {
        match *self {
            Conflict::Supplanted { coord_num } => coord_num,
            Conflict::Converged { coord_num, .. } => coord_num,
        }
    }
}
