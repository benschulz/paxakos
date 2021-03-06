//! # paxakos
//!
//! Paxakos is a pure Rust implementation of a distributed consensus algorithm
//! based on Leslie Lamport's [Paxos][wikipedia]. It enables distributed systems
//! to consistently modify shared state across their network, even in the
//! presence of failures.
//!
//! [wikipedia]: https://en.wikipedia.org/wiki/Paxos_(computer_science)
//!
//! ## Usage
//!
//! In order to use Paxakos, the traits [`LogEntry`], [`State`], [`NodeInfo`]
//! and [`Communicator`][Communicator] need to be implemented. The first two
//! describe what state will be replicated across the network and which
//! operations on it are available. The latter describe the nodes in your
//! network and how to communicate between them.
//!
//! Below are two _partial_ implementations of `LogEntry` and `State`. The other
//! two traits are more abstract and won't be illustrated here.
//!
//! ```
//! use std::collections::HashSet;
//!
//! # use async_trait::async_trait;
//! # use futures::io::AsyncRead;
//! use paxakos::{LogEntry, State};
//! use uuid::Uuid;
//!
//! #[derive(Clone, Copy, Debug)]
//! pub enum CalcOp {
//!     Add(f64, Uuid),
//!     Div(f64, Uuid),
//!     Mul(f64, Uuid),
//!     Sub(f64, Uuid),
//! }
//!
//! # #[async_trait(?Send)]
//! impl LogEntry for CalcOp {
//!     type Id = Uuid;
//!
//! #   type Reader = std::io::Cursor<Vec<u8>>;
//! #   
//! #   async fn from_reader<R: AsyncRead + Unpin>(_read: R) -> Result<Self, paxakos::error::BoxError> {
//! #       unimplemented!()
//! #   }
//! #   
//! #   fn size(&self) -> usize {
//! #       unimplemented!()
//! #   }
//! #   
//! #   fn to_reader(&self) -> Self::Reader {
//! #       unimplemented!()
//! #   }
//! #
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
//! # #[async_trait(?Send)]
//! impl State for CalcState {
//!     type Context = ();
//!
//!     type LogEntry = CalcOp;
//!     type Outcome = f64;
//!     type Event = f64;
//!
//! #   type Reader = std::io::Cursor<Vec<u8>>;
//! #   
//! #   type Node = ();
//! #   
//! #   async fn from_reader<R: AsyncRead + Unpin>(_read: R) -> Result<Self, paxakos::error::BoxError> {
//! #       unimplemented!()
//! #   }
//! #   
//! #   fn size(&self) -> usize {
//! #       unimplemented!()
//! #   }
//! #   
//! #   fn to_reader(&self) -> Self::Reader {
//! #       unimplemented!()
//! #   }
//! #   
//! #   fn cluster_at(&self, round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
//! #       vec![()]
//! #   }
//! #   
//!     fn apply(
//!         &mut self,
//!         log_entry: &Self::LogEntry,
//!         _context: &mut (),
//!     ) -> (Self::Outcome, Self::Event) {
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
//!         (self.value, self.value)
//!     }
//! }
//! ```
//!
//! ### Working example
//!
//! Here is an excerpt of the [chat example][chat-example], which gives a more
//! complete picture of how the API is used once the necessary traits are
//! implemented.
//!
//! [chat-example]: https://github.com/benschulz/paxakos/blob/master/examples/chat/main.rs
//!
//! ```
//! //! This example is for illustrative purposes only. Basing a chat protocol on
//! //! consensus is a bad idea, but makes a neat example.
//!
//! # use async_trait::async_trait;
//! # use futures::io::AsyncRead;
//! use paxakos::append::AppendArgs;
//! use paxakos::prototyping::{DirectCommunicator, PrototypingNode, RetryIndefinitely};
//! use paxakos::{LogEntry, Node, NodeBuilder, NodeHandle, NodeInfo, RoundNum, State};
//! # use uuid::Uuid;
//!
//! type ChatCommunicator = DirectCommunicator<ChatState, u64, u32>;
//!
//! let node_a = PrototypingNode::new();
//! let node_b = PrototypingNode::new();
//! let node_c = PrototypingNode::new();
//!
//! let nodes = vec![node_a, node_b, node_c];
//!
//! let communicator = ChatCommunicator::new();
//!
//! let node_a = spawn_node(node_a, nodes.clone(), communicator.clone());
//! let node_b = spawn_node(node_b, nodes.clone(), communicator.clone());
//! let node_c = spawn_node(node_c, nodes, communicator);
//!
//! futures::executor::block_on(async move {
//!     let _ = node_a
//!         .append(msg("Alice", "Oh, hey guys"), always_retry())
//!         .await;
//! });
//!
//! // Because Bob and Charlie reply without synchronization, either may reply
//! // first. However, all participants will observe the same person replying
//! // first.
//! let b = std::thread::spawn(|| {
//! futures::executor::block_on(async move {
//!         let _ = node_b
//!             .append(msg("Bob", "Hi Alice, long time no see!"), always_retry())
//!             .await;
//!     });
//! });
//! let c = std::thread::spawn(|| {
//!     futures::executor::block_on(async move {
//!         let _ = node_c
//!             .append(msg("Charlie", "Hi Alice, how are you?"), always_retry())
//!             .await;
//!     });
//! });
//!
//! // Let's wait for the appends to go through.
//! b.join().unwrap();
//! c.join().unwrap();
//!
//! // It is guaranteed that all messages above have been appended to the shared log
//! // at this point. However, one node may not know about it yet and the others may
//! // not have gotten a chance to apply it to their state. Let's give them a chance
//! // to do that.
//! std::thread::sleep(std::time::Duration::from_millis(10));
//!
//! // Graceful shutdown is possible (see `Node::shut_down`) but is too involved for
//! // this example.
//! std::process::exit(0);
//!
//! fn spawn_node(
//!     node_info: PrototypingNode,
//!     all_nodes: Vec<PrototypingNode>,
//!     communicator: ChatCommunicator,
//! ) -> NodeHandle<ChatState, u64, u32> {
//!     let (send, recv) = futures::channel::oneshot::channel();
//!
//!     std::thread::spawn(move || {
//!         let (handler, mut node) = futures::executor::block_on(
//!             paxakos::node_builder()
//!                 .for_node(node_info.id())
//!                 .working_ephemerally()
//!                 .communicating_via(communicator.clone())
//!                 .with_initial_state(ChatState::new(node_info.id(), all_nodes))
//!                 .spawn_in(()),
//!         )
//!         .unwrap();
//!
//!         send.send(node.handle()).unwrap();
//!
//!         communicator.register(node_info.id(), handler);
//!
//!         futures::executor::block_on(futures::future::poll_fn(|cx| {
//!             let _ = node.poll_events(cx);
//!
//!             std::task::Poll::<()>::Pending
//!         }));
//!     });
//!
//!     futures::executor::block_on(recv).unwrap()
//! }
//!
//! fn msg(sender: &str, message: &str) -> ChatMessage {
//!     ChatMessage {
//!         id: Uuid::new_v4(),
//!         sender: sender.to_string(),
//!         message: message.to_string(),
//!     }
//! }
//!
//! fn always_retry<R: RoundNum>() -> AppendArgs<R> {
//!     AppendArgs {
//!         retry_policy: Box::new(RetryIndefinitely::without_pausing()),
//!         ..Default::default()
//!     }
//! }
//!
//! #[derive(Clone, Debug)]
//! pub struct ChatMessage {
//!     id: Uuid,
//!     sender: String,
//!     message: String,
//! }
//!
//! #[async_trait(?Send)]
//! impl LogEntry for ChatMessage {
//!     type Id = Uuid;
//! #   type Reader = std::io::Cursor<Vec<u8>>;
//! #
//! #   async fn from_reader<R: AsyncRead + Unpin>(_read: R) -> Result<Self, paxakos::error::BoxError> {
//! #       unimplemented!()
//! #   }
//! #
//! #   fn size(&self) -> usize {
//! #       unimplemented!()
//! #   }
//! #
//! #   fn to_reader(&self) -> Self::Reader {
//! #       unimplemented!()
//! #   }
//!
//!     fn id(&self) -> Self::Id {
//!         self.id
//!     }
//! }
//!
//! #[derive(Clone, Debug)]
//! pub struct ChatState {
//!     node_id: usize,
//!     nodes: Vec<PrototypingNode>,
//! }
//!
//! impl ChatState {
//!     pub fn new(node_id: usize, nodes: Vec<PrototypingNode>) -> Self {
//!         Self { node_id, nodes }
//!     }
//! }
//!
//! #[async_trait(?Send)]
//! impl State for ChatState {
//!     type Context = ();
//!
//! #   type Reader = std::io::Cursor<Vec<u8>>;
//! #
//!     type LogEntry = ChatMessage;
//!     type Outcome = ();
//!     type Event = ();
//!
//!     type Node = PrototypingNode;
//!
//! #   async fn from_reader<R: AsyncRead + Unpin>(_read: R) -> Result<Self, paxakos::error::BoxError> {
//! #       unimplemented!()
//! #   }
//! #
//! #   fn size(&self) -> usize {
//! #       unimplemented!()
//! #   }
//! #
//! #   fn to_reader(&self) -> Self::Reader {
//! #       unimplemented!()
//! #   }
//! #
//!     fn apply(
//!         &mut self,
//!         log_entry: &Self::LogEntry,
//!         _context: &mut (),
//!     ) -> (Self::Outcome, Self::Event) {
//!         let own_node_id = format!("{:X}", self.node_id + 10);
//!
//!         println!(
//!             "[{}] -- {}: {}",
//!             own_node_id, log_entry.sender, log_entry.message
//!         );
//!
//!         ((), ())
//!     }
//!
//!     fn cluster_at(&self, _round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
//!         self.nodes.clone()
//!     }
//! }
//! ```
//!
//! ## Motivation
//!
//! Rust is a great language with which to implement efficient and truly
//! reliable, fault-tolerant services. And while there already are several Rust
//! implementations of consensus algorithms, they are either rudimentary or
//! incomplete or their API was not approachable enough for this author.
//!
//! ### Priorities
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
//!    - **Consistency** means that all nodes in the Paxakos network agree about
//!      the contents of their shared log. While nodes may temporarily fall
//!      behind, i.e. their log may be shorter than other nodes', they're logs
//!      must otherwise be equivalent.
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
//! ## Features
//!
//! Paxakos is a Multi-Paxos implementation. It supports membership changes,
//! concurrency, as well as taking and installing snapshots.
//!
//! ### Membership Changes
//!
//! The `State` trait exposes the [`cluster_at`][State::cluster_at] method. By
//! implementing it, _arbitrary_ membership changes may be made. No restrictions
//! are imposed and it is up to users and implementors to make sure that any
//! transition is safe.
//!
//! ### Concurrency
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
//! #### Interjection
//!
//! Consensus based clusters typically elect a single leader and who drive all
//! progress. This is highly efficient, as each leader election incurs overhead.
//! That notwithstanding, Paxakos has made the unusual design decision to allow
//! multiple leaders at the same time, but for different rounds.
//!
//! This design allows a follower node to "interject" an entry at the end of the
//! concurrency window. This part of the design hasn't been fleshed out yet, but
//! it could allow nodes to conveniently queue operations without introducing
//! additional communication protocols besides Paxakos.
//!
//! ### Snapshots
//!
//! A node may take a snapshot of its current state. The result is a combination
//! of the application specific `State` as well as pakakos specifc information.
//! These snapshots may be used for graceful shutdown and restart, to bootstrap
//! nodes which have joined the cluster or to catch up nodes that have fallen
//! behind.
//!
//! ## Protocol
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
//! 2. Promise or Rejection
//!
//!    When a node receives a prepare request, it checks that it hasn't accepted
//!    a previous such request with a coordination number that's equal or higher
//!    than the given one. If it hasn't, it sends back a _promise_ not to accept
//!    any more _proposals_ with a coordination number that's less the given
//!    one. Otherwise it sends back a rejection.
//!
//!    1. Promise `(Vec<(RoundNum, CoordNum, LogEntry)>)`
//!
//!       The promise is a set of triples, each consisting of a round number, a
//!       coordination number and a log entry. It can be thought to mean "I
//!       acknowledge your bid to become leader and give you my vote. However,
//!       in return you must respect these previous commitments I've made."
//!
//!    2. Rejection `(CoordNum, Option<LogEntry>)`
//!
//!       A rejection is sent with the highest observed coordination number so
//!       far. For the special case that the round has already converged and the
//!       node still has it available, it will send it along as well.
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
//!    number, the one with the highest coordination number wins. (Triples with
//!    the same round and coordination number will have the same log entry as
//!    well.)
//!
//! 4. Acceptance or Rejection
//!
//!    When a node receives a proposal, it will check whether it has made any
//!    conflicting promises. If it hasn't it will simply reply that it has
//!    accepted the entry. Otherwise it will respond with a rejection.
//!
//!    1. Acceptance `()`
//!    2. Rejection `(CoordNum, Option<LogEntry>)`
//!
//!       See 2.2.
//!
//! 5. Commit `(RoundNum, LogEntry::Id)` / CommitById `(RoundNum, LogEntry)`
//!
//!    After having received a quorum of acceptances, the round has converged on
//!    the proposed entry. The leader node will commit the entry locally and
//!    inform other nodes as well. Nodes who sent an acceptance will only be
//!    sent the log entry's id, others will receive the full entry.
//!
//! ## Status
//!
//! The core algorithm of Paxakos is reasonably well-tested. However,
//! "ancillary" features such as snapshots and passive mode are not well tested
//! and presumably contain bugs. Also, APIs and serialized representations will
//! likely change.
//!
//! **Use at your own risk.**
//!
//! ### Nightly Features
//!
//! Paxakos currently relies on several nightly features. None of them are
//! crucial, but there hasn't been any need become compatible with stable Rust.
//!
//! # Future Direction
//!
//! Paxakos will probably remain dormant for the near future. This is because
//! it needs to see some use and experimentation. Likeley exceptions are the
//! following changes.
//!
//! - Improving Compile Times
//!
//!   Any suggestions as to how comile times may be reduced are welcome. Compile
//!   times of _dependent_ projects are the primary concern.
//!
//! - Adding comments and documentation
//!
//! - Adding convenience methods and decorations.
//!
//! - Support for _Master Leases_ (see section 5.2 of [Paxos Made
//!   Live][paxos-made-live]).
//!
//! [paxos-made-live]: https://doi.org/10.1145/1281100.1281103
//!
//! [Communicator]: crate::communicator::Communicator
//! [LogEntry]: crate::log::LogEntry
//! [NodeInfo]: crate::node::NodeInfo
//! [State]: crate::state::State
//! [State::cluster_at]: crate::state::State::cluster_at

//
// Nightly features
#![feature(drain_filter)]
#![feature(map_first_last)]
#![feature(never_type)]
#![feature(range_is_empty)]
#![feature(step_trait)]
#![feature(with_options)]
//
// Lint configuration
#![warn(rust_2018_idioms)]
#![warn(clippy::wildcard_imports)]

pub mod append;
pub mod cluster;
pub mod communicator;
pub mod deco;
pub mod error;
pub mod event;
mod log;
pub mod node;
#[cfg(feature = "prototyping")]
pub mod prototyping;
pub mod state;
#[cfg(feature = "tracer")]
pub mod tracer;
mod util;

use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

// TODO move these three into the communicator module
#[doc(inline)]
pub use error::{AcceptError, CommitError, PrepareError};
#[doc(inline)]
pub use event::{Event, ShutdownEvent};
#[doc(inline)]
pub use log::LogEntry;
#[doc(inline)]
pub use node::builder as node_builder;
#[doc(inline)]
pub use node::{Commit, Node, NodeBuilder, NodeHandle, NodeInfo, NodeKernel, Shutdown};
#[doc(inline)]
pub use node::{NodeStatus, RequestHandler};
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
    + std::iter::Step
    + Into<u128>
    + TryFrom<u128>
    + TryFrom<usize>
    + TryInto<usize>
    + serde::de::DeserializeOwned
    + serde::Serialize
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
        + std::iter::Step
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
pub trait Identifier: 'static + Copy + Debug + Eq + Hash + Ord + Send + Sync + Unpin {}

impl<T: 'static + Copy + Debug + Eq + Hash + Ord + Send + Sync + Unpin> Identifier for T {}

/// A promise not to accept certain proposals anymore.
///
/// Please refer to the [description of the protocol](crate#protocol).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Promise<R, C, E>(Vec<(R, C, Arc<E>)>);

impl<R, C, E> Promise<R, C, E>
where
    R: RoundNum,
    C: CoordNum,
    E: LogEntry,
{
    pub(crate) fn empty() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn log_entry_for(&self, round_num: R) -> Option<Arc<E>> {
        self.0
            .iter()
            .find(|(r, _, _)| *r == round_num)
            .map(|(_, _, e)| Arc::clone(e))
    }

    pub(crate) fn merge_with(self, other: Self) -> Self {
        let a = self.0;
        let b = other.0;

        let mut max = Vec::with_capacity(a.len() + b.len());

        let mut a = a.into_iter();
        let mut b = b.into_iter();

        let mut a_next = a.next();
        let mut b_next = b.next();

        loop {
            match (a_next, b_next) {
                (Some((ra, ca, ea)), Some((rb, cb, eb))) => match ra.cmp(&rb) {
                    std::cmp::Ordering::Equal => {
                        if ca > cb {
                            max.push((ra, ca, ea));
                        } else {
                            max.push((rb, cb, eb));
                        }

                        a_next = a.next();
                        b_next = b.next();
                    }
                    std::cmp::Ordering::Less => {
                        max.push((ra, ca, ea));
                        a_next = a.next();
                        b_next = Some((rb, cb, eb));
                    }
                    std::cmp::Ordering::Greater => {
                        max.push((rb, cb, eb));
                        b_next = b.next();
                        a_next = Some((ra, ca, ea));
                    }
                },
                (Some((ra, ca, ea)), None) => {
                    max.push((ra, ca, ea));
                    max.extend(a);
                    return Promise(max);
                }
                (None, Some((rb, cb, eb))) => {
                    max.push((rb, cb, eb));
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

/// Rejection of a prepare request or a proposal.
///
/// Please refer to the [description of the protocol](crate#protocol).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Rejection<C, E> {
    Conflict {
        coord_num: C,
    },
    Converged {
        coord_num: C,
        log_entry: Option<Arc<E>>,
    },
}

impl<C, E> Rejection<C, E>
where
    C: CoordNum,
    E: LogEntry,
{
    pub(crate) fn coord_num(&self) -> C {
        match *self {
            Rejection::Conflict { coord_num } => coord_num,
            Rejection::Converged { coord_num, .. } => coord_num,
        }
    }
}

impl<R, C, E> IntoIterator for Promise<R, C, E>
where
    R: RoundNum,
    C: CoordNum,
    E: LogEntry,
{
    type Item = (R, C, Arc<E>);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
