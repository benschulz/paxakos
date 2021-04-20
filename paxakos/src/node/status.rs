/// A node's status, usually `Leading` or `Following`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeStatus {
    /// The node has no state and needs to install a snapshot.
    Disoriented,

    /// The node appears to be up to date.
    Following,

    /// The node has state but it has *definitely* fallen behind.
    ///
    /// A node is considered lagging when it has received a request for a round
    /// outside its concurrency window.
    Lagging,

    /// The node committed the latest entry and no other node has been observed
    /// in an attempt to commit an entry for a later round.
    Leading,
}
