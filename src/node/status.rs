// TODO most states should expose a "since: Instant"
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeStatus {
    /// The node has become passive.
    ///
    /// This commonly happens due to I/O errors while reading from/writing to
    /// the log file.
    Stalled,

    /// The node has no state and needs to install a snapshot.
    Disoriented,

    /// The node appears to be up to date.
    Following,

    /// The node has gaps in its log and its state may not be up to date.
    Lagging,

    /// The node committed the latest entry and no other node has been observed
    /// in an attempt to commit an entry for a later round.
    // FIXME this status is currently never taken on
    Leading,
}
