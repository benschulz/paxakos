use crate::Identifier;

/// Describes a node in a Paxakos cluster.
///
/// Beyond an identifier, a `NodeInfo` implementation provides all the
/// information necessary to communicate with that node, typically a hostname
/// and a (TCP) port.
// TODO not wild about the name…
pub trait NodeInfo: 'static + Clone + std::fmt::Debug + Send + Sync {
    type Id: Identifier;

    /// A unique identifier for this node.
    fn id(&self) -> Self::Id;
}

/// To toy around with a single node cluster.
#[cfg(feature = "prototyping")]
impl NodeInfo for () {
    type Id = ();

    fn id(&self) -> Self::Id {}
}
