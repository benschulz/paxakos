use crate::error::SpawnError;
use crate::Node;

/// A decoration around a `Node`.
///
/// Use [`decorated_with`][crate::NodeBuilder::decorated_with] to wrap a
/// decoration around a node.
pub trait Decoration
where
    Self: Node,
{
    type Arguments: 'static;
    type Decorated: Node;

    /// Wraps this decoration around the given node, using the given arguments.
    fn wrap(decorated: Self::Decorated, arguments: Self::Arguments) -> Result<Self, SpawnError>;

    /// Returns a reference to the decorated node.
    fn peek_into(decorated: &Self) -> &Self::Decorated;

    fn unwrap(decorated: Self) -> Self::Decorated;
}
