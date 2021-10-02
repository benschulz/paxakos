//! Defines the [`Decoration`] trait.

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
    /// Arguments this decoration requires.
    ///
    /// See [`NodeBuilder::decorated_with`][crate::NodeBuilder::decorated_with].
    type Arguments: 'static;

    /// Type of the decorated node.
    type Decorated: Node;

    /// Wraps this decoration around the given node, using the given arguments.
    fn wrap(decorated: Self::Decorated, arguments: Self::Arguments) -> Result<Self, SpawnError>;

    /// Returns a reference to the decorated node.
    fn peek_into(decorated: &Self) -> &Self::Decorated;

    /// Unwraps this decoration.
    fn unwrap(decorated: Self) -> Self::Decorated;
}
