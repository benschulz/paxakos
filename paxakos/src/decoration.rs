//! Defines the [`Decoration`] trait.

use crate::node::InvocationOf;
use crate::node::NodeImpl;

/// A decoration around a `Node`.
///
/// Use [`decorated_with`][decorated_with] to wrap a decoration around a node.
///
/// [decorated_with]: crate::node::builder::ExtensibleNodeBuilder::decorated_with
pub trait Decoration
where
    Self: NodeImpl,
{
    /// Arguments this decoration requires.
    ///
    /// See [`decorated_with`][decorated_with].
    ///
    /// [decorated_with]: crate::node::builder::ExtensibleNodeBuilder::decorated_with
    type Arguments: 'static;

    /// Type of the decorated node.
    type Decorated: NodeImpl<Invocation = InvocationOf<Self>>;

    /// Wraps this decoration around the given node, using the given arguments.
    fn wrap(
        decorated: Self::Decorated,
        arguments: Self::Arguments,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>>;

    /// Returns a reference to the decorated node.
    fn peek_into(decorated: &Self) -> &Self::Decorated;

    /// Unwraps this decoration.
    fn unwrap(decorated: Self) -> Self::Decorated;
}
