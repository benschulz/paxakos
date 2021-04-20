mod fill_gaps;

use crate::error::SpawnError;
use crate::Node;

pub use fill_gaps::{FillGaps, FillGapsBuilderExt};

pub trait Decoration
where
    Self: Node,
{
    type Arguments: 'static;
    type Decorated: Node;

    fn wrap(decorated: Self::Decorated, arguments: Self::Arguments) -> Result<Self, SpawnError>;

    fn peek_into(decorated: &Self) -> &Self::Decorated;

    fn unwrap(decorated: Self) -> Self::Decorated;
}
