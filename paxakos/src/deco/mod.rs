mod ensure_leadership;
mod fill_gaps;
mod send_heartbeats;
mod track_leadership;

use crate::error::SpawnError;
use crate::Node;

pub use ensure_leadership::{EnsureLeadership, EnsureLeadershipBuilderExt};
pub use fill_gaps::{FillGaps, FillGapsBuilderExt};
pub use send_heartbeats::{SendHeartbeats, SendHeartbeatsBuilderExt};
pub use track_leadership::{LeadershipAwareNode, TrackLeadership, TrackLeadershipBuilderExt};

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
