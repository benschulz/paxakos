mod ensure_leadership;
mod fill_gaps;
pub mod send_heartbeats;
mod track_leadership;

use crate::error::SpawnError;
use crate::Node;

pub use ensure_leadership::EnsureLeadership;
pub use ensure_leadership::EnsureLeadershipBuilderExt;
pub use fill_gaps::FillGaps;
pub use fill_gaps::FillGapsBuilderExt;
pub use send_heartbeats::SendHeartbeats;
pub use send_heartbeats::SendHeartbeatsBuilderExt;
pub use track_leadership::LeadershipAwareNode;
pub use track_leadership::TrackLeadership;
pub use track_leadership::TrackLeadershipBuilderExt;

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
