#![cfg(all(feature = "prototyping", feature = "tracer"))]

mod calc_app;

use paxakos::node::Snapshot;
use paxakos::prototyping::{DirectCommunicator, PrototypingNode};
use paxakos::{Node, NodeBuilder, NodeInfo, NodeStatus};

use calc_app::CalcState;

#[test]
fn node_without_state_starts_as_lost() {
    let (_, node) = futures::executor::block_on(
        paxakos::node_builder()
            .for_node(PrototypingNode::new().id())
            .working_ephemerally()
            .communicating_via(DirectCommunicator::<CalcState, u64, u32>::new())
            .without::<CalcState>()
            .spawn_in(()),
    )
    .unwrap();

    assert_eq!(node.status(), NodeStatus::Disoriented);
}

#[test]
fn node_given_immediately_ready_snapshot_starts_as_lagging() {
    let node_info = PrototypingNode::new();

    let (_, node) = futures::executor::block_on(
        paxakos::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(DirectCommunicator::<CalcState, u64, u32>::new())
            .joining_with(Snapshot::initial(CalcState::new(vec![node_info], 1)))
            .spawn_in(()),
    )
    .unwrap();

    assert_eq!(node.status(), NodeStatus::Lagging);
}
