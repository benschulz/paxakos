mod calc_app;

use paxakos::node::Snapshot;
use paxakos::prototyping::{DirectCommunicators, PrototypingNode};
use paxakos::{Node, NodeBuilder, NodeInfo, NodeStatus};

use calc_app::CalcState;

#[test]
fn node_without_state_starts_as_lost() {
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcState, u64, u32>::new();

    let (_, node) = futures::executor::block_on(
        paxakos::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .without::<CalcState>()
            .spawn_in(()),
    )
    .unwrap();

    assert_eq!(node.status(), NodeStatus::Disoriented);
}

#[test]
fn node_given_immediately_ready_snapshot_starts_as_lagging() {
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcState, u64, u32>::new();

    let (_, node) = futures::executor::block_on(
        paxakos::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .joining_with(Snapshot::initial(CalcState::new(vec![node_info], 1)))
            .spawn_in(()),
    )
    .unwrap();

    assert_eq!(node.status(), NodeStatus::Lagging);
}
