mod calc_app;

use paxakos::invocation::Invocation;
use paxakos::node::Snapshot;
use paxakos::prototyping::DirectCommunicators;
use paxakos::prototyping::PrototypingNode;
use paxakos::Node;
use paxakos::NodeInfo;
use paxakos::NodeStatus;

use calc_app::CalcState;

use crate::calc_app::CalcInvocation;

#[test]
fn node_without_state_starts_as_disoriented() {
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcInvocation>::new();

    let (_, node) = futures::executor::block_on(
        CalcInvocation::node_builder()
            .for_node(node_info.id())
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .without_state()
            .spawn(),
    )
    .unwrap();

    assert_eq!(node.status(), NodeStatus::Disoriented);
}

#[test]
fn node_given_immediately_ready_snapshot_starts_as_lagging() {
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcInvocation>::new();

    let (_, node) = futures::executor::block_on(
        CalcInvocation::node_builder()
            .for_node(node_info.id())
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .joining_with(Snapshot::initial_with(CalcState::new(vec![node_info], 1)))
            .spawn(),
    )
    .unwrap();

    assert_eq!(node.status(), NodeStatus::Lagging);
}
