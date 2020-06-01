#![cfg(all(feature = "prototyping", feature = "tracer"))]

mod calc_app;

use futures::stream::StreamExt;
use uuid::Uuid;

use paxakos::prototyping::{DirectCommunicator, PrototypingNode};
use paxakos::{Node, NodeBuilder, NodeInfo, Shutdown, ShutdownEvent};

use calc_app::{CalcOp, CalcState};

#[test]
fn clean_shutdown() {
    let node_info = PrototypingNode::new();
    let (_, node) = futures::executor::block_on(
        paxakos::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(DirectCommunicator::<CalcState, u64, u32>::new())
            .with_initial_state(CalcState::new(vec![node_info], 1))
            .spawn_in(()),
    )
    .unwrap();

    futures::executor::block_on(node.append(CalcOp::Add(42.0, Uuid::new_v4()), Default::default()))
        .unwrap();

    let mut shutdown = node.shut_down();

    let mut states = futures::stream::poll_fn(|cx| shutdown.poll_shutdown(cx).map(Some))
        .map(|e| match e {
            ShutdownEvent::Regular(_) => futures::stream::empty().left_stream(),
            ShutdownEvent::Last { state, .. } => {
                futures::stream::once(futures::future::ready(state)).right_stream()
            }
        })
        .flatten();
    let final_state = states.next();

    let state = futures::executor::block_on(final_state).unwrap();
    assert_eq!(Some(42.0), state.map(|s| s.value()));
}
