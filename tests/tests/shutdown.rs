mod calc_app;

use futures::stream::StreamExt;
use futures::FutureExt;
use paxakos::invocation::Invocation;
use uuid::Uuid;

use paxakos::prototyping::DirectCommunicators;
use paxakos::prototyping::PrototypingNode;
use paxakos::Node;
use paxakos::NodeInfo;
use paxakos::Shutdown;
use paxakos::ShutdownEvent;

use calc_app::CalcInvocation;
use calc_app::CalcOp;
use calc_app::CalcState;

#[test]
fn clean_shutdown() {
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcInvocation>::new();

    let (_, node) = futures::executor::block_on(
        CalcInvocation::node_builder()
            .for_node(node_info.id())
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .with_initial_state(CalcState::new(vec![node_info], 1))
            .spawn(),
    )
    .unwrap();

    futures::executor::block_on(node.into_enter_on_poll(|mut node| async move {
        let append = node.append(CalcOp::Add(42.0, Uuid::new_v4()), ());
        let mut events = node.events();
        futures::future::select(
            append,
            async {
                loop {
                    events.next().await;
                }
            }
            .boxed(),
        )
        .await;

        let mut shutdown = node.shut_down();

        let mut states = futures::stream::poll_fn(|cx| shutdown.poll_shutdown(cx).map(Some))
            .map(|e| match e {
                ShutdownEvent::Regular(_) => futures::stream::empty().left_stream(),
                ShutdownEvent::Final { snapshot, .. } => {
                    futures::stream::once(futures::future::ready(snapshot)).right_stream()
                }
            })
            .flatten();
        let final_snapshot = states.next();

        let final_snapshot = final_snapshot.await.unwrap();
        assert_eq!(Some(42.0), final_snapshot.state().map(|s| s.value()));
    }));
}
