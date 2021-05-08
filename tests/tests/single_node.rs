#![feature(never_type)]

mod calc_app;

use futures::future::FutureExt;
use futures::stream::StreamExt;
use uuid::Uuid;

use paxakos::append::AppendArgs;
use paxakos::prototyping::{DirectCommunicator, DirectCommunicators};
use paxakos::prototyping::{PrototypingNode, RetryIndefinitely};
use paxakos::{Event, Node, NodeBuilder, NodeInfo};

use calc_app::{CalcOp, CalcState};

#[test]
fn single_append() {
    let node = setup_node();

    let result = futures::executor::block_on(
        node.append(CalcOp::Add(42.0, Uuid::new_v4()), Default::default()),
    );
    let result = result.unwrap();
    let result = futures::executor::block_on(result).unwrap();
    assert!((42.0 - result).abs() < f64::EPSILON);

    let state = futures::executor::block_on(node.read_stale()).unwrap();
    assert!((42.0 - state.value()).abs() < f64::EPSILON);
}

#[test]
fn multiple_serial_append() {
    let node = setup_node();

    let result = futures::executor::block_on(
        node.append(CalcOp::Add(42.0, Uuid::new_v4()), Default::default()),
    );
    let result = result.unwrap();
    let result = futures::executor::block_on(result).unwrap();
    assert!((42.0 - result).abs() < f64::EPSILON);

    let result = futures::executor::block_on(
        node.append(CalcOp::Mul(3.0, Uuid::new_v4()), Default::default()),
    );
    let result = result.unwrap();
    let result = futures::executor::block_on(result).unwrap();
    assert!((126.0 - result).abs() < f64::EPSILON);

    let state = futures::executor::block_on(node.read_stale()).unwrap();
    assert!((126.0 - state.value()).abs() < f64::EPSILON);
}

#[test]
fn multiple_concurrent_appends() {
    let mut node = setup_node();

    let mut ops: Vec<_> = (0..20)
        .map(|i| CalcOp::Add((1 << i).into(), Uuid::new_v4()))
        .collect();
    let target = f64::from((1 << 20) - 1);

    use rand::seq::SliceRandom;
    ops.shuffle(&mut rand::thread_rng());

    let futures: futures::stream::FuturesUnordered<_> = ops
        .into_iter()
        .map(|op| {
            node.append(
                op,
                AppendArgs {
                    retry_policy: Box::new(RetryIndefinitely::without_pausing()),
                    ..Default::default()
                },
            )
            .map(|_| ())
            .left_future()
        })
        .collect();

    let events = futures::stream::poll_fn(|cx| node.poll_events(cx).map(Some));

    futures.push(
        events
            .take_while(|e| {
                futures::future::ready(
                    !matches!( e, Event::Apply { result,.. } if result.0 >= target ),
                )
            })
            .for_each(|_| futures::future::ready(()))
            .right_future(),
    );

    futures::executor::block_on(futures.for_each_concurrent(None, |_| futures::future::ready(())));

    let state = futures::executor::block_on(node.read_stale()).unwrap();
    assert!((target - state.value()).abs() < f64::EPSILON);
}

fn setup_node() -> paxakos::NodeKernel<CalcState, DirectCommunicator<CalcState, u64, u32, !>> {
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcState, u64, u32, !>::new();

    let (_, node) = futures::executor::block_on(
        paxakos::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .with_initial_state(CalcState::new(vec![node_info], 1))
            .spawn_in(()),
    )
    .unwrap();

    node
}
