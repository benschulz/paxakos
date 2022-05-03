mod calc_app;

use futures::future::FutureExt;
use futures::stream::StreamExt;
use paxakos::invocation::Invocation;
use uuid::Uuid;

use paxakos::prototyping::DirectCommunicator;
use paxakos::prototyping::DirectCommunicators;
use paxakos::prototyping::PrototypingNode;
use paxakos::prototyping::RetryIndefinitely;
use paxakos::Event;
use paxakos::Node;
use paxakos::NodeInfo;

use calc_app::CalcInvocation;
use calc_app::CalcOp;
use calc_app::CalcState;

#[test]
fn single_append() {
    let node = setup_node();

    let result =
        futures::executor::block_on(node.append_static(CalcOp::Add(42.0, Uuid::new_v4()), ()));
    let result = result.unwrap();
    let result = futures::executor::block_on(result).unwrap();
    assert!((42.0 - result).abs() < f64::EPSILON);

    let v = futures::executor::block_on(node.read_stale(|s| s.value())).unwrap();
    assert!((42.0 - v).abs() < f64::EPSILON);
}

#[test]
fn multiple_serial_append() {
    let node = setup_node();

    let result =
        futures::executor::block_on(node.append_static(CalcOp::Add(42.0, Uuid::new_v4()), ()));
    let result = result.unwrap();
    let result = futures::executor::block_on(result).unwrap();
    assert!((42.0 - result).abs() < f64::EPSILON);

    let result =
        futures::executor::block_on(node.append_static(CalcOp::Mul(3.0, Uuid::new_v4()), ()));
    let result = result.unwrap();
    let result = futures::executor::block_on(result).unwrap();
    assert!((126.0 - result).abs() < f64::EPSILON);

    let v = futures::executor::block_on(node.read_stale(|s| s.value())).unwrap();
    assert!((126.0 - v).abs() < f64::EPSILON);
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
            node.append_static(op, RetryIndefinitely::without_pausing())
                .map(|_| ())
                .left_future()
        })
        .collect();

    let events = futures::stream::poll_fn(|cx| node.poll_events(cx).map(Some));

    futures.push(
        events
            .take_while(|e| {
                futures::future::ready(
                    !matches!( e, Event::Apply { effect,.. } if effect.0 >= target ),
                )
            })
            .for_each(|_| futures::future::ready(()))
            .right_future(),
    );

    futures::executor::block_on(futures.for_each_concurrent(None, |_| futures::future::ready(())));

    let v = futures::executor::block_on(node.read_stale(|s| s.value())).unwrap();
    assert!((target - v).abs() < f64::EPSILON);
}

fn setup_node() -> paxakos::Shell<paxakos::Core<CalcInvocation, DirectCommunicator<CalcInvocation>>>
{
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcInvocation>::new();

    let (_, node) = futures::executor::block_on(
        CalcInvocation::node_builder()
            .for_node(node_info.id())
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .with_initial_state(CalcState::new(vec![node_info], 1))
            .spawn_in(()),
    )
    .unwrap();

    node
}
