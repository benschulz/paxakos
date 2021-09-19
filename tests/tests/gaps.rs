#![feature(never_type)]

mod calc_app;

use std::time::Duration;

use futures::stream::StreamExt;
use paxakos::autofill;
use paxakos::invocation::Invocation;
use uuid::Uuid;

use paxakos::communicator::Communicator;
use paxakos::event::Gap;
use paxakos::invocation::CoordNumOf;
use paxakos::invocation::LogEntryOf;
use paxakos::invocation::RoundNumOf;
use paxakos::node::EventFor;
use paxakos::prototyping::DirectCommunicator;
use paxakos::prototyping::DirectCommunicators;
use paxakos::prototyping::PrototypingNode;
use paxakos::Event;
use paxakos::Node;
use paxakos::NodeInfo;
use paxakos::NodeKernel;
use paxakos::RequestHandler;

use calc_app::CalcInvocation;
use calc_app::CalcOp;
use calc_app::CalcState;
use calc_app::PlusZero;

type CalcNode = NodeKernel<CalcInvocation, DirectCommunicator<CalcInvocation>>;

#[test]
fn emit_gaps_event_when_it_first_appears() {
    let (req_handler, mut node) = setup_node();

    commit(&req_handler, 5, 1, CalcOp::Add(123f64, Uuid::new_v4()));
    let gaps_event = next_gaps_event(&mut node);

    assert_eq!(ranges_of(&gaps_event), vec![1..5]);
}

#[test]
fn shrunk_gap_maintains_age() {
    let (req_handler, mut node) = setup_node();

    commit(&req_handler, 5, 1, CalcOp::Add(123f64, Uuid::new_v4()));
    let first_event = next_gaps_event(&mut node);
    commit(&req_handler, 3, 1, CalcOp::Add(321f64, Uuid::new_v4()));
    let second_event = next_gaps_event(&mut node);

    assert_eq!(ranges_of(&second_event), vec![1..3, 4..5]);
    assert_eq!(
        gaps_of(&first_event)[0].since,
        gaps_of(&second_event)[0].since
    );
    assert_eq!(
        gaps_of(&first_event)[0].since,
        gaps_of(&second_event)[1].since
    );
}

#[test]
fn later_gap_is_younger() {
    let (req_handler, mut node) = setup_node();

    commit(&req_handler, 2, 1, CalcOp::Add(321f64, Uuid::new_v4()));
    let first_event = next_gaps_event(&mut node);
    commit(&req_handler, 4, 1, CalcOp::Add(123f64, Uuid::new_v4()));
    let second_event = next_gaps_event(&mut node);

    assert_eq!(ranges_of(&first_event), vec![1..2]);
    assert_eq!(ranges_of(&second_event), vec![1..2, 3..4]);
    assert!(gaps_of(&first_event)[0].since == gaps_of(&second_event)[0].since);
    assert!(gaps_of(&second_event)[1].since > gaps_of(&second_event)[0].since);
}

#[test]
fn auto_fill_gaps() {
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcInvocation>::new();

    use paxakos::autofill::AutofillBuilderExt;

    let (req_handler, mut node) = futures::executor::block_on(
        CalcInvocation::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .with_initial_state(CalcState::new(vec![node_info], 10))
            .fill_gaps(autofill::StaticConfig::<_, PlusZero>::new(
                10,
                Duration::from_millis(10),
            ))
            .spawn_in(()),
    )
    .unwrap();

    let target = 123f64;

    // become leader
    futures::executor::block_on(node.append(CalcOp::Mul(0.0, Uuid::new_v4()), Default::default()))
        .unwrap();

    commit(&req_handler, 5, 0, CalcOp::Add(target, Uuid::new_v4()));

    let events = futures::stream::poll_fn(|cx| node.poll_events(cx).map(Some));

    futures::executor::block_on(
        events
            .take_while(|e| {
                futures::future::ready(
                    !matches!(e, Event::Apply { result,.. } if result.0 >= target),
                )
            })
            .for_each(|_| futures::future::ready(())),
    );
}

fn setup_node() -> (RequestHandler<CalcInvocation>, CalcNode) {
    let node_info = PrototypingNode::new();
    let communicators = DirectCommunicators::<CalcInvocation>::new();

    let (req_handler, node) = futures::executor::block_on(
        CalcInvocation::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(communicators.create_communicator_for(node_info.id()))
            .with_initial_state(CalcState::new(vec![node_info], 1))
            .spawn_in(()),
    )
    .unwrap();

    (req_handler, node)
}

fn commit<I: Invocation>(
    req_handler: &RequestHandler<I>,
    round_num: RoundNumOf<I>,
    coord_num: CoordNumOf<I>,
    log_entry: LogEntryOf<I>,
) {
    let _ = futures::executor::block_on(req_handler.handle_commit(round_num, coord_num, log_entry));
}

fn next_gaps_event<N, C>(node: &mut N) -> EventFor<N>
where
    N: Node<Invocation = CalcInvocation, Communicator = C>,
    C: Communicator,
{
    futures::executor::block_on(
        futures::stream::poll_fn(move |cx| node.poll_events(cx).map(Some))
            .filter(|e| futures::future::ready(matches!(e, Event::Gaps { .. })))
            .into_future(),
    )
    .0
    .unwrap()
}

fn gaps_of<I: Invocation>(e: &Event<I>) -> Vec<Gap<RoundNumOf<I>>> {
    match e {
        Event::Gaps(gs) => gs.clone(),
        _ => panic!("Expected Event::Gaps{{..}}, got {:?}.", e),
    }
}

fn ranges_of<I: Invocation>(e: &Event<I>) -> Vec<std::ops::Range<RoundNumOf<I>>> {
    match e {
        Event::Gaps(gs) => gs.iter().map(|g| g.rounds.clone()).collect(),
        _ => panic!("Expected Event::Gaps{{..}}, got {:?}.", e),
    }
}
