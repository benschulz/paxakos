#![cfg(all(feature = "prototyping", feature = "tracer"))]

mod calc_app;

use futures::stream::StreamExt;
use uuid::Uuid;

use paxakos::communicator::{Communicator, RoundNumOf};
use paxakos::event::Gap;
use paxakos::prototyping::{DirectCommunicator, PrototypingNode};
use paxakos::state::LogEntryOf;
use paxakos::{CoordNum, Event, Node, NodeBuilder, NodeInfo};
use paxakos::{NodeKernel, RequestHandler, RoundNum, State};

use calc_app::{CalcOp, CalcState};

type CalcCommunicator = DirectCommunicator<CalcState, u64, u32>;
type CalcNode = NodeKernel<CalcState, CalcCommunicator>;

#[test]
fn emit_gaps_event_when_it_first_appears() {
    let (req_handler, mut node) = setup_node();

    commit(&req_handler, 5, CalcOp::Add(123f64, Uuid::new_v4()));
    let gaps_event = next_gaps_event(&mut node);

    assert_eq!(ranges_of(&gaps_event), vec![1..5]);
}

#[test]
fn shrunk_gap_maintains_age() {
    let (req_handler, mut node) = setup_node();

    commit(&req_handler, 5, CalcOp::Add(123f64, Uuid::new_v4()));
    let first_event = next_gaps_event(&mut node);
    commit(&req_handler, 3, CalcOp::Add(321f64, Uuid::new_v4()));
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

    commit(&req_handler, 2, CalcOp::Add(321f64, Uuid::new_v4()));
    let first_event = next_gaps_event(&mut node);
    commit(&req_handler, 4, CalcOp::Add(123f64, Uuid::new_v4()));
    let second_event = next_gaps_event(&mut node);

    assert_eq!(ranges_of(&first_event), vec![1..2]);
    assert_eq!(ranges_of(&second_event), vec![1..2, 3..4]);
    assert!(gaps_of(&first_event)[0].since == gaps_of(&second_event)[0].since);
    assert!(gaps_of(&second_event)[1].since > gaps_of(&second_event)[0].since);
}

#[test]
fn auto_fill_gaps() {
    let node_info = PrototypingNode::new();

    use paxakos::deco::AutoFillGapsBuilderExt;

    let (req_handler, mut node) = futures::executor::block_on(
        paxakos::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(DirectCommunicator::<CalcState, u64, u32>::new())
            .with_initial_state(CalcState::new(vec![node_info], 10))
            .fill_gaps(|b| {
                b.with_entry(|| CalcOp::Add(0f64, Uuid::new_v4()))
                    .in_batches_of(10)
                    .after(std::time::Duration::from_millis(10))
            })
            .spawn_in(()),
    )
    .unwrap();

    let target = 123f64;

    // become leader
    futures::executor::block_on(node.append(CalcOp::Mul(0.0, Uuid::new_v4()), Default::default()))
        .unwrap();

    commit(&req_handler, 5, CalcOp::Add(target, Uuid::new_v4()));

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

fn setup_node() -> (RequestHandler<CalcState, u64, u32>, CalcNode) {
    let node_info = PrototypingNode::new();
    let communicator = DirectCommunicator::new();

    let (req_handler, node) = futures::executor::block_on(
        paxakos::node_builder()
            .for_node(node_info.id())
            .working_ephemerally()
            .communicating_via(communicator.clone())
            .with_initial_state(CalcState::new(vec![node_info], 1))
            .spawn_in(()),
    )
    .unwrap();

    (req_handler, node)
}

fn commit<S: State, R: RoundNum, C: CoordNum>(
    req_handler: &RequestHandler<S, R, C>,
    round: R,
    op: LogEntryOf<S>,
) {
    let _ = futures::executor::block_on(req_handler.handle_commit(round, op));
}

fn next_gaps_event<N, S, C>(node: &mut N) -> Event<S, RoundNumOf<C>>
where
    N: Node<State = S, Communicator = C>,
    S: State<LogEntry = <C as Communicator>::LogEntry, Node = <C as Communicator>::Node>,
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

fn gaps_of<S: State, R: RoundNum>(e: &Event<S, R>) -> Vec<Gap<R>> {
    match e {
        Event::Gaps(gs) => gs.clone(),
        _ => panic!("Expected Event::Gaps{{..}}, got {:?}.", e),
    }
}

fn ranges_of<S: State, R: RoundNum>(e: &Event<S, R>) -> Vec<std::ops::Range<R>> {
    match e {
        Event::Gaps(gs) => gs.iter().map(|g| g.rounds.clone()).collect(),
        _ => panic!("Expected Event::Gaps{{..}}, got {:?}.", e),
    }
}
