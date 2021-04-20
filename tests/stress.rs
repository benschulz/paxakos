#![cfg(all(feature = "prototyping", feature = "tracer"))]

mod calc_app;
mod tracer;

use futures::future::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;
use uuid::Uuid;

use paxakos::append::AppendArgs;
use paxakos::deco::AutoFillGapsBuilderExt;
use paxakos::prototyping::{DirectCommunicator, PrototypingNode, RetryIndefinitely};
use paxakos::{Node, NodeBuilder, NodeInfo, Shutdown};

use calc_app::{CalcOp, CalcState};
use tracer::StabilityChecker;

type CalcCommunicator = DirectCommunicator<CalcState, u64, u32>;

#[test]
fn stress_test() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let mut consistency_checker = StabilityChecker::new();
    let communicator =
        DirectCommunicator::with_characteristics(0.1, rand_distr::Normal::new(8.0, 3.0).unwrap());

    let node_count = 5;
    let ops_per_node = 70;
    let target =
        f64::from(node_count) * f64::from(ops_per_node) * (f64::from(ops_per_node) + 1.0) / 2.0;

    let nodes = (0..node_count)
        .map(|_| PrototypingNode::new())
        .collect::<Vec<_>>();

    let mut join_handles = Vec::new();

    for n in nodes.clone() {
        join_handles.push(spawn_node(
            n,
            nodes.clone(),
            communicator.clone(),
            (1..=ops_per_node).map(|i| CalcOp::Add(f64::from(i), Uuid::new_v4())),
            target,
            &mut consistency_checker,
        ));
    }

    consistency_checker.spawn();

    join_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .reduce(|a, b| {
            assert_eq!(a, b);

            a
        })
        .expect("Expected checksum");
}

fn spawn_node(
    node_info: PrototypingNode,
    all_nodes: Vec<PrototypingNode>,
    mut communicator: CalcCommunicator,
    ops: impl std::iter::IntoIterator<Item = CalcOp> + Send + 'static,
    target: f64,
    consistency_checker: &mut StabilityChecker<usize, u64, u32, Uuid>,
) -> std::thread::JoinHandle<blake3::Hash> {
    let tracer = consistency_checker.tracer(node_info.id());

    std::thread::spawn(move || {
        let (handler, node) = futures::executor::block_on(
            paxakos::node_builder()
                .for_node(node_info.id())
                .working_ephemerally()
                .communicating_via(communicator.clone())
                .with_initial_state(CalcState::new(all_nodes, 5))
                .traced_by(tracer)
                .fill_gaps(|c| {
                    c.with_entry(|| CalcOp::Sub(0.0, Uuid::new_v4()))
                        .after(std::time::Duration::from_millis(50))
                        .retry_every(std::time::Duration::from_millis(20))
                })
                .spawn_in(()),
        )
        .unwrap();

        communicator.register(node_info.id(), handler);

        let mut queued_ops = ops.into_iter().collect::<Vec<_>>();
        queued_ops.shuffle(&mut rand::thread_rng());

        let mut running_ops = FuturesUnordered::new();
        let mut hash_at_target = None;

        let mut deadline = None;

        let mut node = Some(node);

        let deadline_duration = std::time::Duration::from_millis(750);

        let node = futures::executor::block_on(futures::future::poll_fn(|cx| {
            while let std::task::Poll::Ready(e) = node.as_mut().unwrap().poll_events(cx) {
                match (&mut deadline, e) {
                    (deadline @ None, paxakos::Event::Apply { result, .. }) => {
                        if target - result.0 < f64::EPSILON {
                            tracing::info!(
                                "Node {} reached target, sets deadline.",
                                node_info.id()
                            );

                            hash_at_target = Some(result.1);
                            *deadline = Some(futures_timer::Delay::new(deadline_duration));

                            // make sure that everyone gets a chance to finish
                            communicator.faiure_rate(0.0);
                            queued_ops.push(CalcOp::Div(1.0, Uuid::new_v4()));
                        }
                    }
                    (deadline @ Some(_), _) => {
                        tracing::info!("Node {} extends deadline.", node_info.id());
                        // extend the deadline
                        *deadline = Some(futures_timer::Delay::new(deadline_duration));
                    }
                    _ => {}
                }
            }

            if let Some(deadline) = deadline.as_mut() {
                if let std::task::Poll::Ready(_) = deadline.poll_unpin(cx) {
                    return std::task::Poll::Ready(node.take());
                }
            }

            while !queued_ops.is_empty() && running_ops.len() < 10 {
                let op = queued_ops.pop().unwrap();

                running_ops.push(
                    node.as_mut()
                        .unwrap()
                        .append(
                            op,
                            AppendArgs {
                                retry_policy: Box::new(RetryIndefinitely::pausing_up_to(
                                    std::time::Duration::from_millis(200),
                                )),
                                ..Default::default()
                            },
                        )
                        .map(move |r| r.err().map(|_| op))
                        .boxed_local(),
                );

                tracing::debug!("Remaining ops in queue: {}", queued_ops.len());
            }

            while let std::task::Poll::Ready(Some(res)) = running_ops.poll_next_unpin(cx) {
                if let Some(op) = res {
                    queued_ops.push(op);
                } else if queued_ops.is_empty() && running_ops.is_empty() {
                    tracing::info!("Node {:?} is out of work.", node_info.id());
                }
            }

            std::task::Poll::Pending
        }));

        let node = node.unwrap();
        let mut shutdown = node.shut_down();

        let snapshot = futures::executor::block_on(
            futures::stream::poll_fn(|cx| shutdown.poll_shutdown(cx).map(Some))
                .filter(|e| futures::future::ready(matches!(e, paxakos::ShutdownEvent::Last {..})))
                .map(|e| match e {
                    paxakos::ShutdownEvent::Last { snapshot, .. } => snapshot,
                    _ => unreachable!(),
                })
                .next(),
        )
        .unwrap()
        .unwrap();

        tracing::info!("Node {} is shut down.", node_info.id());

        assert!(target - snapshot.state().value() < f64::EPSILON);

        hash_at_target.unwrap()
    })
}
