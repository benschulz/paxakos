#![feature(never_type)]

mod calc_app;
mod tracer;

use std::task::Poll;

use futures::channel::oneshot;
use futures::future::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;
use uuid::Uuid;

use paxakos::append::AppendArgs;
use paxakos::deco::{EnsureLeadershipBuilderExt, FillGapsBuilderExt};
use paxakos::deco::{SendHeartbeatsBuilderExt, TrackLeadershipBuilderExt};
use paxakos::prototyping::{DirectCommunicator, DirectCommunicators};
use paxakos::prototyping::{PrototypingNode, RetryIndefinitely};
use paxakos::{Node, NodeBuilder, NodeInfo, Shutdown};

use calc_app::{CalcOp, CalcState};
use tracer::StabilityChecker;

type CalcCommunicators = DirectCommunicators<CalcState, u64, u32, !>;
type CalcCommunicator = DirectCommunicator<CalcState, u64, u32, !>;

#[test]
fn stress_test() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let mut consistency_checker = StabilityChecker::new();
    let communicators =
        DirectCommunicators::with_characteristics(0.1, rand_distr::Normal::new(5.0, 3.0).unwrap());

    let node_count = 5;
    let ops_per_node = 70;
    let target =
        f64::from(node_count) * f64::from(ops_per_node) * (f64::from(ops_per_node) + 1.0) / 2.0;

    let nodes = (0..node_count)
        .map(|_| PrototypingNode::new())
        .collect::<Vec<_>>();

    let mut join_handles = Vec::new();

    let mut trrs = Vec::new();
    let mut dss = Vec::new();

    for n in nodes.clone() {
        let (trs, trr) = oneshot::channel();
        let (ds, dr) = oneshot::channel();

        trrs.push(trr);
        dss.push(ds);

        join_handles.push(spawn_node(
            n,
            nodes.clone(),
            communicators.clone(),
            (1..=ops_per_node).map(|i| CalcOp::Add(f64::from(i), Uuid::new_v4())),
            target,
            &mut consistency_checker,
            trs,
            dr,
        ));
    }

    for trr in trrs {
        futures::executor::block_on(trr).unwrap();
    }

    for ds in dss {
        ds.send(()).unwrap();
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

#[allow(clippy::too_many_arguments)]
fn spawn_node(
    node_info: PrototypingNode,
    all_nodes: Vec<PrototypingNode>,
    communicators: CalcCommunicators,
    ops: impl std::iter::IntoIterator<Item = CalcOp> + Send + 'static,
    target: f64,
    consistency_checker: &mut StabilityChecker<usize, CalcCommunicator>,
    target_reached_sender: oneshot::Sender<()>,
    mut done_receiver: oneshot::Receiver<()>,
) -> std::thread::JoinHandle<blake3::Hash> {
    let tracer = consistency_checker.tracer(node_info.id());

    std::thread::spawn(move || {
        let node_id = node_info.id();

        let (handler, node) = futures::executor::block_on(
            paxakos::node_builder()
                .for_node(node_info.id())
                .working_ephemerally()
                .communicating_via(communicators.create_communicator_for(node_info.id()))
                .with_initial_state(CalcState::new(all_nodes, 5))
                .traced_by(tracer)
                .fill_gaps(|c| {
                    c.with_entry(|| CalcOp::Sub(0.0, Uuid::new_v4()))
                        .after(std::time::Duration::from_millis(50))
                        .retry_every(std::time::Duration::from_millis(20))
                })
                .send_heartbeats(|c| {
                    tracing::info!("Node {:?} is sending a heartbeat.", node_id);

                    c.with_entry(|| CalcOp::Mul(1.0, Uuid::new_v4()))
                        .every(std::time::Duration::from_millis(200))
                })
                .track_leadership()
                .ensure_leadership(|c| {
                    c.with_entry(|| CalcOp::Div(1.0, Uuid::new_v4()))
                        .every(std::time::Duration::from_millis(500))
                })
                .spawn_in(()),
        )
        .unwrap();

        communicators.register(node_info.id(), handler);

        let mut queued_ops = ops.into_iter().collect::<Vec<_>>();
        queued_ops.shuffle(&mut rand::thread_rng());

        let mut running_ops = FuturesUnordered::new();
        let mut hash_at_target = None;

        let mut target_reached_sender = Some(target_reached_sender);

        let handle = node.handle();

        std::thread::spawn(move || {
            futures::executor::block_on(async move {
                loop {
                    while !queued_ops.is_empty() && running_ops.len() < 10 {
                        let op = queued_ops.pop().unwrap();

                        running_ops.push(
                            handle
                                .append(
                                    op,
                                    AppendArgs {
                                        retry_policy: Box::new(RetryIndefinitely::pausing_up_to(
                                            std::time::Duration::from_millis(200),
                                        )),
                                        ..Default::default()
                                    },
                                )
                                .map(move |r| r.err().map(|_| op)),
                        );

                        tracing::debug!("Remaining ops in queue: {}", queued_ops.len());
                    }

                    if let Some(res) = running_ops.next().await {
                        if let Some(op) = res {
                            queued_ops.push(op);
                        } else if queued_ops.is_empty() && running_ops.is_empty() {
                            tracing::info!("Node {:?} is out of work.", node_info.id());
                            return;
                        }
                    }
                }
            })
        });

        let mut node = Some(node);

        let node = futures::executor::block_on(futures::future::poll_fn(|cx| {
            if done_receiver.poll_unpin(cx).is_ready() {
                return Poll::Ready(node.take().unwrap());
            }

            while let Poll::Ready(e) = node.as_mut().unwrap().poll_events(cx) {
                if let paxakos::Event::Apply { result, .. } = e {
                    if (target - result.0).abs() < f64::EPSILON {
                        if let Some(target_reached_sender) = target_reached_sender.take() {
                            tracing::info!("Node {} reached target.", node_info.id());

                            hash_at_target = Some(result.1);
                            target_reached_sender.send(()).unwrap();
                        }
                    }
                }
            }

            Poll::Pending
        }));

        let mut shutdown = node.shut_down();

        let snapshot = futures::executor::block_on(
            futures::stream::poll_fn(|cx| shutdown.poll_shutdown(cx).map(Some))
                .filter(|e| {
                    futures::future::ready(matches!(e, paxakos::ShutdownEvent::Last { .. }))
                })
                .map(|e| match e {
                    paxakos::ShutdownEvent::Last { snapshot, .. } => snapshot,
                    _ => unreachable!(),
                })
                .next(),
        )
        .unwrap()
        .unwrap();

        tracing::info!("Node {} is shut down.", node_info.id());

        assert!((target - snapshot.state().value()).abs() < f64::EPSILON);

        hash_at_target.unwrap()
    })
}
