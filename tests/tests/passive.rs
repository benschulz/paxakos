#![feature(assert_matches)]
#![feature(never_type)]

mod calc_app;

use std::assert_matches::assert_matches;
use std::task::Poll;

use futures::channel::oneshot;
use futures::future::Either;
use paxakos::invocation::Invocation;
use rand::Rng;
use uuid::Uuid;

use paxakos::node::Participation;
use paxakos::node::Snapshot;
use paxakos::prototyping::DirectCommunicator;
use paxakos::prototyping::DirectCommunicators;
use paxakos::prototyping::PrototypingNode;
use paxakos::AcceptError;
use paxakos::Node;
use paxakos::NodeInfo;
use paxakos::NodeKernel;
use paxakos::PrepareError;

use calc_app::CalcInvocation;
use calc_app::CalcOp;
use calc_app::CalcState;

type CalcCommunicator = DirectCommunicator<CalcInvocation>;
type CalcNode = NodeKernel<CalcInvocation, CalcCommunicator>;

#[test]
fn worst_case() {
    // We're in a five node cluster.
    let nodes = vec![
        // This is n1, the node we'll experiment on.
        PrototypingNode::new(),
        // These are our four confederates, n2 and n3.
        PrototypingNode::new(),
        PrototypingNode::new(),
    ];

    let node_id = nodes[0].id();
    let concurrency = 10;
    let initial_state = CalcState::new(nodes, concurrency);
    let initial_snapshot = Snapshot::<CalcState, u64, u32>::initial(initial_state);
    let (req_handler, mut node) = futures::executor::block_on(
        CalcInvocation::node_builder()
            .for_node(node_id)
            .working_ephemerally()
            .communicating_via(
                DirectCommunicators::<CalcInvocation>::new().create_communicator_for(node_id),
            )
            .resuming_from(initial_snapshot)
            .spawn_in(()),
    )
    .unwrap();

    let waker = futures::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);

    // Act I
    //
    // Things are going fine at first. Although, unbeknownst to n1, n3 is
    // unreachable.
    for r in 1..=10 {
        let entry_id = uuid::Uuid::new_v4();
        futures::executor::block_on(req_handler.handle_proposal(r, 10, CalcOp::Add(0.0, entry_id)))
            .unwrap();
        while let Poll::Ready(e) = node.poll_events(&mut cx) {
            println!("{:?}", e);
        }

        futures::executor::block_on(req_handler.handle_commit_by_id(r, 10, entry_id)).unwrap();
        while let Poll::Ready(e) = node.poll_events(&mut cx) {
            println!("{:?}", e);
        }
    }

    // We take a snapshot of n1 at round 10.
    let snapshot = futures::executor::block_on(node.prepare_snapshot()).unwrap();

    // Act II
    //
    // Our little nodes that coult, n1 and n2 forge on, settling further rounds…
    let first_unsettled_round = 20;
    let _last_settled_round =
        first_unsettled_round - 1 + rand::thread_rng().gen_range(0..(concurrency as u64));

    // …when, out of nowhere…
    //
    // …n1 crahes and must be restarted from our previous snapshot.
    let (req_handler, mut node) = futures::executor::block_on(
        CalcInvocation::node_builder()
            .for_node(node_id)
            .working_ephemerally()
            .communicating_via(
                DirectCommunicators::<CalcInvocation>::new().create_communicator_for(node_id),
            )
            .recovering_with(snapshot)
            .spawn_in(()),
    )
    .unwrap();

    for r in 11..first_unsettled_round {
        futures::executor::block_on(req_handler.handle_commit(
            r,
            10,
            CalcOp::Add(0.0, uuid::Uuid::new_v4()),
        ))
        .unwrap();
        while let Poll::Ready(e) = node.poll_events(&mut cx) {
            println!("{:?}", e);
        }
    }

    assert_matches!(node.participation(), Participation::Passive);

    // Act III
    //
    // Nodes n3 are reachable once more and participating.
    for r in first_unsettled_round..(first_unsettled_round + concurrency as u64) {
        assert_matches!(
            node.participation(),
            Participation::Passive | Participation::PartiallyActive(_)
        );

        for r2 in 1..(first_unsettled_round + concurrency as u64) {
            assert_matches!(
                futures::executor::block_on(req_handler.handle_prepare(r2, 100,)).unwrap_err(),
                PrepareError::Passive
            );

            while let Poll::Ready(e) = node.poll_events(&mut cx) {
                println!("{:?}", e);
            }

            assert!(matches!(
                futures::executor::block_on(req_handler.handle_proposal(
                    r2,
                    100,
                    CalcOp::Sub(1.0, uuid::Uuid::new_v4()),
                ))
                .unwrap_err(),
                AcceptError::Converged(..) | AcceptError::Passive
            ));

            while let Poll::Ready(e) = node.poll_events(&mut cx) {
                println!("{:?}", e);
            }
        }

        let entry_id = uuid::Uuid::new_v4();
        assert_matches!(
            futures::executor::block_on(req_handler.handle_proposal(
                r,
                10,
                CalcOp::Add(0.0, entry_id)
            ))
            .unwrap_err(),
            AcceptError::Passive
        );
        while let Poll::Ready(e) = node.poll_events(&mut cx) {
            println!("{:?}", e);
        }

        futures::executor::block_on(req_handler.handle_commit(r, 10, CalcOp::Add(0.0, entry_id)))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        while let Poll::Ready(e) = node.poll_events(&mut cx) {
            println!("{:?}", e);
        }

        assert_matches!(
            node.participation(),
            Participation::PartiallyActive(_) | Participation::Active
        );
    }

    assert_matches!(node.participation(), Participation::Active);
}

#[test]
fn become_active() {
    let concurrency = 5;
    let communicators = DirectCommunicators::<CalcInvocation>::new();

    let nodes = vec![
        PrototypingNode::new(),
        PrototypingNode::new(),
        PrototypingNode::new(),
    ];

    let (send1, recv1) = oneshot::channel();
    let (send2, recv2) = oneshot::channel();

    let node_id1 = nodes[0].id();
    let nodes1 = nodes.clone();
    let communicators1 = communicators.clone();
    std::thread::spawn(move || {
        futures::executor::block_on(recv1).unwrap();

        let mut node1 = setup_node(node_id1, true, nodes1, communicators1, concurrency);

        let node_handle = node1.handle();

        std::thread::spawn(move || {
            futures::executor::block_on(async {
                for _i in 0usize..10 {
                    node_handle
                        .append(CalcOp::Add(1.0, Uuid::new_v4()), Default::default())
                        .await
                        .unwrap()
                        .await
                        .unwrap();
                }
            });

            send2.send(()).unwrap();
        });

        loop {
            futures::executor::block_on(node1.next_event());
        }
    });

    let node_id2 = nodes[1].id();
    let nodes2 = nodes.clone();
    let communicators2 = communicators.clone();
    std::thread::spawn(move || {
        let mut node2 = setup_node(node_id2, true, nodes2, communicators2, concurrency);

        loop {
            futures::executor::block_on(node2.next_event());
        }
    });

    let node_id3 = nodes[2].id();
    let nodes3 = nodes;
    let communicators3 = communicators;
    let join_handle = std::thread::spawn(move || {
        let mut node3 = setup_node(node_id3, false, nodes3, communicators3, concurrency);

        assert_eq!(node3.participation(), Participation::Passive);
        send1.send(()).unwrap();

        let mut recv2 = recv2;

        loop {
            recv2 = match futures::executor::block_on(futures::future::select(
                recv2,
                node3.next_event(),
            )) {
                Either::Left(_) => break,
                Either::Right((_, recv2)) => recv2,
            };
        }

        assert_eq!(node3.participation(), Participation::Active);
    });

    join_handle.join().unwrap();
}

fn setup_node(
    node_id: usize,
    active: bool,
    nodes: Vec<PrototypingNode>,
    communicators: DirectCommunicators<CalcInvocation>,
    concurrency: usize,
) -> CalcNode {
    let initial_state = CalcState::new(nodes, concurrency);
    let initial_snapshot = Snapshot::<CalcState, u64, u32>::initial(initial_state);

    let (req_handler, node) = if active {
        futures::executor::block_on(
            CalcInvocation::node_builder()
                .for_node(node_id)
                .working_ephemerally()
                .communicating_via(communicators.create_communicator_for(node_id))
                .resuming_from(initial_snapshot)
                .spawn_in(()),
        )
        .unwrap()
    } else {
        futures::executor::block_on(
            CalcInvocation::node_builder()
                .for_node(node_id)
                .working_ephemerally()
                .communicating_via(communicators.create_communicator_for(node_id))
                .recovering_with(initial_snapshot)
                .spawn_in(()),
        )
        .unwrap()
    };

    communicators.register(node_id, req_handler);

    node
}
