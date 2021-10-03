use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future;
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use paxakos::append::AppendArgs;
use paxakos::autofill;
use paxakos::autofill::AutofillBuilderExt;
use paxakos::heartbeats::HeartbeatsBuilderExt;
use paxakos::invocation::Invocation;
use paxakos::leadership::ensure::EnsureLeadershipBuilderExt;
use paxakos::leadership::track::LeadershipAwareNode;
use paxakos::leadership::track::TrackLeadershipBuilderExt;
use paxakos::node::Participation;
use paxakos::node::Snapshot;
use paxakos::prototyping::DirectCommunicatorError;
use paxakos::prototyping::DirectCommunicatorPayload;
use paxakos::prototyping::DirectCommunicators;
use paxakos::prototyping::PrototypingNode;
use paxakos::prototyping::RetryIndefinitely;
use paxakos::retry::DoNotRetry;
use paxakos::LogEntry;
use paxakos::Node;
use paxakos::NodeInfo;
use paxakos::Shutdown;
use paxakos::ShutdownEvent;
use paxakos::State;
use rocket::get;
use rocket::patch;
use rocket::post;
use rocket::response::content::Html;
use rocket::routes;
use rocket::tokio::io::AsyncRead;
use uuid::Uuid;

type R = u32;
type C = u32;
type PlaygroundCommunicators = DirectCommunicators<PlaygroundInvocation>;
type PlaygroundNodeHandle = paxakos::NodeHandle<PlaygroundInvocation>;
type Listener = mpsc::Sender<Cursor<Vec<u8>>>;

struct Reaper {
    clusters: Arc<Mutex<HashMap<String, Cluster>>>,
    interval: Duration,
}

impl Reaper {
    fn spawn(self) {
        let Reaper { clusters, interval } = self;

        let mut deadish = HashSet::new();
        let mut dead = HashSet::new();

        std::thread::spawn(move || {
            futures::executor::block_on(async move {
                loop {
                    futures_timer::Delay::new(interval).await;

                    let mut clusters = clusters.lock().await;

                    for (id, cluster) in clusters.iter() {
                        let mut listeners = cluster.listeners.lock().await;

                        listeners.retain(|l| !l.is_closed());

                        if deadish.remove(id) && listeners.is_empty() {
                            dead.insert(id.clone());
                        } else if listeners.is_empty() {
                            deadish.insert(id.clone());
                        }
                    }

                    for id in dead.drain() {
                        clusters.remove(&id);
                    }
                }
            })
        });
    }
}

struct Clusters(Arc<Mutex<HashMap<String, Cluster>>>);

struct Cluster {
    args: PostClusterArguments,
    nodes: Vec<PrototypingNode>,
    communicators: PlaygroundCommunicators,
    listeners: Arc<Mutex<Vec<Listener>>>,
    node_terminators: HashMap<usize, oneshot::Sender<Termination>>,
    node_handles: HashMap<usize, PlaygroundNodeHandle>,
    snapshots: HashMap<String, PlaygroundSnapshot>,
}

enum Termination {
    Crash,
    ShutDown,
}

struct PlaygroundSnapshot {
    node_id: usize,
    resumable: bool,
    snapshot: paxakos::node::Snapshot<PlaygroundState, R, C>,
}

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let rt = rocket::tokio::runtime::Runtime::new().unwrap();

    let clusters = Clusters(Default::default());

    Reaper {
        clusters: Arc::clone(&clusters.0),
        interval: Duration::from_millis(500),
    }
    .spawn();

    rt.block_on(
        rocket::build()
            .manage(clusters)
            .mount(
                "/",
                routes![
                    get_events,
                    get_index_html,
                    patch_cluster,
                    post_cluster,
                    post_cluster_start,
                    post_node_append,
                    post_node_crash,
                    post_node_recover,
                    post_node_resume,
                    post_node_shut_down,
                    post_node_take_snapshot,
                ],
            )
            .launch(),
    )
    .unwrap();
}

#[get("/", format = "html")]
fn get_index_html() -> Html<String> {
    Html(std::fs::read_to_string("./examples/playground/src/index.html").unwrap())
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PostClusterArguments {
    node_count: usize,
    concurrency: usize,
    packet_loss: f32,
    e2e_delay_mean: f32,
    e2e_delay_std_dev: f32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PostClusterResult {
    id: String,
    nodes: Vec<ClusterNode>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterNode {
    id: String,
    name: String,
}

#[post("/", data = "<args>")]
async fn post_cluster(
    clusters: &rocket::State<Clusters>,
    args: rocket::serde::json::Json<PostClusterArguments>,
) -> rocket::serde::json::Json<PostClusterResult> {
    let id = Uuid::new_v4();

    let nodes = (0..args.node_count)
        .map(PrototypingNode::with_id)
        .collect::<Vec<_>>();

    let communicators = DirectCommunicators::with_characteristics(
        args.packet_loss,
        rand_distr::Normal::new(args.e2e_delay_mean, args.e2e_delay_std_dev).unwrap(),
    );

    let cluster = Cluster {
        args: args.into_inner(),
        nodes: nodes.clone(),
        communicators,
        listeners: Arc::new(Mutex::new(Vec::new())),
        node_terminators: HashMap::new(),
        node_handles: HashMap::new(),
        snapshots: HashMap::new(),
    };

    let communicators_clone = cluster.communicators.clone();
    let listeners_clone = Arc::clone(&cluster.listeners);
    rocket::tokio::spawn(async move {
        let mut events = communicators_clone.events();
        drop(communicators_clone);

        while let Some(e) = events.next().await {
            emit_event(
                &*listeners_clone,
                Packet {
                    from: e.sender.to_string(),
                    to: e.receiver.to_string(),
                    e2e_delay: e.e2e_delay.as_millis(),
                    dropped: e.dropped,
                    response: match e.payload {
                        DirectCommunicatorPayload::Promise(true)
                        | DirectCommunicatorPayload::Accept(true)
                        | DirectCommunicatorPayload::Committed(true) => Some("positive"),
                        DirectCommunicatorPayload::Promise(false)
                        | DirectCommunicatorPayload::Accept(false)
                        | DirectCommunicatorPayload::Committed(false) => Some("negative"),
                        _ => None,
                    },
                },
            )
            .await;
        }
    });

    let mut clusters = clusters.inner().0.lock().await;
    clusters.insert(id.to_string(), cluster);

    rocket::serde::json::Json(PostClusterResult {
        id: id.to_string(),
        nodes: nodes
            .into_iter()
            .enumerate()
            .map(|(i, n)| ClusterNode {
                id: n.id().to_string(),
                name: (i + 1).to_string(),
            })
            .collect(),
    })
}

#[post("/<cluster_id>/start")]
async fn post_cluster_start(clusters_state: &rocket::State<Clusters>, cluster_id: String) {
    let mut clusters = clusters_state.0.lock().await;
    let cluster = clusters.get_mut(&cluster_id).unwrap();

    for n in cluster.nodes.clone() {
        let (terminator_send, terminator_recv) = oneshot::channel();

        let node_handle = spawn_node(
            Arc::clone(&clusters_state.0),
            cluster_id.clone(),
            n,
            cluster.communicators.clone(),
            Arc::clone(&cluster.listeners),
            terminator_recv,
            Snapshot::initial(PlaygroundState::new(
                cluster.nodes.clone(),
                cluster.args.concurrency,
            )),
            false,
        )
        .await;

        cluster.node_terminators.insert(n.id(), terminator_send);
        cluster.node_handles.insert(n.id(), node_handle);
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_node(
    clusters: Arc<Mutex<HashMap<String, Cluster>>>,
    cluster_id: String,
    n: PrototypingNode,
    communicators: PlaygroundCommunicators,
    listeners: Arc<Mutex<Vec<Listener>>>,
    mut terminator: oneshot::Receiver<Termination>,
    snapshot: Snapshot<PlaygroundState, R, C>,
    passive: bool,
) -> PlaygroundNodeHandle {
    let node_id = n.id();

    let rt_gaps = rocket::tokio::runtime::Handle::current();
    let listeners_gaps = Arc::clone(&listeners);
    let rt_heartbeat = rt_gaps.clone();
    let listeners_heartbeat = Arc::clone(&listeners);
    let rt_ensure = rt_gaps.clone();
    let listeners_ensure = Arc::clone(&listeners);

    let (node_handle_send, node_handle_recv) = oneshot::channel();

    std::thread::spawn(move || {
        let (handler, mut node) = futures::executor::block_on(
            PlaygroundInvocation::node_builder()
                .for_node(n.id())
                .communicating_via(communicators.create_communicator_for(n.id()))
                .with_snapshot_and_passivity(snapshot, passive)
                .track_leadership()
                .fill_gaps(AutofillConfig::new(rt_gaps, listeners_gaps))
                .send_heartbeats(HeartbeatConfig::new(rt_heartbeat, listeners_heartbeat))
                .ensure_leadership(|c| {
                    c.with_entry(move || {
                        let listeners = listeners_ensure.clone();

                        rt_ensure.spawn(async move {
                            emit_event(
                                &*listeners,
                                Action {
                                    node: node_id.to_string(),
                                    action: "ensure-leadership",
                                },
                            )
                            .await
                        });

                        PlaygroundLogEntry::EnsureLeadership(Uuid::new_v4())
                    })
                    .every(Duration::from_secs(10))
                })
                .spawn(),
        )
        .unwrap();

        communicators.register(n.id(), handler);

        let _ = node_handle_send.send(node.handle());

        let mut leader = None;

        futures::executor::block_on(async move {
            let mut active = false;

            loop {
                let next_event = match future::select(node.next_event(), terminator).await {
                    future::Either::Left((e, t)) => {
                        terminator = t;

                        e
                    }
                    future::Either::Right((t, _)) => {
                        let event = match t {
                            Ok(Termination::Crash) | Err(_) => "crash",
                            Ok(Termination::ShutDown) => {
                                let mut shut_down = node.shut_down();

                                let snapshot = loop {
                                    match future::poll_fn(|cx| shut_down.poll_shutdown(cx)).await {
                                        ShutdownEvent::Regular(_) => {}
                                        ShutdownEvent::Final { snapshot, .. } => {
                                            break snapshot.unwrap();
                                        }
                                    }
                                };

                                let mut clusters = clusters.lock().await;
                                let cluster = clusters.get_mut(&cluster_id).unwrap();

                                let id = Uuid::new_v4();
                                let round = snapshot.round();

                                cluster.snapshots.insert(
                                    id.to_string(),
                                    PlaygroundSnapshot {
                                        node_id,
                                        resumable: true,
                                        snapshot,
                                    },
                                );

                                emit_event(
                                    &*cluster.listeners,
                                    SnapshotEvent {
                                        id: id.to_string(),
                                        taken_by: node_id.to_string(),
                                        round,
                                        resumable_by: Some(node_id.to_string()),
                                    },
                                )
                                .await;

                                "shut-down"
                            }
                        };

                        emit_event(
                            &*listeners,
                            TerminationEvent {
                                node: node_id.to_string(),
                                event,
                            },
                        )
                        .await;

                        return;
                    }
                };

                match next_event {
                    paxakos::Event::Commit {
                        round, log_entry, ..
                    } => {
                        emit_event(
                            &*listeners,
                            Event {
                                node: n.id().to_string(),
                                round,
                                event: "commit",
                                action: log_entry.as_str(),
                            },
                        )
                        .await;
                    }
                    paxakos::Event::Apply {
                        round, log_entry, ..
                    } => {
                        emit_event(
                            &*listeners,
                            Event {
                                node: n.id().to_string(),
                                round,
                                event: "apply",
                                action: log_entry.as_str(),
                            },
                        )
                        .await;
                    }

                    _ => {}
                }

                if !active && node.participation() == Participation::Active {
                    active = true;

                    emit_event(
                        &*listeners,
                        Activate {
                            node: n.id().to_string(),
                            event: "activate",
                        },
                    )
                    .await;
                }

                let new_leader = node.strict_leadership().get(0).map(|l| l.leader);

                if new_leader != leader {
                    leader = new_leader;

                    emit_event(
                        &*listeners,
                        NewLeader {
                            node: n.id().to_string(),
                            leader: leader.map(|l| l.to_string()),
                        },
                    )
                    .await;
                }
            }
        });

        tracing::info!("Node {} shut down.", node_id);
    });

    node_handle_recv.await.unwrap()
}

struct HeartbeatConfig<N, I> {
    runtime: rocket::tokio::runtime::Handle,
    listeners: Arc<Mutex<Vec<Listener>>>,
    node_id: usize,

    _p: std::marker::PhantomData<(N, I)>,
}

impl<N, I> HeartbeatConfig<N, I> {
    fn new(runtime: rocket::tokio::runtime::Handle, listeners: Arc<Mutex<Vec<Listener>>>) -> Self {
        Self {
            runtime,
            listeners,
            node_id: usize::MAX,

            _p: std::marker::PhantomData,
        }
    }
}

impl<N, I> paxakos::heartbeats::Config for HeartbeatConfig<N, I>
where
    N: LeadershipAwareNode<I, Invocation = PlaygroundInvocation>,
{
    type Node = N;
    type Applicable = PlaygroundLogEntry;

    fn init(&mut self, node: &Self::Node) {
        self.node_id = node.id();
    }
    fn leader_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(3))
    }

    fn interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(5))
    }

    fn new_heartbeat(&self) -> Self::Applicable {
        let node_id = self.node_id;
        let listeners = Arc::clone(&self.listeners);

        self.runtime.spawn(async move {
            emit_event(
                &*listeners,
                Action {
                    node: node_id.to_string(),
                    action: "heartbeat",
                },
            )
            .await
        });

        PlaygroundLogEntry::Heartbeat(Uuid::new_v4())
    }
}

struct AutofillConfig<N> {
    runtime: rocket::tokio::runtime::Handle,
    listeners: Arc<Mutex<Vec<Listener>>>,
    node_id: usize,

    _p: std::marker::PhantomData<N>,
}

impl<N> AutofillConfig<N> {
    fn new(runtime: rocket::tokio::runtime::Handle, listeners: Arc<Mutex<Vec<Listener>>>) -> Self {
        Self {
            runtime,
            listeners,
            node_id: usize::MAX,

            _p: std::marker::PhantomData,
        }
    }
}

impl<N: Node<Invocation = PlaygroundInvocation>> autofill::Config for AutofillConfig<N> {
    type Node = N;
    type Applicable = PlaygroundLogEntry;
    type RetryPolicy = DoNotRetry<PlaygroundInvocation>;

    fn init(&mut self, node: &Self::Node) {
        self.node_id = node.id();
    }

    fn batch_size(&self) -> usize {
        10
    }

    fn delay(&self) -> Duration {
        Duration::from_millis(1000)
    }

    fn new_filler(&self) -> Self::Applicable {
        let node_id = self.node_id;
        let listeners = Arc::clone(&self.listeners);

        self.runtime.spawn(async move {
            emit_event(
                &*listeners,
                Action {
                    node: node_id.to_string(),
                    action: "fill",
                },
            )
            .await
        });

        PlaygroundLogEntry::Fill(Uuid::new_v4())
    }

    fn retry_policy(&self) -> Self::RetryPolicy {
        DoNotRetry::new()
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
enum ClusterPatch {
    #[serde(rename_all = "camelCase")]
    Link {
        from: String,
        to: String,
        #[serde(default)]
        packet_loss: Option<f32>,
        #[serde(default)]
        e2e_delay_mean: Option<f32>,
        #[serde(default)]
        e2e_delay_std_dev: Option<f32>,
    },
}

#[patch("/<cluster_id>", data = "<patches>")]
async fn patch_cluster(
    clusters: &rocket::State<Clusters>,
    cluster_id: String,
    patches: rocket::serde::json::Json<Vec<ClusterPatch>>,
) {
    let mut clusters = clusters.0.lock().await;
    let cluster = clusters.get_mut(&cluster_id).unwrap();

    for patch in patches.into_inner() {
        match patch {
            ClusterPatch::Link {
                from,
                to,
                packet_loss,
                e2e_delay_mean,
                e2e_delay_std_dev,
            } => {
                let from = from.parse().unwrap();
                let to = to.parse().unwrap();

                if let Some(packet_loss) = packet_loss {
                    cluster
                        .communicators
                        .set_packet_loss(from, to, packet_loss)
                        .await;

                    let listeners = Arc::clone(&cluster.listeners);

                    rocket::tokio::spawn(async move {
                        let _ = emit_event(
                            &*listeners,
                            PacketLossPatched {
                                from: from.to_string(),
                                to: to.to_string(),
                                packet_loss,
                            },
                        )
                        .await;
                    });
                }

                if let (Some(e2e_delay_mean), Some(e2e_delay_std_dev)) =
                    (e2e_delay_mean, e2e_delay_std_dev)
                {
                    cluster
                        .communicators
                        .set_delay(
                            from,
                            to,
                            rand_distr::Normal::new(e2e_delay_mean, e2e_delay_std_dev).unwrap(),
                        )
                        .await;

                    let listeners = Arc::clone(&cluster.listeners);

                    rocket::tokio::spawn(async move {
                        let _ = emit_event(
                            &*listeners,
                            E2eDelayPatched {
                                from: from.to_string(),
                                to: to.to_string(),
                                e2e_delay_mean,
                                e2e_delay_std_dev,
                            },
                        )
                        .await;
                    });
                }
            }
        }
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Packet {
    from: String,
    to: String,
    e2e_delay: u128,
    dropped: bool,
    response: Option<&'static str>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Action {
    node: String,
    action: &'static str,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct AppendResult {
    append: String,
    outcome: &'static str,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotEvent {
    id: String,
    taken_by: String,
    round: R,
    resumable_by: Option<String>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TerminationEvent {
    node: String,
    event: &'static str,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PacketLossPatched {
    from: String,
    to: String,
    packet_loss: f32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct E2eDelayPatched {
    from: String,
    to: String,
    e2e_delay_mean: f32,
    e2e_delay_std_dev: f32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Event {
    node: String,
    round: R,
    event: &'static str,
    action: &'static str,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Activate {
    node: String,
    event: &'static str,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct NewLeader {
    node: String,
    leader: Option<String>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PostNodeAppendArguments {
    amount: usize,
    from_round: Option<u32>,
    until_round: Option<u32>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PostNodeAppendResult {
    id: String,
}

#[post("/<cluster_id>/node/<node_id>/appends", data = "<args>")]
async fn post_node_append(
    clusters: &rocket::State<Clusters>,
    cluster_id: String,
    node_id: usize,
    args: rocket::serde::json::Json<PostNodeAppendArguments>,
) -> rocket::serde::json::Json<PostNodeAppendResult> {
    let clusters = clusters.inner().0.lock().await;
    let cluster = clusters.get(&cluster_id).unwrap();
    let node_handle = cluster.node_handles.get(&node_id).unwrap().clone();

    let id = Uuid::new_v4();

    let mut queue = futures::stream::FuturesUnordered::new();

    for _ in 0..args.amount {
        queue.push(node_handle.append(
            PlaygroundLogEntry::Regular(Uuid::new_v4()),
            AppendArgs {
                retry_policy: RetryIndefinitely::pausing_up_to(Duration::from_secs(4)),
                round: args.from_round.unwrap_or(0)..=args.until_round.unwrap_or(u32::MAX),
                importance: Default::default(),
            },
        ));
    }

    let listeners = Arc::clone(&cluster.listeners);

    rocket::tokio::spawn(async move {
        while let Some(result) = queue.next().await {
            let outcome = result.map(|_| "success").unwrap_or("failure");

            let _ = emit_event(
                &*listeners,
                AppendResult {
                    append: id.to_string(),
                    outcome,
                },
            )
            .await;
        }
    });

    rocket::serde::json::Json(PostNodeAppendResult { id: id.to_string() })
}

#[post("/<cluster_id>/node/<node_id>/crash")]
async fn post_node_crash(clusters: &rocket::State<Clusters>, cluster_id: String, node_id: usize) {
    let mut clusters = clusters.inner().0.lock().await;
    let cluster = clusters.get_mut(&cluster_id).unwrap();

    if let Some(terminator) = cluster.node_terminators.remove(&node_id) {
        let _ = terminator.send(Termination::Crash);
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PostNodeRecoverArguments {
    snapshot: String,
}

#[post("/<cluster_id>/node/<node_id>/recover", data = "<args>")]
async fn post_node_recover(
    clusters_state: &rocket::State<Clusters>,
    cluster_id: String,
    node_id: usize,
    args: rocket::serde::json::Json<PostNodeRecoverArguments>,
) {
    let mut clusters = clusters_state.inner().0.lock().await;
    let cluster = clusters.get_mut(&cluster_id).unwrap();

    cluster.snapshots.values_mut().for_each(|s| {
        if s.node_id == node_id {
            s.resumable = false;
        }
    });

    let snapshot = cluster
        .snapshots
        .get(&args.snapshot)
        .unwrap()
        .snapshot
        .clone();

    if cluster.nodes.iter().any(|n| n.id() == node_id)
        && !cluster.node_terminators.contains_key(&node_id)
    {
        let (terminator_send, terminator_recv) = oneshot::channel();

        let node_handle = spawn_node(
            Arc::clone(&clusters_state.0),
            cluster_id,
            *cluster.nodes.iter().find(|n| n.id() == node_id).unwrap(),
            cluster.communicators.clone(),
            Arc::clone(&cluster.listeners),
            terminator_recv,
            snapshot,
            true,
        )
        .await;

        cluster.node_terminators.insert(node_id, terminator_send);
        cluster.node_handles.insert(node_id, node_handle);
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PostNodeResumeArguments {
    snapshot: String,
}

#[post("/<cluster_id>/node/<node_id>/resume", data = "<args>")]
async fn post_node_resume(
    clusters_state: &rocket::State<Clusters>,
    cluster_id: String,
    node_id: usize,
    args: rocket::serde::json::Json<PostNodeResumeArguments>,
) {
    let mut clusters = clusters_state.inner().0.lock().await;
    let cluster = clusters.get_mut(&cluster_id).unwrap();

    let snapshot = cluster.snapshots.get_mut(&args.snapshot).unwrap();

    if snapshot.node_id == node_id && snapshot.resumable {
        snapshot.resumable = false;
    }

    let snapshot = snapshot.snapshot.clone();

    if cluster.nodes.iter().any(|n| n.id() == node_id)
        && !cluster.node_terminators.contains_key(&node_id)
    {
        let (terminator_send, terminator_recv) = oneshot::channel();

        let node_handle = spawn_node(
            Arc::clone(&clusters_state.0),
            cluster_id,
            *cluster.nodes.iter().find(|n| n.id() == node_id).unwrap(),
            cluster.communicators.clone(),
            Arc::clone(&cluster.listeners),
            terminator_recv,
            snapshot,
            false,
        )
        .await;

        cluster.node_terminators.insert(node_id, terminator_send);
        cluster.node_handles.insert(node_id, node_handle);
    }
}

#[post("/<cluster_id>/node/<node_id>/shut-down")]
async fn post_node_shut_down(
    clusters: &rocket::State<Clusters>,
    cluster_id: String,
    node_id: usize,
) {
    let mut clusters = clusters.inner().0.lock().await;
    let cluster = clusters.get_mut(&cluster_id).unwrap();

    if let Some(terminator) = cluster.node_terminators.remove(&node_id) {
        let _ = terminator.send(Termination::ShutDown);
    }
}

#[post("/<cluster_id>/node/<node_id>/take-snapshot")]
async fn post_node_take_snapshot(
    clusters: &rocket::State<Clusters>,
    cluster_id: String,
    node_id: usize,
) {
    let mut clusters = clusters.inner().0.lock().await;
    let cluster = clusters.get_mut(&cluster_id).unwrap();
    let node_handle = cluster.node_handles.get(&node_id).unwrap().clone();

    let id = Uuid::new_v4();
    let snapshot = node_handle.prepare_snapshot().await.unwrap();
    let round = snapshot.round();

    cluster.snapshots.insert(
        id.to_string(),
        PlaygroundSnapshot {
            node_id,
            resumable: false,
            snapshot,
        },
    );

    emit_event(
        &*cluster.listeners,
        SnapshotEvent {
            id: id.to_string(),
            taken_by: node_id.to_string(),
            round,
            resumable_by: None,
        },
    )
    .await;
}

#[get("/<cluster_id>/events" /*, format = "text/event-stream"*/)]
async fn get_events(
    clusters: &rocket::State<Clusters>,
    cluster_id: String,
) -> rocket::response::content::Custom<
    rocket::response::stream::ReaderStream<rocket::response::stream::One<impl AsyncRead>>,
> {
    let mut clusters = clusters.inner().0.lock().await;

    let cluster = clusters.get_mut(&cluster_id).unwrap();

    let (mut s, r) = futures::channel::mpsc::channel(16);

    let _ = s
        .send(Cursor::new("data: {}\n\n".to_string().into_bytes()))
        .await;

    let mut listeners = cluster.listeners.lock().await;
    listeners.push(s.clone());

    let ct = rocket::http::ContentType::with_params("text", "event-stream", ("charset", "utf-8"));
    rocket::response::content::Custom(
        ct,
        rocket::response::stream::ReaderStream::one(tokio_util::io::StreamReader::new(
            r.map(Result::<_, std::io::Error>::Ok),
        )),
    )
}

async fn emit_event<E: serde::Serialize>(listeners: &Mutex<Vec<Listener>>, event: E) {
    let event = serde_json::to_string(&event).unwrap();
    let event = format!("data: {}\n\n", event);
    let event = event.into_bytes();

    let mut listeners = listeners.lock().await;

    for listener in listeners.iter_mut().filter(|l| !l.is_closed()) {
        let _ = listener.send(Cursor::new(event.clone())).await;
    }
}

pub struct PlaygroundInvocation;

impl Invocation for PlaygroundInvocation {
    type RoundNum = u32;
    type CoordNum = u32;

    type State = PlaygroundState;

    type Yea = Duration;
    type Nay = ();
    type Abstain = Duration;

    type CommunicationError = DirectCommunicatorError;
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub enum PlaygroundLogEntry {
    Regular(Uuid),
    Heartbeat(Uuid),
    EnsureLeadership(Uuid),
    Fill(Uuid),
}

impl PlaygroundLogEntry {
    fn as_str(&self) -> &'static str {
        match self {
            PlaygroundLogEntry::Heartbeat(_) => "heartbeat",
            PlaygroundLogEntry::EnsureLeadership(_) => "ensure-leadership",
            PlaygroundLogEntry::Regular(_) => "regular",
            PlaygroundLogEntry::Fill(_) => "fill",
        }
    }
}

impl LogEntry for PlaygroundLogEntry {
    type Id = Uuid;

    fn id(&self) -> Self::Id {
        match *self {
            PlaygroundLogEntry::Regular(id)
            | PlaygroundLogEntry::Heartbeat(id)
            | PlaygroundLogEntry::EnsureLeadership(id)
            | PlaygroundLogEntry::Fill(id) => id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PlaygroundState {
    applied: HashSet<Uuid>,
    concurrency: std::num::NonZeroUsize,
    nodes: Vec<PrototypingNode>,
}

impl PlaygroundState {
    pub fn new(nodes: Vec<PrototypingNode>, concurrency: usize) -> Self {
        Self {
            applied: HashSet::new(),
            concurrency: std::num::NonZeroUsize::new(concurrency).unwrap(),
            nodes,
        }
    }
}

impl State for PlaygroundState {
    type Context = ();

    type LogEntry = PlaygroundLogEntry;
    type Outcome = usize;
    type Event = usize;

    type Node = PrototypingNode;

    fn apply(
        &mut self,
        log_entry: &Self::LogEntry,
        _context: &mut (),
    ) -> (Self::Outcome, Self::Event) {
        self.applied.insert(log_entry.id());

        (self.applied.len(), self.applied.len())
    }

    fn concurrency(&self) -> std::num::NonZeroUsize {
        self.concurrency
    }

    fn cluster_at(&self, _round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
        self.nodes.clone()
    }
}
