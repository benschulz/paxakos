#![allow(clippy::unused_unit)]

use std::cell::Cell;
use std::collections::HashSet;
use std::convert::Infallible;
use std::rc::Rc;

use futures::channel::oneshot;
use futures::FutureExt;
use futures::StreamExt;
use paxakos::append::AppendArgs;
use paxakos::autofill;
use paxakos::autofill::AutofillBuilderExt;
use paxakos::executor::WasmExecutor;
use paxakos::heartbeats::HeartbeatsBuilderExt;
use paxakos::leadership;
use paxakos::leadership::ensure::EnsureLeadershipBuilderExt;
use paxakos::leadership::track::LeadershipAwareNode;
use paxakos::leadership::track::TrackLeadershipBuilderExt;
use paxakos::node::Participation;
use paxakos::node_builder::Starter;
use paxakos::prototyping::DirectCommunicatorError;
use paxakos::prototyping::DirectCommunicatorPayload;
use paxakos::prototyping::DirectCommunicators;
use paxakos::prototyping::PrototypingNode;
use paxakos::prototyping::RetryIndefinitely;
use paxakos::retry::DoNotRetry;
use paxakos::state;
use paxakos::Invocation;
use paxakos::LogEntry;
use paxakos::Node;
use paxakos::NodeHandle;
use paxakos::NodeInfo;
use paxakos::NodeKit;
use paxakos::Shutdown;
use paxakos::ShutdownEvent;
use paxakos::State;
use uuid::Uuid;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Network {
    callbacks: Rc<Callbacks>,
    communicators: DirectCommunicators<PlaygroundInvocation>,
}

#[wasm_bindgen]
impl Network {
    #[wasm_bindgen(method, js_name = setPacketLoss)]
    pub fn set_packet_loss(&mut self, from: usize, to: usize, packet_loss: f32) -> js_sys::Promise {
        let callbacks = Rc::clone(&self.callbacks);
        let mut communicators = self.communicators.clone();

        wasm_bindgen_futures::future_to_promise(async move {
            communicators.set_packet_loss(from, to, packet_loss).await;

            callbacks.packet_loss_updated(from, to, packet_loss);

            Ok(JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(method, js_name = setE2eDelay)]
    pub fn set_e2e_delay(
        &mut self,
        from: usize,
        to: usize,
        mean: f32,
        std_dev: f32,
    ) -> js_sys::Promise {
        let callbacks = Rc::clone(&self.callbacks);
        let mut communicators = self.communicators.clone();

        wasm_bindgen_futures::future_to_promise(async move {
            communicators
                .set_delay(
                    from,
                    to,
                    rand_distr::Normal::new(mean, std_dev).expect("rand_distr::Normal::new"),
                )
                .await;

            callbacks.e2e_delay_updated(from, to, mean, std_dev);

            Ok(JsValue::UNDEFINED)
        })
    }
}

#[wasm_bindgen]
pub struct Configuration {
    nodes: Vec<PrototypingNode>,
    #[wasm_bindgen(readonly)]
    pub concurrency: usize,
    #[wasm_bindgen(readonly, js_name = heartbeatIntervalMs)]
    pub heartbeat_interval_ms: Option<u32>,
    #[wasm_bindgen(readonly, js_name = leaderHeartbeatIntervalMs)]
    pub leader_heartbeat_interval_ms: Option<u32>,
    #[wasm_bindgen(readonly, js_name = ensureLeadershipIntervalMs)]
    pub ensure_leadership_interval_ms: Option<u32>,
    #[wasm_bindgen(readonly, js_name = autofillDelayMs)]
    pub autofill_delay_ms: Option<u32>,
    #[wasm_bindgen(readonly, js_name = autofillBatchSize)]
    pub autofill_batch_size: usize,
}

#[wasm_bindgen]
impl Configuration {
    #[wasm_bindgen(method, getter)]
    pub fn nodes(&self) -> Vec<usize> {
        self.nodes.iter().map(|n| n.id()).collect()
    }
}

impl<'a> From<&'a Reconfiguration> for Configuration {
    fn from(r: &'a Reconfiguration) -> Self {
        Self {
            nodes: r.new_nodes.clone(),
            concurrency: r.new_concurrency,
            heartbeat_interval_ms: r.new_heartbeat_interval.map(as_u32_millis),
            leader_heartbeat_interval_ms: r.new_leader_heartbeat_interval.map(as_u32_millis),
            ensure_leadership_interval_ms: r.new_ensure_leadership_interval.map(as_u32_millis),
            autofill_delay_ms: r.new_autofill_delay.map(as_u32_millis),
            autofill_batch_size: r.new_autofill_batch_size,
        }
    }
}

impl<'a> From<&'a PlaygroundState> for Configuration {
    fn from(s: &'a PlaygroundState) -> Self {
        Self {
            nodes: s.target_cluster.clone(),
            concurrency: state::concurrency_of(s).into(),
            heartbeat_interval_ms: s.heartbeat_interval.map(as_u32_millis),
            leader_heartbeat_interval_ms: s.leader_heartbeat_interval.map(as_u32_millis),
            ensure_leadership_interval_ms: s.ensure_leadership_interval.map(as_u32_millis),
            autofill_delay_ms: s.autofill_delay.map(as_u32_millis),
            autofill_batch_size: s.autofill_batch_size,
        }
    }
}

fn as_u32_millis(d: instant::Duration) -> u32 {
    u128::clamp(d.as_millis(), 0, u32::MAX.into()) as u32
}

#[wasm_bindgen]
extern "C" {
    pub type JsConfiguration;

    #[wasm_bindgen(method, getter)]
    pub fn nodes(this: &JsConfiguration) -> Nodes;

    #[wasm_bindgen(method, getter)]
    pub fn concurrency(this: &JsConfiguration) -> usize;

    #[wasm_bindgen(method, getter, js_name = heartbeatIntervalMs)]
    pub fn heartbeat_interval_ms(this: &JsConfiguration) -> Option<u32>;

    #[wasm_bindgen(method, getter, js_name = leaderHeartbeatIntervalMs)]
    pub fn leader_heartbeat_interval_ms(this: &JsConfiguration) -> Option<u32>;

    #[wasm_bindgen(method, getter, js_name = ensureLeadershipIntervalMs)]
    pub fn ensure_leadership_interval_ms(this: &JsConfiguration) -> Option<u32>;

    #[wasm_bindgen(method, getter, js_name = autofillDelayMs)]
    pub fn autofill_delay_ms(this: &JsConfiguration) -> Option<u32>;

    #[wasm_bindgen(method, getter, js_name = autofillBatchSize)]
    pub fn autofill_batch_size(this: &JsConfiguration) -> usize;

    pub type Callbacks;

    #[wasm_bindgen(method)]
    pub fn apply(
        this: &Callbacks,
        node_id: usize,
        round: u32,
        action: String,
        new_concurrency: usize,
    );

    #[wasm_bindgen(method)]
    pub fn autofill(this: &Callbacks, node_id: usize);

    #[wasm_bindgen(method)]
    pub fn commit(this: &Callbacks, node_id: usize, round: u32, action: String);

    #[wasm_bindgen(method, js_name = e2eDelayUpdated)]
    pub fn e2e_delay_updated(this: &Callbacks, from: usize, to: usize, mean: f32, std_dev: f32);

    #[wasm_bindgen(method, js_name = ensureLeadership)]
    pub fn ensure_leadership(this: &Callbacks, node_id: usize);

    #[wasm_bindgen(method)]
    pub fn heartbeat(this: &Callbacks, node_id: usize);

    #[wasm_bindgen(method, js_name = newLeader)]
    pub fn new_leader(this: &Callbacks, node_id: usize, leader: Option<usize>);

    #[wasm_bindgen(method, js_name = newSnapshot)]
    pub fn new_snapshot(this: &Callbacks, snapshot: Snapshot);

    #[wasm_bindgen(method)]
    pub fn packet(
        this: &Callbacks,
        from: usize,
        to: usize,
        delay: u32,
        dropped: bool,
        response: Option<String>,
    );

    #[wasm_bindgen(method, js_name = packetLossUpdated)]
    pub fn packet_loss_updated(this: &Callbacks, from: usize, to: usize, packet_loss: f32);

    #[wasm_bindgen(method, js_name = participationChanged)]
    pub fn participation_changed(this: &Callbacks, node_id: usize, active: bool);

    #[wasm_bindgen(method)]
    pub fn reconfigured(this: &Callbacks, round: u32, new_configuration: Configuration);

    #[wasm_bindgen(method, js_name = statusChanged)]
    pub fn status_changed(this: &Callbacks, node_id: usize, new_status: String);

}

#[wasm_bindgen]
pub struct Snapshot {
    #[wasm_bindgen(readonly, js_name = nodeId)]
    pub node_id: usize,
    #[wasm_bindgen(readonly)]
    pub round: u32,
    resumable: Cell<bool>,
    inner: paxakos::invocation::SnapshotFor<PlaygroundInvocation>,
}

#[wasm_bindgen]
impl Snapshot {
    #[wasm_bindgen(method, getter)]
    pub fn resumable(&self) -> bool {
        self.resumable.get()
    }
}

#[wasm_bindgen]
pub struct ClusterBuilder {
    default_packet_loss: f32,
    default_e2e_delay_mean: f32,
    default_e2e_delay_std_dev: f32,
    initial_concurrency: usize,
    callbacks: Rc<Callbacks>,
}

#[wasm_bindgen]
impl ClusterBuilder {
    #[wasm_bindgen(constructor)]
    pub fn new(callbacks: Callbacks) -> Self {
        let callbacks = Rc::new(callbacks);

        Self {
            default_packet_loss: 0.0,
            default_e2e_delay_mean: 0.0,
            default_e2e_delay_std_dev: 0.0,
            initial_concurrency: 1,

            callbacks,
        }
    }

    #[wasm_bindgen(method, js_name = setDefaultPacketLoss)]
    pub fn set_default_packet_loss(&mut self, v: f32) {
        self.default_packet_loss = v;
    }

    #[wasm_bindgen(method, js_name = setDefaultE2eDelay)]
    pub fn set_default_e2e_delay(&mut self, mean: f32, std_dev: f32) {
        self.default_e2e_delay_mean = mean;
        self.default_e2e_delay_std_dev = std_dev;
    }

    #[wasm_bindgen(method, js_name = setInitialConcurrency)]
    pub fn set_initial_concurrency(&mut self, concurrency: usize) {
        self.initial_concurrency = concurrency;
    }

    #[wasm_bindgen(method)]
    pub fn build(self) -> Cluster {
        let communicators = DirectCommunicators::with_characteristics(
            self.default_packet_loss,
            rand_distr::Normal::new(self.default_e2e_delay_mean, self.default_e2e_delay_std_dev)
                .unwrap(),
        );

        let communicators_clone = communicators.clone();
        let callbacks = self.callbacks.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let mut events = communicators_clone.events();
            drop(communicators_clone);

            while let Some(e) = events.next().await {
                callbacks.packet(
                    e.sender,
                    e.receiver,
                    u32::try_from(e.e2e_delay.as_millis()).expect("e2e delay exceeds u32"),
                    e.dropped,
                    match e.payload {
                        DirectCommunicatorPayload::Promise(true)
                        | DirectCommunicatorPayload::Accept(true)
                        | DirectCommunicatorPayload::Committed(true) => {
                            Some("positive".to_string())
                        }
                        DirectCommunicatorPayload::Promise(false)
                        | DirectCommunicatorPayload::Accept(false)
                        | DirectCommunicatorPayload::Committed(false) => {
                            Some("negative".to_string())
                        }
                        _ => None,
                    },
                );
            }
        });

        Cluster {
            callbacks: self.callbacks,
            communicators,
            initial_concurrency: self.initial_concurrency,
            node_infos: Vec::new(),
        }
    }
}

#[wasm_bindgen]
pub struct Cluster {
    callbacks: Rc<Callbacks>,
    communicators: DirectCommunicators<PlaygroundInvocation>,
    initial_concurrency: usize,
    node_infos: Vec<PrototypingNode>,
}

#[wasm_bindgen]
impl Cluster {
    #[wasm_bindgen(method, getter)]
    pub fn network(&self) -> Network {
        Network {
            callbacks: Rc::clone(&self.callbacks),
            communicators: self.communicators.clone(),
        }
    }

    #[wasm_bindgen(method, js_name = addNode)]
    pub fn add_node(&mut self) -> NodeIdentity {
        let node_info = PrototypingNode::new();

        self.node_infos.push(node_info);

        NodeIdentity(node_info)
    }

    #[wasm_bindgen(method, js_name = recoverNode)]
    pub fn recover_node(&mut self, id: &NodeIdentity, snapshot: &Snapshot) -> PlaygroundNode {
        self.start_node_internal(id, Starter::Recover(snapshot.inner.clone()))
    }

    #[wasm_bindgen(method, js_name = resumeNode)]
    pub fn resume_node(&mut self, id: &NodeIdentity, snapshot: &Snapshot) -> PlaygroundNode {
        if snapshot.resumable() {
            snapshot.resumable.set(false);
            self.start_node_internal(id, Starter::Resume(snapshot.inner.clone()))
        } else {
            panic!("Snapshot isn't resumable.")
        }
    }

    // TODO create an initial snapshot and use resume_node
    #[wasm_bindgen(method, js_name = startNode)]
    pub fn start_node(&mut self, id: &NodeIdentity) -> PlaygroundNode {
        self.start_node_internal(
            id,
            Starter::Resume(paxakos::node::Snapshot::initial_with(PlaygroundState::new(
                self.node_infos.clone(),
                self.initial_concurrency,
            ))),
        )
    }
}

impl Cluster {
    pub fn start_node_internal(
        &mut self,
        id: &NodeIdentity,
        stateyness: Starter<PlaygroundInvocation>,
    ) -> PlaygroundNode {
        let kit = NodeKit::new();
        let handle = kit.handle();

        let node_info = id.0;
        let communicators = self.communicators.clone();
        let communicator = communicators.create_communicator_for(node_info.id());

        let callbacks = Rc::clone(&self.callbacks);

        let (sender, mut receiver) = oneshot::channel();

        wasm_bindgen_futures::spawn_local(async move {
            let (handler, mut node) = PlaygroundInvocation::node_builder()
                .for_node(node_info.id())
                .communicating_via(communicator)
                .with(stateyness)
                .using(kit)
                .driven_by(WasmExecutor)
                .track_leadership()
                .fill_gaps(AutofillConfig::new(Rc::clone(&callbacks)))
                .send_heartbeats(HeartbeatConfig::new(Rc::clone(&callbacks)))
                .ensure_leadership(EnsureLeadershipConfig::new(Rc::clone(&callbacks)))
                .spawn()
                .await
                .unwrap();

            communicators.register(node_info.id(), handler).await;

            let mut active = false;
            callbacks.participation_changed(node.id(), active);

            let mut leader = None;

            let mut next_event = node.next_event();

            loop {
                match futures::future::select(&mut receiver, &mut next_event).await {
                    futures::future::Either::Left((Ok(Msg::ShutDown), _)) => {
                        let node_id = node.id();

                        let mut shut_down = node.shut_down();

                        let snapshot = loop {
                            match futures::future::poll_fn(|cx| shut_down.poll_shutdown(cx)).await {
                                ShutdownEvent::Regular(_) => {}
                                ShutdownEvent::Final { snapshot, .. } => {
                                    break snapshot;
                                }
                            }
                        };

                        callbacks.new_snapshot(Snapshot {
                            node_id,
                            round: snapshot.round(),
                            resumable: Cell::new(true),
                            inner: snapshot,
                        });

                        break;
                    }
                    futures::future::Either::Left((Ok(Msg::Crash) | Err(_), _)) => break,
                    futures::future::Either::Right((event, _)) => {
                        match &event {
                            paxakos::Event::Init {
                                round,
                                state: Some(state),
                                ..
                            }
                            | paxakos::Event::Install {
                                round,
                                state: Some(state),
                                ..
                            } => {
                                callbacks.reconfigured(*round, Configuration::from(&**state));
                            }

                            paxakos::Event::StatusChange { new_status, .. } => {
                                callbacks.status_changed(node.id(), format!("{new_status:?}"))
                            }

                            paxakos::Event::Commit {
                                round, log_entry, ..
                            } => {
                                callbacks.commit(node.id(), *round, log_entry.as_str().into());
                            }

                            paxakos::Event::Apply {
                                round,
                                log_entry,
                                new_concurrency,
                                ..
                            } => {
                                callbacks.apply(
                                    node.id(),
                                    *round,
                                    log_entry.as_str().into(),
                                    (*new_concurrency).into(),
                                );

                                if let PlaygroundLogEntry::Reconfigure(_, r) = &**log_entry {
                                    callbacks.reconfigured(*round, Configuration::from(r));
                                }
                            }

                            _ => {}
                        }

                        let new_leader = node.strict_leadership().get(0).map(|l| l.leader);
                        if new_leader != leader {
                            leader = new_leader;

                            callbacks.new_leader(node.id(), leader);
                        }

                        if active != (node.participation() == Participation::Active) {
                            active = node.participation() == Participation::Active;
                            callbacks.participation_changed(node.id(), active);
                        }

                        next_event = node.next_event();
                    }
                }
            }
        });

        PlaygroundNode {
            callbacks: Rc::clone(&self.callbacks),
            id: id.id(),
            handle,
            sender,
        }
    }
}

enum Msg {
    Crash,
    ShutDown,
}

#[wasm_bindgen]
#[derive(Default)]
pub struct Nodes(Vec<PrototypingNode>);

#[wasm_bindgen]
impl Nodes {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    #[wasm_bindgen(method)]
    pub fn push(mut self, node: &NodeIdentity) -> Self {
        self.0.push(node.0);
        self
    }
}

#[wasm_bindgen]
pub struct NodeIdentity(PrototypingNode);

#[wasm_bindgen]
impl NodeIdentity {
    #[wasm_bindgen(method)]
    pub fn id(&self) -> usize {
        self.0.id()
    }
}

#[wasm_bindgen]
pub struct PlaygroundNode {
    callbacks: Rc<Callbacks>,
    id: usize,
    handle: NodeHandle<PlaygroundInvocation>,
    sender: oneshot::Sender<Msg>,
}

#[wasm_bindgen]
impl PlaygroundNode {
    #[wasm_bindgen(method)]
    pub fn crash(self) {
        let _ = self.sender.send(Msg::Crash);
    }

    #[wasm_bindgen(method, js_name = shutDown)]
    pub fn shut_down(self) {
        let _ = self.sender.send(Msg::ShutDown);
    }

    #[wasm_bindgen(method, js_name = takeSnapshot)]
    pub fn take_snapshot(&self) {
        let f = self.handle.prepare_snapshot();
        let callbacks = Rc::clone(&self.callbacks);
        let node_id = self.id;

        wasm_bindgen_futures::spawn_local(async move {
            if let Ok(s) = f.await {
                callbacks.new_snapshot(Snapshot {
                    node_id,
                    round: s.round(),
                    resumable: Cell::new(false),
                    inner: s,
                })
            }
        })
    }

    #[wasm_bindgen(method, js_name = queueAppends)]
    pub fn queue_appends(&self, min_round: u32, max_round: u32, num: u32) {
        for _ in 0..num {
            wasm_bindgen_futures::spawn_local(
                self.handle
                    .append(
                        PlaygroundLogEntry::Regular(Uuid::new_v4()),
                        AppendArgs {
                            retry_policy: RetryIndefinitely::pausing_up_to(
                                instant::Duration::from_secs(4),
                            ),
                            round: min_round..=max_round,
                            importance: Default::default(),
                        },
                    )
                    .map(|_| ()),
            );
        }
    }

    #[wasm_bindgen(method)]
    pub fn reconfigure(&self, new_configuration: JsConfiguration) {
        wasm_bindgen_futures::spawn_local(
            self.handle
                .append(
                    PlaygroundLogEntry::Reconfigure(
                        Uuid::new_v4(),
                        Reconfiguration {
                            new_nodes: new_configuration.nodes().0,
                            new_concurrency: new_configuration.concurrency(),
                            new_heartbeat_interval: new_configuration
                                .heartbeat_interval_ms()
                                .map(|d| instant::Duration::from_millis(d.into())),
                            new_leader_heartbeat_interval: new_configuration
                                .leader_heartbeat_interval_ms()
                                .map(|d| instant::Duration::from_millis(d.into())),
                            new_ensure_leadership_interval: new_configuration
                                .ensure_leadership_interval_ms()
                                .map(|d| instant::Duration::from_millis(d.into())),
                            new_autofill_delay: new_configuration
                                .autofill_delay_ms()
                                .map(|d| instant::Duration::from_millis(d.into())),
                            new_autofill_batch_size: new_configuration.autofill_batch_size(),
                        },
                    ),
                    RetryIndefinitely::pausing_up_to(instant::Duration::from_secs(4)),
                )
                .map(|_| ()),
        );
    }
}

#[derive(Debug)]
pub struct PlaygroundInvocation;

impl Invocation for PlaygroundInvocation {
    type RoundNum = u32;
    type CoordNum = u32;

    type State = PlaygroundState;

    type Yea = instant::Duration;
    type Nay = ();
    type Abstain = instant::Duration;

    type Ejection = Infallible;

    type CommunicationError = DirectCommunicatorError;
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum PlaygroundLogEntry {
    Regular(Uuid),
    Reconfigure(Uuid, Reconfiguration),
    Heartbeat(Uuid),
    EnsureLeadership(Uuid),
    Fill(Uuid),
}

impl PlaygroundLogEntry {
    fn as_str(&self) -> &'static str {
        match self {
            PlaygroundLogEntry::Heartbeat(_) => "heartbeat",
            PlaygroundLogEntry::EnsureLeadership(_) => "ensure-leadership",
            PlaygroundLogEntry::Reconfigure(..) => "reconfigure",
            PlaygroundLogEntry::Regular(_) => "regular",
            PlaygroundLogEntry::Fill(_) => "fill",
        }
    }
}

impl LogEntry for PlaygroundLogEntry {
    type Id = Uuid;

    fn id(&self) -> Self::Id {
        match *self {
            PlaygroundLogEntry::Reconfigure(id, _)
            | PlaygroundLogEntry::Regular(id)
            | PlaygroundLogEntry::Heartbeat(id)
            | PlaygroundLogEntry::EnsureLeadership(id)
            | PlaygroundLogEntry::Fill(id) => id,
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Reconfiguration {
    new_nodes: Vec<PrototypingNode>,
    new_concurrency: usize,
    new_heartbeat_interval: Option<instant::Duration>,
    new_leader_heartbeat_interval: Option<instant::Duration>,
    new_ensure_leadership_interval: Option<instant::Duration>,
    new_autofill_delay: Option<instant::Duration>,
    new_autofill_batch_size: usize,
}

#[derive(Clone, Debug)]
pub struct PlaygroundState {
    applied: HashSet<Uuid>,

    cluster: paxakos::cluster::Cluster<PrototypingNode>,

    target_cluster: Vec<PrototypingNode>,
    heartbeat_interval: Option<instant::Duration>,
    leader_heartbeat_interval: Option<instant::Duration>,
    ensure_leadership_interval: Option<instant::Duration>,
    autofill_delay: Option<instant::Duration>,
    autofill_batch_size: usize,
}

impl PlaygroundState {
    pub fn new(nodes: Vec<PrototypingNode>, concurrency: usize) -> Self {
        Self {
            applied: HashSet::new(),

            cluster: paxakos::cluster::Cluster::new(
                nodes.clone(),
                std::num::NonZeroUsize::new(concurrency).unwrap(),
            ),

            target_cluster: nodes,
            heartbeat_interval: Some(instant::Duration::from_secs(5)),
            leader_heartbeat_interval: Some(instant::Duration::from_secs(3)),
            ensure_leadership_interval: Some(instant::Duration::from_secs(11)),
            autofill_delay: Some(instant::Duration::from_secs(1)),
            autofill_batch_size: 10,
        }
    }
}

impl State for PlaygroundState {
    type Frozen = Self;

    type Context = ();

    type LogEntry = PlaygroundLogEntry;
    type Outcome = usize;
    type Effect = PlaygroundEffect;

    type Node = PrototypingNode;

    fn apply(
        &mut self,
        log_entry: &Self::LogEntry,
        _context: &mut (),
    ) -> (Self::Outcome, Self::Effect) {
        self.cluster.apply(&PlaygroundClusterLogEntry {
            previous_target_cluster: &self.target_cluster,
            inner: log_entry,
        });

        self.applied.insert(log_entry.id());

        if let PlaygroundLogEntry::Reconfigure(_, reconfiguration) = log_entry {
            self.target_cluster = reconfiguration.new_nodes.clone();
            self.heartbeat_interval = reconfiguration.new_heartbeat_interval;
            self.leader_heartbeat_interval = reconfiguration.new_leader_heartbeat_interval;
            self.ensure_leadership_interval = reconfiguration.new_ensure_leadership_interval;
            self.autofill_delay = reconfiguration.new_autofill_delay;
            self.autofill_batch_size = reconfiguration.new_autofill_batch_size;

            (
                self.applied.len(),
                PlaygroundEffect::Reconfigured(reconfiguration.clone()),
            )
        } else {
            (self.applied.len(), PlaygroundEffect::None)
        }
    }

    fn concurrency(this: Option<&Self>) -> Option<std::num::NonZeroUsize> {
        this.map(|t| t.cluster.concurrency_at_offset_one())
    }

    fn cluster_at(&self, round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
        self.cluster
            .nodes_at(round_offset)
            .expect("offset out of concurrency range")
    }

    fn freeze(&self) -> Self::Frozen {
        self.clone()
    }
}

struct PlaygroundClusterLogEntry<'a> {
    previous_target_cluster: &'a Vec<PrototypingNode>,
    inner: &'a PlaygroundLogEntry,
}

impl<'a> paxakos::cluster::ClusterLogEntry<PrototypingNode> for PlaygroundClusterLogEntry<'a> {
    fn concurrency(&self) -> Option<std::num::NonZeroUsize> {
        match self.inner {
            PlaygroundLogEntry::Reconfigure(_, r) => {
                Some(std::num::NonZeroUsize::new(r.new_concurrency).unwrap())
            }
            _ => None,
        }
    }

    fn added_nodes(&self) -> Vec<PrototypingNode> {
        let mut added = Vec::new();

        if let PlaygroundLogEntry::Reconfigure(_, r) = self.inner {
            for n in &r.new_nodes {
                if !self.previous_target_cluster.contains(n) {
                    added.push(*n)
                }
            }
        }

        added
    }

    fn removed_nodes(&self) -> Vec<PrototypingNode> {
        let mut removed = Vec::new();

        if let PlaygroundLogEntry::Reconfigure(_, r) = self.inner {
            for n in self.previous_target_cluster {
                if !r.new_nodes.contains(n) {
                    removed.push(*n)
                }
            }
        }

        removed
    }
}

#[derive(Clone, Debug)]
pub enum PlaygroundEffect {
    None,
    Reconfigured(Reconfiguration),
}

struct HeartbeatConfig<N, I> {
    callbacks: Rc<Callbacks>,
    node_id: usize,

    interval: Option<instant::Duration>,
    leader_interval: Option<instant::Duration>,

    _p: std::marker::PhantomData<(N, I)>,
}

impl<N, I> HeartbeatConfig<N, I> {
    fn new(callbacks: Rc<Callbacks>) -> Self {
        Self {
            callbacks,
            node_id: usize::MAX,

            interval: None,
            leader_interval: None,

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
    type RetryPolicy = DoNotRetry<PlaygroundInvocation>;

    fn init(&mut self, node: &Self::Node) {
        self.node_id = node.id();
    }

    fn update(&mut self, event: &paxakos::node::EventFor<Self::Node>) {
        if let paxakos::Event::Init {
            state: Some(state), ..
        }
        | paxakos::Event::Install {
            state: Some(state), ..
        } = event
        {
            self.interval = state.heartbeat_interval;
            self.leader_interval = state.leader_heartbeat_interval;
        }

        if let paxakos::Event::Apply {
            effect: PlaygroundEffect::Reconfigured(reconfiguration),
            ..
        } = event
        {
            self.interval = reconfiguration.new_heartbeat_interval;
            self.leader_interval = reconfiguration.new_leader_heartbeat_interval;
        }
    }

    fn leader_interval(&self) -> Option<instant::Duration> {
        self.leader_interval
    }

    fn interval(&self) -> Option<instant::Duration> {
        self.interval
    }

    fn new_heartbeat(&self) -> Self::Applicable {
        self.callbacks.heartbeat(self.node_id);

        PlaygroundLogEntry::Heartbeat(Uuid::new_v4())
    }

    fn retry_policy(&self) -> Self::RetryPolicy {
        DoNotRetry::new()
    }
}

struct AutofillConfig<N> {
    callbacks: Rc<Callbacks>,
    node_id: usize,

    delay: Option<instant::Duration>,
    batch_size: usize,

    _p: std::marker::PhantomData<N>,
}

impl<N> AutofillConfig<N> {
    fn new(callbacks: Rc<Callbacks>) -> Self {
        Self {
            callbacks,
            node_id: usize::MAX,

            delay: None,
            batch_size: 1,

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

    fn update(&mut self, event: &paxakos::node::EventFor<Self::Node>) {
        if let paxakos::Event::Init {
            state: Some(state), ..
        }
        | paxakos::Event::Install {
            state: Some(state), ..
        } = event
        {
            self.delay = state.autofill_delay;
            self.batch_size = state.autofill_batch_size;
        }

        if let paxakos::Event::Apply {
            effect: PlaygroundEffect::Reconfigured(reconfiguration),
            ..
        } = event
        {
            self.delay = reconfiguration.new_autofill_delay;
            self.batch_size = reconfiguration.new_autofill_batch_size;
        }
    }

    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn delay(&self) -> Option<instant::Duration> {
        self.delay
    }

    fn new_filler(&self) -> Self::Applicable {
        self.callbacks.autofill(self.node_id);

        PlaygroundLogEntry::Fill(Uuid::new_v4())
    }

    fn retry_policy(&self) -> Self::RetryPolicy {
        DoNotRetry::new()
    }
}

struct EnsureLeadershipConfig<N> {
    callbacks: Rc<Callbacks>,
    node_id: usize,

    interval: Option<instant::Duration>,

    _p: std::marker::PhantomData<N>,
}

impl<N> EnsureLeadershipConfig<N> {
    fn new(callbacks: Rc<Callbacks>) -> Self {
        Self {
            callbacks,
            node_id: usize::MAX,

            interval: None,

            _p: std::marker::PhantomData,
        }
    }
}

impl<N: Node<Invocation = PlaygroundInvocation>> leadership::ensure::Config
    for EnsureLeadershipConfig<N>
{
    type Node = N;
    type Applicable = PlaygroundLogEntry;
    type RetryPolicy = DoNotRetry<PlaygroundInvocation>;

    fn init(&mut self, node: &Self::Node) {
        self.node_id = node.id();
    }

    fn update(&mut self, event: &paxakos::node::EventFor<Self::Node>) {
        if let paxakos::Event::Init {
            state: Some(state), ..
        }
        | paxakos::Event::Install {
            state: Some(state), ..
        } = event
        {
            self.interval = state.ensure_leadership_interval;
        }

        if let paxakos::Event::Apply {
            effect: PlaygroundEffect::Reconfigured(reconfiguration),
            ..
        } = event
        {
            self.interval = reconfiguration.new_ensure_leadership_interval;
        }
    }

    fn interval(&self) -> Option<instant::Duration> {
        self.interval
    }

    fn new_leadership_taker(&self) -> Self::Applicable {
        self.callbacks.ensure_leadership(self.node_id);

        PlaygroundLogEntry::EnsureLeadership(Uuid::new_v4())
    }

    fn retry_policy(&self) -> Self::RetryPolicy {
        DoNotRetry::new()
    }
}
