//! This example is for illustrative purposes only. Basing a chat protocol on
//! consensus is a bad idea, but makes a neat example.

use std::convert::Infallible;

use paxakos::prototyping::DirectCommunicatorError;
use paxakos::prototyping::DirectCommunicators;
use paxakos::prototyping::PrototypingNode;
use paxakos::prototyping::RetryIndefinitely;
use paxakos::Invocation;
use paxakos::LogEntry;
use paxakos::Node;
use paxakos::NodeHandle;
use paxakos::NodeInfo;
use paxakos::State;
use uuid::Uuid;

fn main() {
    let node_a = PrototypingNode::new();
    let node_b = PrototypingNode::new();
    let node_c = PrototypingNode::new();

    let nodes = vec![node_a, node_b, node_c];

    let communicators = DirectCommunicators::new();

    let node_a = spawn_node(node_a, nodes.clone(), communicators.clone());
    let node_b = spawn_node(node_b, nodes.clone(), communicators.clone());
    let node_c = spawn_node(node_c, nodes, communicators);

    futures::executor::block_on(async move {
        let _ = node_a
            .append(
                msg("Alice", "Oh, hey guys"),
                RetryIndefinitely::without_pausing(),
            )
            .await;
    });

    // Because Bob and Charlie reply without synchronization, either may reply
    // first. However, all participants will observe the same person replying
    // first.
    let b = std::thread::spawn(|| {
        futures::executor::block_on(async move {
            let _ = node_b
                .append(
                    msg("Bob", "Hi Alice, long time no see!"),
                    RetryIndefinitely::without_pausing(),
                )
                .await;
        });
    });
    let c = std::thread::spawn(|| {
        futures::executor::block_on(async move {
            let _ = node_c
                .append(
                    msg("Charlie", "Hi Alice, how are you?"),
                    RetryIndefinitely::without_pausing(),
                )
                .await;
        });
    });

    // Let's wait for the appends to go through.
    b.join().unwrap();
    c.join().unwrap();

    // It is guaranteed that all messages above have been appended to the shared log
    // at this point. However, one node may not know about it yet and the others may
    // not have gotten a chance to apply it to their state. Let's give them a chance
    // to do that.
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Graceful shutdown is possible (see `Node::shut_down`) but is too involved for
    // this example.
    std::process::exit(0);
}

fn spawn_node(
    node_info: PrototypingNode,
    all_nodes: Vec<PrototypingNode>,
    communicators: DirectCommunicators<ChatInvocation>,
) -> NodeHandle<ChatInvocation> {
    let (send, recv) = futures::channel::oneshot::channel();

    std::thread::spawn(move || {
        let (handler, mut node) = futures::executor::block_on(
            ChatInvocation::node_builder()
                .for_node(node_info.id())
                .communicating_via(communicators.create_communicator_for(node_info.id()))
                .with_initial_state(ChatState::new(node_info.id(), all_nodes))
                .spawn(),
        )
        .unwrap();

        send.send(node.handle()).unwrap();

        futures::executor::block_on(communicators.register(node_info.id(), handler));

        futures::executor::block_on(futures::future::poll_fn(|cx| {
            let _ = node.poll_events(cx);

            std::task::Poll::<()>::Pending
        }));
    });

    futures::executor::block_on(recv).unwrap()
}

fn msg(sender: &str, message: &str) -> ChatMessage {
    ChatMessage {
        id: Uuid::new_v4(),
        sender: sender.to_string(),
        message: message.to_string(),
    }
}

pub struct ChatInvocation;

impl Invocation for ChatInvocation {
    type RoundNum = u32;
    type CoordNum = u32;

    type State = ChatState;

    type Yea = ();
    type Nay = Infallible;
    type Abstain = Infallible;

    type Ejection = Infallible;

    type CommunicationError = DirectCommunicatorError;
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ChatMessage {
    id: Uuid,
    sender: String,
    message: String,
}

impl LogEntry for ChatMessage {
    type Id = Uuid;

    fn id(&self) -> Self::Id {
        self.id
    }
}

#[derive(Clone, Debug)]
pub struct ChatState {
    node_id: usize,
    nodes: Vec<PrototypingNode>,
}

impl ChatState {
    pub fn new(node_id: usize, nodes: Vec<PrototypingNode>) -> Self {
        Self { node_id, nodes }
    }
}

impl State for ChatState {
    type Frozen = Self;

    type Context = ();

    type LogEntry = ChatMessage;
    type Outcome = ();
    type Effect = ();
    type Error = Infallible;

    type Node = PrototypingNode;

    fn apply(
        &mut self,
        log_entry: &Self::LogEntry,
        _context: &mut Self::Context,
    ) -> Result<(Self::Outcome, Self::Effect), Self::Error> {
        let own_node_id = format!("{:X}", self.node_id + 10);

        println!(
            "[{}] -- {}: {}",
            own_node_id, log_entry.sender, log_entry.message
        );

        Ok(((), ()))
    }

    fn cluster_at(&self, _round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
        self.nodes.clone()
    }

    fn freeze(&self, _context: &mut Self::Context) -> Self::Frozen {
        self.clone()
    }
}
