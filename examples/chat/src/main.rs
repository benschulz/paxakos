//! This example is for illustrative purposes only. Basing a chat protocol on
//! consensus is a bad idea, but makes a neat example.
#![feature(never_type)]

use std::convert::Infallible;

use async_trait::async_trait;
use futures::io::AsyncRead;
use paxakos::append::AppendArgs;
use paxakos::prototyping::{DirectCommunicator, DirectCommunicators};
use paxakos::prototyping::{PrototypingNode, RetryIndefinitely};
use paxakos::{LogEntry, Node, NodeHandle, NodeInfo, State};
use uuid::Uuid;

type ChatCommunicator = DirectCommunicator<ChatState, u64, u32, !>;
type ChatCommunicators = DirectCommunicators<ChatState, u64, u32, !>;

fn main() {
    let node_a = PrototypingNode::new();
    let node_b = PrototypingNode::new();
    let node_c = PrototypingNode::new();

    let nodes = vec![node_a, node_b, node_c];

    let communicators = ChatCommunicators::new();

    let node_a = spawn_node(node_a, nodes.clone(), communicators.clone());
    let node_b = spawn_node(node_b, nodes.clone(), communicators.clone());
    let node_c = spawn_node(node_c, nodes, communicators);

    futures::executor::block_on(async move {
        let _ = node_a
            .append(msg("Alice", "Oh, hey guys"), always_retry())
            .await;
    });

    // Because Bob and Charlie reply without synchronization, either may reply
    // first. However, all participants will observe the same person replying
    // first.
    let b = std::thread::spawn(|| {
        futures::executor::block_on(async move {
            let _ = node_b
                .append(msg("Bob", "Hi Alice, long time no see!"), always_retry())
                .await;
        });
    });
    let c = std::thread::spawn(|| {
        futures::executor::block_on(async move {
            let _ = node_c
                .append(msg("Charlie", "Hi Alice, how are you?"), always_retry())
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
    communicators: ChatCommunicators,
) -> NodeHandle<ChatState, ChatCommunicator> {
    let (send, recv) = futures::channel::oneshot::channel();

    std::thread::spawn(move || {
        let (handler, mut node) = futures::executor::block_on(
            paxakos::node_builder()
                .for_node(node_info.id())
                .working_ephemerally()
                .communicating_via(communicators.create_communicator_for(node_info.id()))
                .with_initial_state(ChatState::new(node_info.id(), all_nodes))
                .spawn_in(()),
        )
        .unwrap();

        send.send(node.handle()).unwrap();

        communicators.register(node_info.id(), handler);

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

fn always_retry() -> AppendArgs<ChatCommunicator> {
    AppendArgs {
        retry_policy: Box::new(RetryIndefinitely::without_pausing()),
        ..Default::default()
    }
}

#[derive(Clone, Debug)]
pub struct ChatMessage {
    id: Uuid,
    sender: String,
    message: String,
}

#[async_trait]
impl LogEntry for ChatMessage {
    type Id = Uuid;
    type Reader = std::io::Cursor<Vec<u8>>;
    type ReadError = Infallible;

    async fn from_reader<R: AsyncRead + Send + Unpin>(_read: R) -> Result<Self, Self::ReadError> {
        unimplemented!()
    }

    fn size(&self) -> usize {
        unimplemented!()
    }

    fn to_reader(&self) -> Self::Reader {
        unimplemented!()
    }

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

#[async_trait]
impl State for ChatState {
    type Context = ();

    type Reader = std::io::Cursor<Vec<u8>>;
    type ReadError = Infallible;

    type LogEntry = ChatMessage;
    type Outcome = ();
    type Event = ();

    type Node = PrototypingNode;

    async fn from_reader<R: AsyncRead + Send + Unpin>(_read: R) -> Result<Self, Self::ReadError> {
        unimplemented!()
    }

    fn size(&self) -> usize {
        unimplemented!()
    }

    fn to_reader(&self) -> Self::Reader {
        unimplemented!()
    }

    fn apply(
        &mut self,
        log_entry: &Self::LogEntry,
        _context: &mut (),
    ) -> (Self::Outcome, Self::Event) {
        let own_node_id = format!("{:X}", self.node_id + 10);

        println!(
            "[{}] -- {}: {}",
            own_node_id, log_entry.sender, log_entry.message
        );

        ((), ())
    }

    fn cluster_at(&self, _round_offset: std::num::NonZeroUsize) -> Vec<Self::Node> {
        self.nodes.clone()
    }
}
