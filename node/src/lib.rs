use protocol::{Body, Echo, Init, Message, Workload};
use serde_json::Value;
use tokio::sync::mpsc::{Receiver, Sender};

const MISSING_MESSAGE_ID: u64 = 0;

#[derive(Debug)]
pub struct MessageHandler {
    receiver: Receiver<Message>,
    sender: Sender<Message>,
    id: Option<String>,
    other_nodes: Vec<String>,
    messages: Vec<Value>,
    neighbours: Vec<String>,
}

impl MessageHandler {
    pub fn new(receiver: Receiver<Message>, sender: Sender<Message>) -> Self {
        MessageHandler {
            receiver,
            sender,
            id: None,
            other_nodes: vec![],
            messages: vec![],
            neighbours: vec![],
        }
    }
    pub async fn handle(&mut self) -> anyhow::Result<()> {
        while let Some(message) = self.receiver.recv().await {
            let Message { src, dest, body } = message;
            let Body {
                msg_id,
                in_reply_to: _,
                payload,
            } = body;
            match payload {
                protocol::Workload::Echo(echo) => self.handle_echo(echo, src, dest, msg_id).await?,
                protocol::Workload::Init(init) => self.handle_init(init, src, dest, msg_id).await?,
                protocol::Workload::Broadcast(broadcast) => {
                    self.handle_broadcast(broadcast, src, dest, msg_id).await?
                }
            }
        }
        Ok(())
    }

    async fn send(&mut self, message: Message) -> anyhow::Result<()> {
        self.sender.send(message).await?;
        Ok(())
    }

    async fn send_message(
        &mut self,
        src: String,
        dest: String,
        payload: Workload,
        in_reply_to: Option<u64>,
        msg_id: Option<u64>
    ) -> anyhow::Result<()> {
        let msg_id = msg_id.or(Some(MISSING_MESSAGE_ID));
        let msg = Message {
            src,
            dest,
            body: Body {
                payload,
                in_reply_to,
                msg_id,
            },
        };
        self.send(msg).await?;
        Ok(())
    }
    async fn handle_echo(
        &mut self,
        echo: Echo,
        src: String,
        dest: String,
        msg_id: Option<u64>,
    ) -> anyhow::Result<()> {
        match echo {
            Echo::Echo { echo } => {
                self.send_message(dest, src, Workload::Echo(Echo::EchoOk { echo }), msg_id, None)
                    .await?;
            }
            // Do nothing
            Echo::EchoOk { .. } => eprintln!(
                "Received unexpected echo_ok from {}, msg_id {:?}",
                src, msg_id
            ),
        };
        eprintln!("Echo handled");
        Ok(())
    }

    async fn handle_init(
        &mut self,
        init: Init,
        src: String,
        dest: String,
        msg_id: Option<u64>,
    ) -> anyhow::Result<()> {
        match init {
            Init::Init { node_id, node_ids } => {
                self.id = Some(node_id);
                self.other_nodes = node_ids;
                self.send_message(dest, src, Workload::Init(Init::InitOk), msg_id, None)
                    .await?;
            }
            Init::InitOk => eprintln!(
                "Should not receive init_ok, but did from {}, {:?}",
                src,
                msg_id
            ),
        }
        eprintln!("Initialized node {:?}", self.id);
        Ok(())
    }

    async fn handle_broadcast(
        &mut self,
        broadcast: protocol::Broadcast,
        src: String,
        dest: String,
        msg_id: Option<u64>,
    ) -> anyhow::Result<()> {
        match broadcast {
            protocol::Broadcast::Topology { topology } => {
                // set neighbours
                self.neighbours = self
                    .id
                    .as_ref()
                    .and_then(|id| topology.get(id))
                    .cloned()
                    .unwrap_or_default();
                eprintln!("Neighbours set for node {:?} as {:?}", self.id, self.neighbours);
                // reply
                self.send_message(
                    dest,
                    src,
                    Workload::Broadcast(protocol::Broadcast::TopologyOk),
                    msg_id,
                    None
                )
                .await?;
            }
            protocol::Broadcast::Broadcast { message } => {
                eprintln!("Broadcast message {:?} to neighbours {:?}", message, self.neighbours);
                // Add only of absent
                if self.messages.contains(&message) {
                    eprintln!("Node {:?} has the message {:?} so this is a no-op", self.id, message);
                }  else {
                    self.messages.push(message.clone());
                    // Clone for temp iteration
                    let neighbours = self.neighbours.clone();
                    // broadcast to neighbours
                    for n in neighbours {
                        self.send_message(
                            dest.clone(),
                            n,
                            Workload::Broadcast(protocol::Broadcast::Broadcast {
                                message: message.clone(),
                            }),
                            msg_id,
                            None
                        )
                        .await?;
                    }
                    if msg_id.is_some() {
                        // Respond back
                        self.send_message(
                            dest,
                            src,
                            Workload::Broadcast(protocol::Broadcast::BroadcastOk),
                            msg_id,
                            None
                        )
                        .await?;
                    }
                }
            }
            protocol::Broadcast::Read => {
                eprintln!("Read messages {:?} from  node {:?}", self.messages, self.id);
                self.send_message(
                    dest,
                    src,
                    Workload::Broadcast(protocol::Broadcast::ReadOk {
                        messages: self.messages.clone(),
                    }),
                    msg_id,
                    None
                )
                .await?
            }
            _ => {}
        }
        Ok(())
    }
}
