use anyhow::bail;
use protocol::{Body, Message, Payload};
use tokio::sync::mpsc::{Receiver, Sender};

pub struct MessageHandler {
    receiver: Receiver<Message>,
    sender: Sender<Message>,
    id: Option<String>,
    other_nodes: Vec<String>,
}

impl MessageHandler {
    pub fn new(receiver: Receiver<Message>, sender: Sender<Message>) -> Self {
        MessageHandler {
            receiver,
            sender,
            id: None,
            other_nodes: vec![],
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
                Payload::Echo { echo } => {
                    self.send(Message {
                        src: dest,
                        dest: src,
                        body: Body {
                            payload: Payload::EchoOk { echo },
                            in_reply_to: msg_id,
                            msg_id: Some(1),
                        },
                    })
                    .await?;
                }
                // Do nothing
                Payload::EchoOk { .. } => eprintln!(
                    "Received unexpected echo_ok from {}, msg_id {:?}",
                    src, msg_id
                ),
                Payload::Init { node_id, node_ids } => {
                    self.handle_init(node_id, node_ids);
                    self.send(Message {
                        src: dest,
                        dest: src,
                        body: Body {
                            payload: Payload::InitOk,
                            in_reply_to: msg_id,
                            msg_id: Some(1),
                        },
                    })
                    .await?;
                }
                Payload::InitOk => bail!(
                    "Should not receive init_ok, but did from {}, {:?}",
                    src,
                    msg_id
                ),
            }
        }
        Ok(())
    }

    fn handle_init(&mut self, node_id: String, other_nodes: Vec<String>) {
        self.id = Some(node_id);
        self.other_nodes = other_nodes;
    }

    async fn send(&mut self, message: Message) -> anyhow::Result<()> {
        eprintln!("sending {:?}", message);
        self.sender.send(message).await?;
        Ok(())
    }
}
