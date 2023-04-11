use anyhow::bail;
use protocol::{Body, Echo, Init, Message, Workload};
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
                protocol::Workload::Echo(echo) => self.handle_echo(echo, src, dest, msg_id).await?,
                protocol::Workload::Init(init) => self.handle_init(init, src, dest, msg_id).await?,
                protocol::Workload::Broadcast(_) => todo!(),
            }
        }
        Ok(())
    }

    async fn send(&mut self, message: Message) -> anyhow::Result<()> {
        eprintln!("sending {:?}", message);
        self.sender.send(message).await?;
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
                self.send(Message {
                    src: dest,
                    dest: src,
                    body: Body {
                        payload: Workload::Echo(Echo::EchoOk { echo }),
                        in_reply_to: msg_id,
                        msg_id: Some(1),
                    },
                })
                .await?;
            }
            // Do nothing
            Echo::EchoOk { .. } => eprintln!(
                "Received unexpected echo_ok from {}, msg_id {:?}",
                src, msg_id
            ),
        };
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
                self.send(Message {
                    src: dest,
                    dest: src,
                    body: Body {
                        payload: Workload::Init(Init::InitOk),
                        in_reply_to: msg_id,
                        msg_id: Some(1),
                    },
                })
                .await?;
            }
            Init::InitOk => bail!(
                "Should not receive init_ok, but did from {}, {:?}",
                src,
                msg_id
            ),
        }
        Ok(())
    }
}
