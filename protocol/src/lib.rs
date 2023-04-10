//! Implementation of the [maelstrom](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md) protocol

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Body {
    pub msg_id: Option<u64>,
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

#[cfg(test)]
mod tests {
    use crate::Message;

    #[test]
    fn test_payloads() -> anyhow::Result<()> {
        let serialized_result = serde_json::to_string(&Message {
            src: "src".into(),
            dest: "dst".into(),
            body: crate::Body {
                msg_id: Some(1),
                in_reply_to: Some(2),
                payload: crate::Payload::InitOk,
            },
        })?;
        println!("{}", serialized_result);
        Ok(())
    }
}