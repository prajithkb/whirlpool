use anyhow::{Context, Ok};
use echo::MessageHandler;
use protocol::Message;
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::mpsc,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let reader = BufReader::new(stdin);
    let mut writer = BufWriter::new(stdout);
    let mut lines = reader.lines();
    let (input_tx, input_rx) = mpsc::channel::<Message>(32);
    let (output_tx, mut output_rx) = mpsc::channel::<Message>(32);
    let mut message_handler = MessageHandler::new(input_rx, output_tx);
    // Process loop
    tokio::spawn(async move { message_handler.handle().await });
    // Write loop
    tokio::spawn(async move {
        while let Some(message) = output_rx.recv().await {
            let buf = serde_json::to_string(&message).context("Serialization failed")?;
            eprintln!("Outgoing serialized message <{}>", &buf);
            writer.write_all(buf.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        }
        Ok(())
    });
    
    // Read loop
    while let Some(line) = lines.next_line().await? {
        eprintln!("Incoming serialized message <{}>", &line);
        let message =
            serde_json::from_str::<Message>(line.as_str()).context("Message parsing failed")?;
        input_tx
            .send(message)
            .await
            .context("Sending message failed")?;
    }
    Ok(())
}
