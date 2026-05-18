use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use http::Uri;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_websockets::{ClientBuilder, Message};

#[tokio::main]
async fn main() -> Result<(), tokio_websockets::Error> {
    let (mut ws_stream, _) =
        ClientBuilder::from_uri(Uri::from_static("ws://127.0.0.1:8080"))
            .connect()
            .await?;

    let stdin = tokio::io::stdin();
    let mut stdin = BufReader::new(stdin).lines();

    println!("Connected to chat server.");
    println!("Type a message and press Enter.");

    loop {
        tokio::select! {
            user_input = stdin.next_line() => {
                match user_input {
                    Ok(Some(line)) => {
                        ws_stream.send(Message::text(line)).await?;
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        println!("Error reading input: {e}");
                        break;
                    }
                }
            }

            server_msg = ws_stream.next() => {
                match server_msg {
                    Some(Ok(msg)) => {
                        if let Some(text) = msg.as_text() {
                            println!("Edward's Computer - From server: {text}");
                        }
                    }
                    Some(Err(e)) => {
                        println!("Error receiving message: {e}");
                        break;
                    }
                    None => {
                        println!("Server disconnected.");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}