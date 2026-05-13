use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use std::error::Error;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{channel, Sender};
use tokio_websockets::{Message, ServerBuilder, WebSocketStream};

async fn handle_connection(
    addr: SocketAddr,
    mut ws_stream: WebSocketStream<TcpStream>,
    bcast_tx: Sender<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut bcast_rx = bcast_tx.subscribe();

    loop {
        tokio::select! {
            incoming_msg = ws_stream.next() => {
                match incoming_msg {
                    Some(Ok(msg)) => {
                        if let Some(text) = msg.as_text() {
                            println!("From {addr:?}: {text}");

                            let broadcast_msg = format!("{addr}: {text}");
                            bcast_tx.send(broadcast_msg)?;
                        }
                    }
                    Some(Err(e)) => {
                        println!("Error from {addr:?}: {e}");
                        break;
                    }
                    None => {
                        println!("Client {addr:?} disconnected");
                        break;
                    }
                }
            }

            broadcast_msg = bcast_rx.recv() => {
                match broadcast_msg {
                    Ok(msg) => {
                        ws_stream.send(Message::text(msg)).await?;
                    }
                    Err(e) => {
                        println!("Broadcast error for {addr:?}: {e}");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let (bcast_tx, _) = channel(16);

    let listener = TcpListener::bind("127.0.0.1:2000").await?;
    println!("listening on port 2000");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {addr:?}");

        let bcast_tx = bcast_tx.clone();

        tokio::spawn(async move {
            let (_req, ws_stream) = ServerBuilder::new().accept(socket).await?;

            handle_connection(addr, ws_stream, bcast_tx).await
        });
    }
}