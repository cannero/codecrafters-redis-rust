use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast::Sender,
};

use crate::{
    db::Db, handler::client_server::MessageHandler, message::Message, parser::parse_data,
    ServerConfig,
};

struct ServerState {
    handler: MessageHandler,
    stream: TcpStream,
    sender: Option<Sender<Message>>,
}

pub async fn start(config: Arc<ServerConfig>, db: Arc<Db>, tx: Sender<Message>) -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", config.listener_port)).await?;

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let db_cloned = db.clone();
                let config_cloned = config.clone();
                let tx_cloned = tx.clone();
                let o_tx_cloned2 = Some(tx.clone());
                tokio::spawn(async move {
                    let state = ServerState {
                        handler: MessageHandler::new(db_cloned, config_cloned, tx_cloned),
                        stream,
                        sender: o_tx_cloned2,
                    };
                    handle_connection(state)
                        .await
                        .unwrap_or_else(|error| eprintln!("{:?}", error));
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut state: ServerState) -> Result<()> {
    let mut buffer = BytesMut::with_capacity(1024);

    loop {
        let n = state.stream.read_buf(&mut buffer).await?;

        if n == 0 {
            println!("Connection closed by client");
            return Ok(());
        }

        let messages = parse_data(buffer.split())?;

        for message in messages {
            println!("Received from client: {}", message);
            let response = state.handler.handle(message).await?;

            for message in response {
                println!("Responding: {}", message);
                write_all(&mut state.stream, message).await?;
            }
        }

        if state.handler.replication_client_acknowleged() {
            return handle_replication_client(state).await;
        }
    }
}

async fn handle_replication_client(mut state: ServerState) -> Result<()> {
    println!("upgrading to replication");
    let sender = state.sender.take().expect("sender must be set");
    // TODO: check if this is the correct logic to not have any receiver open.
    // The main goal is to open the receiver when it is needed, for that
    // the sender is used. Does a resubscribe lead to the receiver to just fill up?
    let mut rx = sender.subscribe();
    std::mem::drop(sender);

    loop {
        let message = rx.recv().await?;
        write_all(&mut state.stream, message).await?;
    }
}

async fn write_all(stream: &mut TcpStream, message: Message) -> Result<()> {
    stream.write_all(&message.to_data()).await?;
    Ok(())
}
