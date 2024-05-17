use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use db::Db;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use bytes::BytesMut;

use crate::{handler::MessageHandler, parser::parse_data};

mod db;
mod handler;
mod message;
mod parser;

/// A redis server implementation
#[derive(Parser, Debug)]
struct Args {
    /// Which port should be used
    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<Vec<String>>,
}

struct ServerState {
    role: String,
    master_replid: String,
    master_repl_offset: u32,
}

#[tokio::main]
async fn main() {

    let args = Args::parse();
    let port = args.port;

    println!("Using port {port}");
    println!("{:?}", args.replicaof);

    let db = Arc::new(Db::new());
    let state = Arc::new(ServerState {
        role : if args.replicaof.is_some() {
            "slave".to_string()
        } else {
            "master".to_string()
        },
        master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        master_repl_offset: 0,
    });

    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let db_cloned = db.clone();
                let state_cloned = state.clone();
                tokio::spawn(async move {
                    handle_connection(stream, db_cloned, state_cloned).await.unwrap_or_else(|error| eprintln!("{:?}", error));
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, db: Arc<Db>, state: Arc<ServerState>) -> Result<()> {
    let mut buffer = BytesMut::with_capacity(1024);
    let handler = MessageHandler::new(db, state);

    loop {
        let n = stream.read_buf(&mut buffer).await?;

        if n == 0 {
            println!("Connection closed by client");
            return Ok(()); 
        }

        let message = parse_data(buffer.split())?;

        println!("Received from client: {}", message);

        let response = handler.handle(message).await?;
        println!("Responding: {}", response);
        stream.write_all(&response.to_data()).await?;
    }
}
