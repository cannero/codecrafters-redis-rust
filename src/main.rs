use std::sync::Arc;

use anyhow::{bail, Result};
use clap::Parser;
use db::Db;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use bytes::BytesMut;

use crate::{handler::MessageHandler, parser::parse_data};

mod db;
mod handler;
mod message;
mod parser;
mod replication_client;

/// A redis server implementation
#[derive(Parser, Debug)]
struct Args {
    /// Which port should be used
    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,
}

impl Args {
    fn get_leader_addr(&self) -> Result<(String, u16)> {
        match self.replicaof.clone() {
            Some(addr_and_port) => {
                let parts = addr_and_port.split(' ').collect::<Vec<_>>();
                if parts.len() != 2 {
                    bail!("replicaof parts wrong");
                }

                let address = parts[0];
                match parts[1].parse::<u16>() {
                    Ok(port) => Ok((address.to_string(), port)),
                    Err(err) => bail!(err),
                }
            }
            None => bail!("replicaof not set")
        }
    }
}

#[derive(PartialEq)]
enum ServerRole {
    Leader,
    Follower,
}

struct ServerState {
    role: ServerRole,
    master_replid: String,
    master_repl_offset: u32,
}

#[tokio::main]
async fn main() {

    let args = Args::parse();
    let port = args.port;

    println!("Using port {port}");

    let db = Arc::new(Db::new());
    let state = Arc::new(ServerState {
        role : if args.replicaof.is_some() {
            ServerRole::Follower
        } else {
            ServerRole::Leader
        },
        master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        master_repl_offset: 0,
    });

    if state.role == ServerRole::Follower {
        let leader_addr = args.get_leader_addr().expect("replicaof not set correctly");
        tokio::spawn(async move{
            replication_client::start_replication(leader_addr).await
                .unwrap_or_else(|error| eprintln!("replication: {:?}", error));
        });
    }

    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let db_cloned = db.clone();
                let state_cloned = state.clone();
                tokio::spawn(async move {
                    handle_connection(stream, db_cloned, state_cloned).await
                        .unwrap_or_else(|error| eprintln!("{:?}", error));
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
