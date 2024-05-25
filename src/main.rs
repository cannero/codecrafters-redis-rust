use std::sync::Arc;

use anyhow::{bail, Result};
use clap::Parser;
use db::Db;
use tokio::{net::ToSocketAddrs, sync::broadcast};

use crate::handler::MessageHandler;

mod command_parser;
mod db;
mod handler;
mod message;
mod parser;
mod replication_client;
mod server;

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
    fn get_leader_addr(&self) -> Result<impl ToSocketAddrs> {
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

struct ServerConfig {
    role: ServerRole,
    master_replid: String,
    master_repl_offset: u32,
    listener_port: u16,
}

#[tokio::main]
async fn main() {

    let args = Args::parse();
    let port = args.port;

    println!("Using port {port}");

    let db = Arc::new(Db::new());
    let config = Arc::new(ServerConfig {
        role : if args.replicaof.is_some() {
            ServerRole::Follower
        } else {
            ServerRole::Leader
        },
        master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        master_repl_offset: 0,
        listener_port: args.port,
    });

    let (tx, rx) = broadcast::channel(20);
    std::mem::drop(rx);

    if config.role == ServerRole::Follower {
        let leader_addr = args.get_leader_addr().expect("replicaof not set correctly");
        let config_cloned = config.clone();
        let db_cloned = db.clone();
        let tx_cloned = tx.clone();
        let handler = MessageHandler::new(db_cloned, config_cloned, tx_cloned);
        let listener_port = config.listener_port;
        tokio::spawn(async move{
            replication_client::start_replication(listener_port, leader_addr, handler).await
                .unwrap_or_else(|error| eprintln!("replication: {:?}", error));
        });
    }

    server::start(config, db, tx).await.expect("running server failed");
}
