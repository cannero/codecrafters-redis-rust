use std::sync::Arc;

use anyhow::{bail, Result};
use clap::Parser;
use db::Db;
use tokio::{
    net::ToSocketAddrs,
    sync::{broadcast, RwLock},
};

use crate::handler::replication::ReplicationHandler;

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
            None => bail!("replicaof not set"),
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
    replication_clients: RwLock<u16>,
}

impl ServerConfig {
    pub fn new(role: ServerRole, listener_port: u16) -> Self {
        Self {
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            listener_port,
            replication_clients: RwLock::new(0),
        }
    }

    pub async fn add_replication_client(&self) {
        let mut count = self.replication_clients.write().await;
        *count += 1;
    }

    pub async fn remove_replication_client(&self) {
        let mut count = self.replication_clients.write().await;
        assert!(*count > 1, "remove non-existing client");
        *count -= 1;
    }

    pub async fn active_replication_clients(&self) -> u16 {
        let count = self.replication_clients.read().await;
        *count
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let port = args.port;
    let role = if args.replicaof.is_some() {
        ServerRole::Follower
    } else {
        ServerRole::Leader
    };

    println!("Using port {port}");

    let db = Arc::new(Db::new());
    let config = Arc::new(ServerConfig::new(role, args.port));

    let (tx, rx) = broadcast::channel(20);
    std::mem::drop(rx);

    if config.role == ServerRole::Follower {
        let leader_addr = args.get_leader_addr().expect("replicaof not set correctly");
        let db_cloned = db.clone();
        let tx_cloned = tx.clone();
        let handler = ReplicationHandler::new(db_cloned, tx_cloned);
        let listener_port = config.listener_port;
        tokio::spawn(async move {
            replication_client::start_replication(listener_port, leader_addr, handler)
                .await
                .unwrap_or_else(|error| eprintln!("replication: {:?}", error));
        });
    }

    server::start(config, db, tx)
        .await
        .expect("running server failed");
}
