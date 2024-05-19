use anyhow::Result;
use tokio::{io::AsyncWriteExt, net::{TcpStream, ToSocketAddrs}};

use crate::handler::MessageHandler;

pub async fn start_replication(leader_addr: impl ToSocketAddrs) -> Result<()>{
    let mut stream = TcpStream::connect(leader_addr).await?;
    let ping = MessageHandler::get_ping_command();
    stream.write_all(&ping.to_data()).await?;
    Ok(())
}
