use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, ToSocketAddrs}};

use crate::{handler::MessageHandler, message::Message, parser::parse_data, ServerConfig};

pub async fn start_replication(server_state: Arc<ServerConfig>, leader_addr: impl ToSocketAddrs) -> Result<()>{
    let mut stream = TcpStream::connect(leader_addr).await?;

    send_command(MessageHandler::get_ping_command(), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    MessageHandler::check_ping_reply(&reply)?;

    send_command(MessageHandler::get_replconf_command("listening-port", server_state.listener_port), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    MessageHandler::check_replconf_reply(&reply).context("listening port")?;

    send_command(MessageHandler::get_replconf_command("capa", "psync2"), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    MessageHandler::check_replconf_reply(&reply).context("capa")?;

    send_command(MessageHandler::get_psync_command("?", -1), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    MessageHandler::check_psync_reply(&reply)?;

    //TODO: get rdb file

    loop {
        let repl_message = get_reply(&mut stream).await?;
        println!("repl: {repl_message}");
    }
}

async fn send_command(command: Message, stream: &mut TcpStream) -> Result<()> {
    stream.write_all(&command.to_data()).await?;
    Ok(())
}

async fn get_reply(stream: &mut TcpStream) -> Result<Message> {
    let mut buffer = BytesMut::with_capacity(1024);
    let n = stream.read_buf(&mut buffer).await?;
    if n == 0 {
        bail!("connection closed by leader");
    }

    Ok(parse_data(buffer.split())?)
}
