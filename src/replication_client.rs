use anyhow::{bail, Context, Result};
use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, ToSocketAddrs}};

use crate::{command_parser::Command, handler::replication::ReplicationHandler, message::Message, parser::parse_data};

pub async fn start_replication(listener_port: u16, leader_addr: impl ToSocketAddrs, mut handler: ReplicationHandler) -> Result<()>{
    let mut stream = TcpStream::connect(leader_addr).await?;

    send_command(Command::get_ping_command(), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    ReplicationHandler::check_ping_reply(&reply)?;

    send_command(Command::get_replconf_command("listening-port", listener_port), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    ReplicationHandler::check_replconf_reply(&reply).context("listening port")?;

    send_command(Command::get_replconf_command("capa", "psync2"), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    ReplicationHandler::check_replconf_reply(&reply).context("capa")?;

    send_command(Command::get_psync_command("?", -1), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    ReplicationHandler::check_psync_reply(&reply)?;

    let _rdb_file = get_reply_raw(&mut stream).await?;

    loop {
        let repl_message = get_reply(&mut stream).await?;
        handler.handle(repl_message).await?;
     }
}

async fn send_command(command: Message, stream: &mut TcpStream) -> Result<()> {
    stream.write_all(&command.to_data()).await?;
    Ok(())
}

async fn get_reply_raw(stream: &mut TcpStream) -> Result<BytesMut> {
    let mut buffer = BytesMut::with_capacity(1024);
    let n = stream.read_buf(&mut buffer).await?;
    if n == 0 {
        bail!("connection closed by leader");
    }

    Ok(buffer.split())
}

async fn get_reply(stream: &mut TcpStream) -> Result<Message> {
    let buffer = get_reply_raw(stream).await?;

    Ok(parse_data(buffer)?)
}
