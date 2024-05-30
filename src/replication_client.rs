use anyhow::{bail, Context, Result};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
};

use crate::{
    command_parser::Command, handler::replication::ReplicationHandler, message::Message,
    parser::parse_data,
};

pub async fn start_replication(
    listener_port: u16,
    leader_addr: impl ToSocketAddrs,
    mut handler: ReplicationHandler,
) -> Result<()> {
    let mut stream = TcpStream::connect(leader_addr).await?;

    send_message(Command::get_ping_command(), &mut stream).await?;
    let reply = get_reply(&mut stream).await.context("leader ping")?;
    ReplicationHandler::check_ping_reply(&reply)?;

    send_message(
        Command::get_replconf_command("listening-port", listener_port),
        &mut stream,
    )
    .await?;
    let reply = get_reply(&mut stream).await.context("replconf port")?;
    ReplicationHandler::check_replconf_reply(&reply).context("replconf listening port")?;

    send_message(Command::get_replconf_command("capa", "psync2"), &mut stream).await?;
    let reply = get_reply(&mut stream).await.context("replconf capa")?;
    ReplicationHandler::check_replconf_reply(&reply).context("capa")?;

    send_message(Command::get_psync_command("?", -1), &mut stream).await?;
    let replies = read_from_leader(&mut stream).await.context("psync")?;
    ReplicationHandler::check_psync_reply(&replies[0])?;

    if replies.len() == 1 {
        // the reply to psync did not contain the rdb file, read it separately
        let _rdb_file = read_from_leader(&mut stream)
            .await
            .context("replication rdb file")?;
    } else if replies.len() > 2 {
        handle_messages(&replies[2..], &mut stream, &mut handler).await?;
    }

    loop {
        let repl_messages = read_from_leader(&mut stream).await?;
        handle_messages(&repl_messages, &mut stream, &mut handler).await?;
    }
}

async fn send_message(message: Message, stream: &mut TcpStream) -> Result<()> {
    stream.write_all(&message.to_data()).await?;
    Ok(())
}

async fn read_from_leader_raw(stream: &mut TcpStream) -> Result<BytesMut> {
    let mut buffer = BytesMut::with_capacity(1024);
    let n = stream.read_buf(&mut buffer).await?;
    if n == 0 {
        bail!("connection closed by leader");
    }

    Ok(buffer.split())
}

async fn read_from_leader(stream: &mut TcpStream) -> Result<Vec<Message>> {
    let buffer = read_from_leader_raw(stream).await?;

    Ok(parse_data(buffer)?)
}

async fn get_reply(stream: &mut TcpStream) -> Result<Message> {
    let mut res = read_from_leader(stream).await?;
    if res.len() != 1 {
        bail!("Exactly one reply expected");
    }

    Ok(res.swap_remove(0))
}

async fn handle_messages(repl_messages: &[Message], stream: &mut TcpStream, handler: &mut ReplicationHandler) -> Result<()> {
    for message in repl_messages {
        if let Some(reply) = handler.handle(message).await? {
            send_message(reply, stream).await?;
        }
    }

    Ok(())
}
