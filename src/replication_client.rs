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
    let reply = read_from_leader(&mut stream).await?;
    ReplicationHandler::check_ping_reply(&reply)?;

    send_message(
        Command::get_replconf_command("listening-port", listener_port),
        &mut stream,
    )
    .await?;
    let reply = read_from_leader(&mut stream).await?;
    ReplicationHandler::check_replconf_reply(&reply).context("listening port")?;

    send_message(Command::get_replconf_command("capa", "psync2"), &mut stream).await?;
    let reply = read_from_leader(&mut stream).await?;
    ReplicationHandler::check_replconf_reply(&reply).context("capa")?;

    send_message(Command::get_psync_command("?", -1), &mut stream).await?;
    let reply = read_from_leader(&mut stream).await?;
    ReplicationHandler::check_psync_reply(&reply)?;

    let _rdb_file = read_from_leader_raw(&mut stream).await?;
    println!("rdb received");
    loop {
        let repl_message = read_from_leader(&mut stream).await?;
        println!("next one {}", repl_message);
        if let Some(reply) = handler.handle(repl_message).await? {
            println!("reply {}", reply);
            send_message(reply, &mut stream).await?;
        } else {
            println!("must not be handled");
        }
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

async fn read_from_leader(stream: &mut TcpStream) -> Result<Message> {
    let buffer = read_from_leader_raw(stream).await?;

    Ok(parse_data(buffer)?)
}
