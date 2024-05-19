use anyhow::{bail, Result};
use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::{handler::MessageHandler, message::Message, parser::parse_data};

pub async fn start_replication((leader_ip, leader_port): (String, u16)) -> Result<()>{
    let mut stream = TcpStream::connect((leader_ip, leader_port)).await?;

    send_command(MessageHandler::get_ping_command(), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    if !MessageHandler::is_valid_return_for_ping(&reply){
        bail!("Unknown reply for ping: {}", reply);
    }

    send_command(MessageHandler::get_replconf_command("listening-port", leader_port), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    if !MessageHandler::is_valid_return_for_replconf(&reply){
        bail!("Unknown reply for listening-port: {}", reply);
    }

    send_command(MessageHandler::get_replconf_command("capa", "psync2"), &mut stream).await?;
    let reply = get_reply(&mut stream).await?;
    if !MessageHandler::is_valid_return_for_replconf(&reply){
        bail!("Unknown reply for capa: {}", reply);
    }

    Ok(())
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
