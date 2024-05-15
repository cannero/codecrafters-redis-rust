use std::{env, sync::Arc};

use anyhow::{bail, Result};
use db::Db;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use bytes::BytesMut;

use crate::{handler::MessageHandler, parser::parse_data};

mod db;
mod handler;
mod message;
mod parser;

#[tokio::main]
async fn main() {

    let port = parse_port().unwrap().unwrap_or(6379);

    let db = Arc::new(Db::new());
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let db_cloned = db.clone();
                tokio::spawn(async move {
                    handle_connection(stream, db_cloned).await.unwrap_or_else(|error| eprintln!("{:?}", error));
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn parse_port() -> Result<Option<u16>> {
    let args = env::args().collect::<Vec<_>>();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--port" {
            if i == args.len() -1 {
                bail!("--port without number");
            }

            match args[i+1].parse::<u16>() {
                Ok(port) => return Ok(Some(port)),
                Err(err) => bail!(err),
            }
        }

        i += 1;
    }

    Ok(None)
}

async fn handle_connection(mut stream: TcpStream, db: Arc<Db>) -> Result<()> {
    let mut buffer = BytesMut::with_capacity(1024);
    let handler = MessageHandler::new(db);

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
