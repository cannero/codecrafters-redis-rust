use std::sync::Arc;

use anyhow::Result;
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

    let db = Arc::new(Db::new());
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

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
