use anyhow::Result;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use bytes::BytesMut;

use crate::{handler::handle, parser::parse_data};

mod handler;
mod message;
mod parser;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    handle_connection(stream).await.unwrap_or_else(|error| eprintln!("{:?}", error));
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    let mut buffer = BytesMut::with_capacity(1024);

    loop {
        let n = stream.read_buf(&mut buffer).await?;

        if n == 0 {
            println!("Connection closed by client");
            return Ok(()); 
        }

        let message = parse_data(buffer.split())?;

        println!("Received from client: {}", message);

        let response = handle(message).await?;
        println!("Responding: {}", response);
        stream.write_all(&response.to_data()).await?;
//        write!(stream, "+PONG\r\n").unwrap();
    }
}
