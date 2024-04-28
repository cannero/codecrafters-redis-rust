use std::io::{prelude::*, Error};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move ||{
                    handle_connection(stream).unwrap_or_else(|error| eprintln!("{:?}", error));
                });
                //write!(stream, "+PONG\r\n").unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) -> Result<(), Error> {
    let mut buffer = [0; 1024];

    loop {
        // Read data
        let n = stream.read(&mut buffer)?;

        if n == 0 {
            // Connection closed by client
            println!("Connection closed by client");
            return Ok(()); 
        }

        println!("Received from client: {}", String::from_utf8_lossy(&buffer[..n]));

        write!(stream, "+PONG\r\n").unwrap();
    }
}
