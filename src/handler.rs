use anyhow::{bail, Context, Result};

use crate::message::Message;

pub async fn handle(message: Message) -> Result<Message> {
    match message {
        // Message::SimpleString(the_string) if the_string.to_uppercase() == "PING" => {
        //     stream.write_all(Message::SimpleString("PONG".to_string())).await?;
        // }
        Message::Array(vec) if vec.len() > 0 => {
            handle_array(vec)
        }
        _ => bail!("don't know how to react to {}", message),
    }
}

fn handle_array(vec: Vec<Message>) -> Result<Message> {
    let command = vec.first().context("at least one message must exist")?;
    match command {
        Message::BulkString(command_string) => {
            let command_string = command_string.to_uppercase();
            if command_string == "PING" {
                //Ok(Message::Array(vec![
                    Ok(Message::BulkString("PONG".to_string()))
                //]))
            } else if command_string == "ECHO" {
                Ok(vec[1].clone())
            } else {
                bail!("unknown command {}", command)
            }
        }
        _ => bail!("unknown command type {}", command),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping() {
        let message = Message::Array(vec![
            Message::BulkString("ping".to_string()),
        ]);

        assert_eq!(Message::BulkString("PONG".to_string()), handle(message).await.unwrap());
    }

    #[tokio::test]
    async fn test_echo() {
        let message = Message::Array(vec![
            Message::BulkString("Echo".to_string()),
            Message::BulkString("some data".to_string()),
        ]);

        assert_eq!(Message::BulkString("some data".to_string()), handle(message).await.unwrap());
    }
}
