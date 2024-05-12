use std::collections::HashMap;

use anyhow::{bail, Context, Result};

use crate::message::Message;

pub struct MessageHandler {
    storage: HashMap<Message, Message>,
}

impl MessageHandler {

    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
        }
    }

    pub async fn handle(&mut self, message: Message) -> Result<Message> {
        match message {
            // Message::SimpleString(the_string) if the_string.to_uppercase() == "PING" => {
            //     stream.write_all(Message::SimpleString("PONG".to_string())).await?;
            // }
            Message::Array(vec) if vec.len() > 0 => {
                self.handle_array(vec)
            }
            _ => bail!("don't know how to react to {}", message),
        }
    }

    fn handle_array(&mut self, vec: Vec<Message>) -> Result<Message> {
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
                } else if command_string == "SET" {
                    let key = vec[1].clone();
                    let value = vec[2].clone();
                    self.storage.insert(key, value);
                    Ok(Message::SimpleString("OK".to_string()))
                } else if command_string == "GET" {
                    let key = vec[1].clone();
                    match self.storage.get(&key){
                        Some(value) => Ok(value.clone()),
                        None => Ok(Message::Null)
                    }
                } else {
                    bail!("unknown command {}", command)
                }
            }
            _ => bail!("unknown command type {}", command),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn handle_test(message: Message) -> Message {
        let mut handler = MessageHandler::new();
        handler.handle(message).await.unwrap()
    }

    #[tokio::test]
    async fn test_ping() {
        let message = Message::Array(vec![
            Message::BulkString("ping".to_string()),
        ]);

        assert_eq!(Message::BulkString("PONG".to_string()), handle_test(message).await);
    }

    #[tokio::test]
    async fn test_echo() {
        let message = Message::Array(vec![
            Message::BulkString("Echo".to_string()),
            Message::BulkString("some data".to_string()),
        ]);

        assert_eq!(Message::BulkString("some data".to_string()), handle_test(message).await);
    }

    #[tokio::test]
    async fn test_get_no_value() {
        let message = Message::Array(vec![
            Message::BulkString("GET".to_string()),
            Message::BulkString("key1".to_string()),
        ]);

        assert_eq!(Message::Null, handle_test(message).await);
    }

    #[tokio::test]
    async fn test_set_and_get_value() {
        let mut handler = MessageHandler::new();
        let key = Message::BulkString("key1".to_string());
        let value = Message::BulkString("value1".to_string());

        let message_set = Message::Array(vec![
            Message::BulkString("SET".to_string()),
            key.clone(),
            value.clone(),
        ]);

        let result_set = handler.handle(message_set).await.unwrap();

        assert_eq!(Message::SimpleString("OK".to_string()), result_set);

        let message_get = Message::Array(vec![
            Message::BulkString("GET".to_string()),
            key,
        ]);

        let result_get = handler.handle(message_get).await.unwrap();

        assert_eq!(value, result_get);
    }
}
