use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::{db::Db, message::Message, ServerRole, ServerState};

pub struct MessageHandler {
    db: Arc<Db>,
    state: Arc<ServerState>,
}

fn get_expire_time(messages: &Vec<Message>) -> Result<Option<i64>> {
    match messages.get(3) {
        Some(_) => {
            let time = messages[4].clone();
            match time {
                Message::BulkString(value) => Ok(Some(value.parse::<i64>().unwrap())),
                m => bail!("unknown message for expire_time {}", m),
            }
        }
        None => Ok(None),
    }
}

impl MessageHandler {

    pub fn new(db: Arc<Db>, state: Arc<ServerState>) -> Self {
        Self {
            db,
            state,
        }
    }

    pub async fn handle(&self, message: Message) -> Result<Message> {
        match message {
            Message::Array(vec) if vec.len() > 0 => {
                self.handle_array(vec).await
            }
            _ => bail!("don't know how to react to {}", message),
        }
    }

    async fn handle_array(&self, vec: Vec<Message>) -> Result<Message> {
        let command = vec.first().context("at least one message must exist")?;
        match command {
            Message::BulkString(command_string) => {
                let command_string = command_string.to_uppercase();
                if command_string == "PING" {
                    Ok(Message::BulkString("PONG".to_string()))

                } else if command_string == "ECHO" {
                    Ok(vec[1].clone())

                } else if command_string == "SET" {
                    let key = vec[1].clone();
                    let value = vec[2].clone();
                    let expire_time = get_expire_time(&vec)?;
                    self.db.set(key, value, expire_time).await?;
                    Ok(Message::SimpleString("OK".to_string()))

                } else if command_string == "GET" {
                    let key = vec[1].clone();
                    match self.db.get(&key).await {
                        Some(value) => Ok(value.clone()),
                        None => Ok(Message::Null)
                    }

                } else if command_string == "INFO" {
                    let info_type = vec[1].clone();
                    if info_type != Message::BulkString("replication".to_string()){
                        bail!("unknown info type {}", info_type);
                    }

                    self.build_replication_info()

                } else if command_string == "REPLCONF" {
                    // for now just respond with ok
                    Ok(Message::SimpleString("OK".to_string()))

                } else {
                    bail!("unknown command {}", command)
                }
            }
            _ => bail!("unknown command type {}", command),
        }
    }

    fn build_replication_info(&self) -> Result<Message> {
        let role = match self.state.role {
            ServerRole::Leader => "master",
            ServerRole::Follower => "slave",
        };

        Ok(Message::BulkString(format!("role:{}\nmaster_replid:{}\nmaster_repl_offset:{}",
                                       role,
                                       self.state.master_replid,
                                       self.state.master_repl_offset)))
    }

    pub fn get_ping_command() -> Message {
        Message::Array(vec![
            Message::BulkString("PING".to_string()),
        ])
    }

    pub fn get_replconf_command<T1: ToString, T2: ToString>(name: T1, value: T2) -> Message {
        Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString(name.to_string()),
            Message::BulkString(value.to_string()),
        ])
    }

    pub fn is_valid_return_for_ping(message: &Message) -> bool {
        match message {
            Message::BulkString(resp) |
            Message::SimpleString(resp) if resp.to_uppercase() == "PONG" => true,
            _ => false,
        }
    }

    pub fn is_valid_return_for_replconf(message: &Message) -> bool {
        match message {
            Message::BulkString(resp) |
            Message::SimpleString(resp) if resp.to_uppercase() == "OK" => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_handler() -> MessageHandler {
        let db = Arc::new(Db::new());
        let state = Arc::new(ServerState {
            role: ServerRole::Leader,
            master_replid: "2310921903".to_string(),
            master_repl_offset: 0,
            listener_port: 1234,
        });
        let handler = MessageHandler::new(db, state);
        handler
    }

    async fn handle_test(message: Message) -> Message {
        let handler = create_handler();
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
        let handler = create_handler();
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

    #[test]
    fn test_get_expire_time() {
        let messages = vec![
            Message::BulkString("SET".to_string()),
            Message::BulkString("key".to_string()),
            Message::BulkString("value".to_string()),
            Message::BulkString("PX".to_string()),
            Message::BulkString("100".to_string()),
        ];

        assert_eq!(Some(100), get_expire_time(&messages).unwrap());
    }

    #[tokio::test]
    async fn test_info_replication() {
        let handler = create_handler();
        let messages = vec![
            Message::BulkString("INFO".to_string()),
            Message::BulkString("replication".to_string()),
        ];

        if let Message::BulkString(result) = handler.handle_array(messages).await.unwrap() {
            assert!(result.contains("master_replid"));
        } else {
            assert!(false, "Info command should return a bulk string");
        }
    }
}
