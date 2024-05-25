use std::sync::Arc;

use anyhow::{bail, Result};
use tokio::sync::broadcast::Sender;

use crate::{command_parser::{parse_command, Command}, db::Db, message::Message, ServerConfig, ServerRole};

pub struct MessageHandler {
    db: Arc<Db>,
    state: Arc<ServerConfig>,
    sender: Sender<Message>,
    replication_client_ack: bool,
}

impl MessageHandler {

    pub fn new(db: Arc<Db>, state: Arc<ServerConfig>, sender: Sender<Message>) -> Self {
        Self {
            db,
            state,
            sender,
            replication_client_ack: false,
        }
    }

    pub fn replication_client_acknowleged(&self) -> bool {
        self.replication_client_ack
    }

    pub async fn handle(&mut self, message: Message) -> Result<Vec<Message>> {
        let command = parse_command(message)?;
        match command {
            Command::Ping => Ok(vec![Message::BulkString("PONG".to_string())]),
            Command::Echo(message) => Ok(vec![message]),
            Command::Get { key } => {
                match self.db.get(&key).await {
                        Some(value) => Ok(vec![value.clone()]),
                        None => Ok(vec![Message::Null])
                }
            }
            Command::Set { ref key, ref value, expire_time } => {
                self.db.set(key.clone(), value.clone(), expire_time).await?;
                let message = Message::SimpleString("OK".to_string());
                self.distribute_message(&command.clone().to_message());
                Ok(vec![message])
            }
            Command::Info { sections } => {
                if sections.len() != 1 ||
                    sections[0] != Message::BulkString("replication".to_string()){
                        bail!("unknown section type {:?}", sections);
                    }

                    self.build_replication_info()
            }
            Command::Replconf => // for now just respond with okay
                Ok(vec![Message::SimpleString("OK".to_string())]),

            Command::Psync => {
                self.replication_client_ack = true;
                Ok(vec![Message::SimpleString(format!("FULLRESYNC {} 0",
                                                      self.state.master_replid)),
                        Self::get_rdb_file()])

            }
        }
    }

    fn build_replication_info(&self) -> Result<Vec<Message>> {
        let role = match self.state.role {
            ServerRole::Leader => "master",
            ServerRole::Follower => "slave",
        };

        Ok(vec![Message::BulkString(format!("role:{}\nmaster_replid:{}\nmaster_repl_offset:{}",
                                       role,
                                       self.state.master_replid,
                                       self.state.master_repl_offset))])
    }

    fn distribute_message(&self, message: &Message){
        // A SendError may be returned when no receivers exist.
        // As they are only created when replication is running, this is no problem.
        _ = self.sender.send(message.clone());
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

    pub fn get_psync_command(master_replid: &str, master_offset: i64) -> Message {
        Message::Array(vec![
            Message::BulkString("PSYNC".to_string()),
            Message::BulkString(master_replid.to_string()),
            Message::BulkString(master_offset.to_string()),
        ])
    }

    pub fn check_ping_reply(message: &Message) -> Result<()> {
        match message {
            Message::BulkString(resp) |
            Message::SimpleString(resp) if resp.to_uppercase() == "PONG" => Ok(()),
            _ => bail!("wrong ping reply: {}", message),
        }
    }

    pub fn check_replconf_reply(message: &Message) -> Result<()> {
        match message {
            Message::BulkString(resp) |
            Message::SimpleString(resp) if resp.to_uppercase() == "OK" => Ok(()),
            _ => bail!("wrong replconf reply: {}", message),
        }
    }

    pub fn check_psync_reply(message: &Message) -> Result<()> {
        match message {
            Message::SimpleString(resp) if resp.to_uppercase()
                .starts_with("FULLRESYNC") => Ok(()),
            _ => bail!("wrong psync reply: {}", message),
        }
    }

    fn get_rdb_file() -> Message {
        let hex_string = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let bytes = (0..hex_string.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex_string[i..i + 2], 16)
                 .expect("hex_string is invalid"))
            .collect::<Vec<_>>();
        Message::RdbFile(bytes)
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast::{self, Receiver};

    use super::*;

    fn create_handler() -> MessageHandler {
        let (handler, _) = create_handler_and_recx();
        handler
    }

    fn create_handler_and_recx() -> (MessageHandler, Receiver<Message>) {
        let db = Arc::new(Db::new());
        let state = Arc::new(ServerConfig {
            role: ServerRole::Leader,
            master_replid: "2310921903".to_string(),
            master_repl_offset: 0,
            listener_port: 1234,
        });
        let (tx, rx) = broadcast::channel(1);

        let handler = MessageHandler::new(db, state, tx);
        (handler, rx)
    }

    fn get_set_command(key: &str, value: &str) -> (Message, Message, Message) {
        let key = Message::BulkString(key.to_string());
        let value = Message::BulkString(value.to_string());

        let message_set = Message::Array(vec![
            Message::BulkString("SET".to_string()),
            key.clone(),
            value.clone(),
        ]);

        (key, value, message_set)
    }

    async fn handle_test(message: Message) -> Message {
        let mut handler = create_handler();
        handler.handle(message).await.unwrap()[0].clone()
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
        let mut handler = create_handler();
        let key = "key1";
        let value = "value1";
        let (key, value, message_set) = get_set_command(key, value);

        let result_set = handler.handle(message_set).await.unwrap();

        assert_eq!(Message::SimpleString("OK".to_string()), result_set[0]);

        let message_get = Message::Array(vec![
            Message::BulkString("GET".to_string()),
            key,
        ]);

        let result_get = handler.handle(message_get).await.unwrap();

        assert_eq!(value, result_get[0]);
    }


    #[tokio::test]
    async fn test_info_replication() {
        let mut handler = create_handler();
        let messages = vec![
            Message::BulkString("INFO".to_string()),
            Message::BulkString("replication".to_string()),
        ];

        if let Message::BulkString(result) = handler.handle_array(messages).await.unwrap()[0].clone() {
            assert!(result.contains("master_replid"));
        } else {
            assert!(false, "Info command should return a bulk string");
        }
    }

    #[tokio::test]
    async fn test_handle_psync() {
        let mut handler = create_handler();
        let result = handler.handle(MessageHandler::get_psync_command("id", 123)).await.unwrap();
        assert_eq!(2, result.len());
    }

    #[tokio::test]
    async fn test_broadcast_without_receiver_does_not_fail() {
        let (mut handler, rx) = create_handler_and_recx();
        std::mem::drop(rx);
        let (_, _, set_command) = get_set_command("keyyyy", "val");

        let result = handler.handle(set_command).await.unwrap();
        assert_eq!(Message::SimpleString("OK".to_string()), result[0]);
    }

    #[tokio::test]
    async fn test_broadcast_receive_message() {
        let (mut handler, mut rx) = create_handler_and_recx();
        let (_, _, set_command) = get_set_command("keyyyy", "val");

        let result = handler.handle(set_command.clone()).await.unwrap();
        assert_eq!(Message::SimpleString("OK".to_string()), result[0]);

        let message_recv = rx.recv().await.unwrap();
        assert_eq!(set_command, message_recv);
    }
}
