use std::sync::Arc;

use anyhow::{bail, Result};
use tokio::sync::broadcast::Sender;

use crate::{
    command_parser::{parse_command, Command},
    db::Db,
    message::Message,
};

use super::distribute_message;

pub struct ReplicationHandler {
    db: Arc<Db>,
    sender: Sender<Message>,
}

impl ReplicationHandler {
    pub fn new(db: Arc<Db>, sender: Sender<Message>) -> Self {
        Self { db, sender }
    }

    pub async fn handle(&mut self, message: Message) -> Result<Option<Message>> {
        let command = parse_command(message)?;
        match command {
            Command::Set {
                ref key,
                ref value,
                expire_time,
            } => {
                self.db.set(key.clone(), value.clone(), expire_time).await?;
                distribute_message(&self.sender, &command.clone().to_message());
                Ok(None)
            }
            Command::Replconf { name, value: _ } => {
                if name.to_uppercase() != "GETACK" {
                    bail!("Only GETACK implemented for repl");
                }

                Ok(Some(Command::get_replconf_command("ACK", 0)))
            }
            Command::Ping
            | Command::Echo(_)
            | Command::Get { .. }
            | Command::Info { .. }
            | Command::Psync => bail!("wrong command for replication {}", command.to_message()),
        }
    }

    pub fn check_ping_reply(message: &Message) -> Result<()> {
        match message {
            Message::BulkString(resp) | Message::SimpleString(resp)
                if resp.to_uppercase() == "PONG" =>
            {
                Ok(())
            }
            _ => bail!("wrong ping reply: {}", message),
        }
    }

    pub fn check_replconf_reply(message: &Message) -> Result<()> {
        match message {
            Message::BulkString(resp) | Message::SimpleString(resp)
                if resp.to_uppercase() == "OK" =>
            {
                Ok(())
            }
            _ => bail!("wrong replconf reply: {}", message),
        }
    }

    pub fn check_psync_reply(message: &Message) -> Result<()> {
        match message {
            Message::SimpleString(resp) if resp.to_uppercase().starts_with("FULLRESYNC") => Ok(()),
            _ => bail!("wrong psync reply: {}", message),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        sync::broadcast::{self, Receiver},
        time::timeout,
    };

    use crate::handler::test_functions::get_set_command;

    use super::*;

    fn create_handler_and_recx() -> (ReplicationHandler, Receiver<Message>) {
        let db = Arc::new(Db::new());
        let (tx, rx) = broadcast::channel(1);
        let handler = ReplicationHandler::new(db, tx);
        (handler, rx)
    }

    #[tokio::test]
    async fn test_set_does_broadcast() {
        let (mut handler, mut rx) = create_handler_and_recx();
        let (_, _, message_set) = get_set_command("key", "value");
        handler.handle(message_set.clone()).await.unwrap();

        match timeout(Duration::from_millis(10), rx.recv()).await {
            Ok(Ok(msg)) => assert_eq!(msg, message_set),
            Ok(Err(_)) => panic!("message not received"),
            Err(_) => panic!("nothing received"),
        }
    }

    #[tokio::test]
    async fn test_ackget_returns_message() -> Result<()> {
        let (mut handler, _rx) = create_handler_and_recx();
        let replmessage = Command::get_replconf_command("GETACK", "*");
        let expected_return = Some(Command::get_replconf_command("ACK", 0));

        assert_eq!(expected_return, handler.handle(replmessage).await?);

        Ok(())
    }
}
