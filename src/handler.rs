use tokio::sync::broadcast::Sender;

use crate::message::Message;

pub mod client_server;
pub mod replication;

pub fn distribute_message(sender: &Sender<Message>, message: &Message) {
    // A SendError may be returned when no receivers exist.
    // As they are only created when replication is running, this is no problem.
    _ = sender.send(message.clone());
}

#[cfg(test)]
mod test_functions {
    use crate::message::Message;
    pub fn get_set_command(key: &str, value: &str) -> (Message, Message, Message) {
        let key = Message::BulkString(key.to_string());
        let value = Message::BulkString(value.to_string());

        let message_set = Message::Array(vec![
            Message::BulkString("SET".to_string()),
            key.clone(),
            value.clone(),
        ]);

        (key, value, message_set)
    }
}
