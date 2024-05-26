use anyhow::{bail, Context, Result};

use crate::message::Message;

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo(Message),
    Set {
        key: Message,
        value: Message,
        expire_time: Option<i64>,
    },
    Get {
        key: Message,
    },
    Info {
        sections: Vec<Message>,
    },
    Replconf,
    Psync,
}

impl Command {
    pub fn to_message(&self) -> Message {
        let inner = match self {
            Command::Ping => vec![Message::BulkString("PING".to_string())],
            Command::Echo(message) => {
                vec![Message::BulkString("ECHO".to_string()), message.clone()]
            }
            Command::Get { key } => vec![Message::BulkString("GET".to_string()), key.clone()],
            Command::Set {
                key,
                value,
                expire_time,
            } => {
                let mut set_messages = vec![
                    Message::BulkString("SET".to_string()),
                    key.clone(),
                    value.clone(),
                ];
                if let Some(time) = expire_time {
                    set_messages.push(Message::BulkString("SET".to_string()));
                    set_messages.push(Message::BulkString(time.to_string()));
                }

                set_messages
            }
            Command::Replconf => unimplemented!(),
            Command::Psync => unimplemented!(),
            Command::Info { sections: _ } => unimplemented!(),
        };

        Message::Array(inner)
    }

    pub fn get_ping_command() -> Message {
        Message::Array(vec![Message::BulkString("PING".to_string())])
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
}

pub fn parse_command(message: Message) -> Result<Command> {
    match message {
        Message::Array(vec) if vec.len() > 0 => handle_array(vec),
        _ => bail!("unknown message {} for command", message),
    }
}

fn handle_array(vec: Vec<Message>) -> Result<Command> {
    let command_message = vec.first().context("at least one message must exist")?;
    if let Message::BulkString(command_string) = command_message {
        match command_string.to_uppercase().as_str() {
            "PING" => Ok(Command::Ping),
            "ECHO" => Ok(Command::Echo(vec[1].clone())),
            "SET" => {
                let key = vec[1].clone();
                let value = vec[2].clone();
                let expire_time = get_expire_time(&vec)?;
                Ok(Command::Set {
                    key,
                    value,
                    expire_time,
                })
            }
            "GET" => Ok(Command::Get {
                key: vec[1].clone(),
            }),
            "INFO" => match vec.get(1) {
                Some(ele) => Ok(Command::Info {
                    sections: vec![ele.clone()],
                }),
                None => Ok(Command::Info { sections: vec![] }),
            },
            "REPLCONF" => Ok(Command::Replconf),
            "PSYNC" => Ok(Command::Psync),
            _ => bail!("unknown command {}", command_string),
        }
    } else {
        bail!("unknown command type {}", command_message);
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn get_set_message(
        key: &str,
        value: &str,
        expire_time: Option<i64>,
    ) -> (Message, Message, Message) {
        let key = Message::BulkString(key.to_string());
        let value = Message::BulkString(value.to_string());

        let message_set = if let Some(time) = expire_time {
            Message::Array(vec![
                Message::BulkString("SET".to_string()),
                key.clone(),
                value.clone(),
                Message::BulkString("PX".to_string()),
                Message::BulkString(time.to_string()),
            ])
        } else {
            Message::Array(vec![
                Message::BulkString("SET".to_string()),
                key.clone(),
                value.clone(),
            ])
        };

        (key, value, message_set)
    }

    #[test]
    fn test_get_expire_time() {
        let expire_time = 100;
        if let (_, _, Message::Array(vec_messages)) =
            get_set_message("key", "val", Some(expire_time))
        {
            assert_eq!(Some(expire_time), get_expire_time(&vec_messages).unwrap());
        } else {
            unreachable!();
        }
    }

    fn assert_command(expected_command: Command, message: Message) {
        assert_eq!(expected_command, parse_command(message).unwrap())
    }

    #[test]
    fn test_ping_command() {
        let message = Message::Array(vec![Message::BulkString("ping".to_string())]);

        assert_command(Command::Ping, message);
    }

    #[test]
    fn test_echo_command() {
        let data = Message::BulkString("some data".to_string());
        let message = Message::Array(vec![Message::BulkString("Echo".to_string()), data.clone()]);

        assert_command(Command::Echo(data), message);
    }

    #[test]
    fn test_set_command() {
        let (key, value, message_set) = get_set_message("the_key", "the_value", None);

        assert_command(
            Command::Set {
                key,
                value,
                expire_time: None,
            },
            message_set,
        );

        let (key, value, message_set) = get_set_message("the_key", "the_value", Some(123));

        assert_command(
            Command::Set {
                key,
                value,
                expire_time: Some(123),
            },
            message_set,
        );
    }

    #[test]
    fn test_get_command() {
        let key = Message::BulkString("key1".to_string());
        let message_get = Message::Array(vec![Message::BulkString("GET".to_string()), key.clone()]);

        assert_command(Command::Get { key }, message_get);
    }
}
