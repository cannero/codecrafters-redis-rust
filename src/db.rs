use std::{collections::HashMap, ops::Add};

use anyhow::{bail, Result};
use chrono::{prelude::*, TimeDelta};
use tokio::sync::RwLock;

use crate::message::Message;

pub struct Db {
    storage: RwLock<HashMap<Message, (Message, Option<DateTime<Utc>>)>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            storage: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, key: &Message) -> Option<Message> {
        let map = self.storage.read().await;
        match map.get(key) {
            Some((m, expire_date)) => {
                let now = Utc::now();
                if expire_date.is_none() || now <= expire_date.unwrap() {
                    Some(m.clone())
                } else {
                    // TODO: remove entry
                    Some(Message::NullBulkString)
                }
            }
            None => None,
        }
    }

    // expire time in milliseconds
    pub async fn set(
        &self,
        key: Message,
        value: Message,
        expire_milliseconds: Option<i64>,
    ) -> Result<()> {
        let mut map = self.storage.write().await;

        let expire_time = match expire_milliseconds {
            Some(millis) => {
                let timedelta = TimeDelta::try_milliseconds(millis);
                if let Some(delta) = timedelta {
                    Some(Utc::now().add(delta))
                } else {
                    bail!("timedelta cannot be constructed");
                }
            }
            None => None,
        };

        map.insert(key, (value, expire_time));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_expired_entry() {
        let db = Db::new();
        let key = Message::SimpleString("key".to_string());
        let value = Message::SimpleString("value".to_string());
        db.set(key.clone(), value, Some(-100)).await.unwrap();

        let val = db.get(&key).await.unwrap();

        assert_eq!(Message::NullBulkString, val);
    }
}
