use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::message::Message;

pub struct Db {
    storage: RwLock<HashMap<Message, Message>>,
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
            Some(m) => Some(m.clone()),
            None => None,
        }
    }

    pub async fn set(&self, key: Message, value: Message) {
        let mut map = self.storage.write().await;
        map.insert(key, value);
    }
}
