use std::{ops::Deref, sync::Arc};

use dashmap::DashMap;

use crate::{RespFrame, RespMap, SimpleString};

#[derive(Debug, Clone)]
pub struct Backend(BackendInner);

#[derive(Debug, Clone)]
pub struct BackendInner {
    pub map: Arc<DashMap<String, RespFrame>>,
    pub hmap: Arc<DashMap<String, DashMap<String, RespFrame>>>,
}

impl Deref for Backend {
    type Target = BackendInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Backend {
    pub fn new() -> Self {
        Backend(BackendInner::new())
    }
}

impl Default for Backend {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendInner {
    fn new() -> Self {
        BackendInner {
            map: Arc::new(DashMap::new()),
            hmap: Arc::new(DashMap::new()),
        }
    }
}

impl Backend {
    pub fn get(&self, key: &str) -> Option<RespFrame> {
        self.map.get(key).map(|v| v.value().clone())
    }

    pub fn set(&self, key: String, value: RespFrame) {
        self.map.insert(key, value);
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<RespFrame> {
        self.hmap
            .get(key)
            .and_then(|v| v.get(field).map(|v| v.value().clone()))
    }

    pub fn hset(&self, key: String, field: String, value: RespFrame) {
        let hmap = self.hmap.entry(key).or_default();
        hmap.insert(field, value);
    }

    pub fn hgetall(&self, key: &str) -> Option<RespFrame> {
        self.hmap.get(key).map(|field_map| {
            let mut map = RespMap::new();
            field_map.iter().for_each(|entry| {
                map.insert(
                    SimpleString::new(entry.key().clone()),
                    entry.value().clone(),
                );
            });
            RespFrame::Map(map)
        })
    }
}
