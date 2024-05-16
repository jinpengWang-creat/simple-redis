use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use dashmap::DashMap;

use crate::{BulkString, RespArray, RespFrame};

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

    pub fn hset(&self, key: String, field: String, value: RespFrame) -> RespFrame {
        let hmap = self.hmap.entry(key).or_default();
        let ret = hmap.insert(field, value);
        match ret {
            Some(_) => RespFrame::Integer(0),
            None => RespFrame::Integer(1),
        }
    }

    pub fn hgetall(&self, key: &str) -> Option<RespFrame> {
        self.hmap.get(key).map(|field_map| {
            let mut map = BTreeMap::new();
            field_map.iter().for_each(|entry| {
                map.insert(entry.key().clone(), entry.value().clone());
            });
            let mut vec = Vec::with_capacity(map.len() * 2);
            map.into_iter().for_each(|(key, value)| {
                vec.push(BulkString::new(Some(key)).into());
                vec.push(value);
            });
            RespFrame::Array(RespArray::new(Some(vec)))
        })
    }
}
