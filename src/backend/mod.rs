use std::{ops::Deref, sync::Arc};

use dashmap::DashMap;

use crate::RespFrame;

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

impl BackendInner {
    fn new() -> Self {
        BackendInner {
            map: Arc::new(DashMap::new()),
            hmap: Arc::new(DashMap::new()),
        }
    }
}
