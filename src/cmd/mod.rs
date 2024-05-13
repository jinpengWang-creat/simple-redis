mod hmap;
mod map;
use crate::{RespError, RespFrame};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0}")]
    REspError(#[from] RespError),
}

pub trait CommandExecutor {
    fn execute(self) -> RespFrame;
}
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
}

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Self {
        Get { key }
    }
}

impl TryFrom<Vec<u8>> for Get {
    type Error = CommandError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Get::new(String::from_utf8(value).map_err(|e| {
            CommandError::InvalidArgument(format!("{:?}", e))
        })?))
    }
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGet {
    key: String,
    field: String,
}

#[derive(Debug)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,
}

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;

    fn try_from(_value: RespFrame) -> Result<Self, Self::Error> {
        todo!()
    }
}