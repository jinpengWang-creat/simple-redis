mod decode;
mod encode;

use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode {
    fn decode(self) -> Result<RespFrame, String>;
}
pub enum RespFrame {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    NullBulkString(RespNullBulkString),
    Array(RespArray),
    NullArray(RespNullArray),
    Null(RespNull),
    Boolean(bool),
    Double(f64),
    Map(RespMap),
    Set(RespSet),
}

pub struct SimpleString(String);
pub struct SimpleError(String);
pub struct BulkString(Vec<u8>);
pub struct RespArray(Vec<RespFrame>);
pub struct RespNull;
pub struct RespNullArray;
pub struct RespNullBulkString;

pub struct RespMap(HashMap<SimpleString, RespFrame>);

pub struct RespSet(HashSet<RespFrame>);

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SimpleString {
    pub fn new(str: impl Into<String>) -> Self {
        SimpleString(str.into())
    }
}

impl Deref for SimpleError {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespMap {
    type Target = HashMap<SimpleString, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespSet {
    type Target = HashSet<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
