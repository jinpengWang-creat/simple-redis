mod decode;
mod encode;

use std::{
    collections::{BTreeMap, HashSet},
    ops::{Deref, DerefMut},
};

use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode {
    fn decode(self) -> Result<RespFrame, String>;
}
#[derive(Debug)]
#[enum_dispatch(RespEncode)]
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

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SimpleString(String);

impl SimpleString {
    pub fn new(str: impl Into<String>) -> Self {
        SimpleString(str.into())
    }
}

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct SimpleError(String);

impl SimpleError {
    pub fn new(str: impl Into<String>) -> Self {
        SimpleError(str.into())
    }
}

impl Deref for SimpleError {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct BulkString(Vec<u8>);

impl BulkString {
    pub fn new(vec: impl Into<Vec<u8>>) -> Self {
        BulkString(vec.into())
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct RespArray(Vec<RespFrame>);

impl RespArray {
    pub fn new(vec: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(vec.into())
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct RespNull;

impl RespNull {
    pub fn new() -> Self {
        RespNull
    }
}

#[derive(Debug)]
pub struct RespNullArray;

impl RespNullArray {
    pub fn new() -> Self {
        RespNullArray
    }
}

#[derive(Debug)]
pub struct RespNullBulkString;

impl RespNullBulkString {
    pub fn new() -> Self {
        RespNullBulkString
    }
}

#[derive(Debug)]
pub struct RespMap(BTreeMap<SimpleString, RespFrame>);

impl RespMap {
    pub fn new() -> Self {
        RespMap(BTreeMap::new())
    }
}

impl Deref for RespMap {
    type Target = BTreeMap<SimpleString, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct RespSet(HashSet<RespFrame>);

impl Deref for RespSet {
    type Target = HashSet<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
