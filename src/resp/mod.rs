mod decode;
mod encode;

use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use thiserror::Error;
const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = CRLF.len();
const AGGREGATE_FRAME_TYPE: [&[u8]; 4] = [b"$", b"*", b"%", b"~"];

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        find_crlf(buf, 1)
            .map(|end| end + CRLF_LEN)
            .ok_or(RespError::NotComplete)
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
    #[error("Invalid frame length: {0}")]
    InvalidFrameLength(isize),
    #[error("Invalid frame data: {0}")]
    InvalidFrameData(String),
    #[error("Invalid utf8 value: {0}")]
    InvalidUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Invalid int value: {0}")]
    InvalidIntError(#[from] std::num::ParseIntError),
    #[error("Invalid float value: {0}")]
    InvalidFloatError(#[from] std::num::ParseFloatError),
    #[error("Frame is not complete")]
    NotComplete,
}

#[derive(Debug, PartialEq)]
#[enum_dispatch(RespEncode)]
pub enum RespFrame {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    Array(RespArray),
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub struct BulkString(Option<Vec<u8>>);

impl BulkString {
    pub fn new(vec: Option<impl Into<Vec<u8>>>) -> Self {
        BulkString(vec.map(|v| v.into()))
    }
}

impl Deref for BulkString {
    type Target = Option<Vec<u8>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq)]
pub struct RespArray(Option<Vec<RespFrame>>);

impl RespArray {
    pub fn new(vec: Option<impl Into<Vec<RespFrame>>>) -> Self {
        RespArray(vec.map(|v| v.into()))
    }
}

impl Deref for RespArray {
    type Target = Option<Vec<RespFrame>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq)]
pub struct RespNull;

impl RespNull {
    pub fn new() -> Self {
        RespNull
    }
}

impl Default for RespNull {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug, PartialEq)]
pub struct RespNullArray;

impl RespNullArray {
    pub fn new() -> Self {
        RespNullArray
    }
}

impl Default for RespNullArray {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq)]
pub struct RespNullBulkString;

impl RespNullBulkString {
    pub fn new() -> Self {
        RespNullBulkString
    }
}

impl Default for RespNullBulkString {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug, PartialEq)]
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

impl Default for RespMap {
    fn default() -> Self {
        Self::new()
    }
}
impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, PartialEq)]
pub struct RespSet(Vec<RespFrame>);

impl RespSet {
    pub fn new(vec: impl Into<Vec<RespFrame>>) -> Self {
        RespSet(vec.into())
    }
}

impl Deref for RespSet {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn find_crlf(buf: &[u8], nth_crlf: usize) -> Option<usize> {
    let mut cur_times = 0;
    for i in 0..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            cur_times += 1;
        }
        if cur_times == nth_crlf {
            return Some(i);
        }
    }
    None
}
