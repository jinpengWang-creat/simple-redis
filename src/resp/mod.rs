mod array;
mod bool;
mod bulk_string;
mod double;
mod frame;
mod integer;
mod map;
mod null;
mod set;
mod simple_error;
mod simple_string;

use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use thiserror::Error;

pub use self::{
    array::RespArray, bulk_string::BulkString, frame::RespFrame, map::RespMap, null::RespNull,
    set::RespSet, simple_error::SimpleError, simple_string::SimpleString,
};

pub use crate::CRLF;
const CRLF_LEN: usize = CRLF.len();
const AGGREGATE_FRAME_TYPE: [&[u8]; 4] = [b"$", b"*", b"%", b"~"];
const DEFAULT_FRAME_SIZE: usize = 16;

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
    #[error("Parse error")]
    ParseError,
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

fn parse_aggregate_length(buf: &[u8], prefix: &[u8]) -> Result<(usize, isize), RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }
    if !AGGREGATE_FRAME_TYPE.contains(&prefix) {
        return Err(RespError::InvalidFrameType(format!(
            "frame type {} is not a aggregate type",
            String::from_utf8_lossy(prefix)
        )));
    }
    let end = find_crlf(buf, 1).ok_or(RespError::NotComplete)?;
    Ok((
        end,
        String::from_utf8((&buf[prefix.len()..end]).into())?.parse()?,
    ))
}
