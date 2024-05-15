use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use tracing::info;

use crate::{RespDecode, RespError};

use super::{
    array::RespArray, bulk_string::BulkString, map::RespMap, null::RespNull, set::RespSet,
    simple_error::SimpleError, simple_string::SimpleString,
};

#[derive(Debug, PartialEq, Clone)]
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

impl RespDecode for RespFrame {
    const PREFIX: &'static str = "";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        info!("if iter peek: {:?}", iter.peek());
        match iter.peek() {
            Some(b'+') => Ok(SimpleString::decode(buf)?.into()),
            Some(b'-') => Ok(SimpleError::decode(buf)?.into()),
            Some(b':') => Ok(i64::decode(buf)?.into()),
            Some(b'#') => Ok(bool::decode(buf)?.into()),
            Some(b'$') => Ok(BulkString::decode(buf)?.into()),
            Some(b'*') => Ok(RespArray::decode(buf)?.into()),
            Some(b'_') => Ok(RespNull::decode(buf)?.into()),
            Some(b',') => Ok(f64::decode(buf)?.into()),
            Some(b'%') => Ok(RespMap::decode(buf)?.into()),
            Some(b'~') => Ok(RespSet::decode(buf)?.into()),
            Some(val) => Err(RespError::InvalidFrameType(format!(
                "unknown frame type: {:?}",
                val
            ))),
            None => Err(RespError::NotComplete),
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => SimpleString::expect_length(buf),
            Some(b'-') => SimpleError::expect_length(buf),
            Some(b':') => i64::expect_length(buf),
            Some(b'#') => bool::expect_length(buf),
            Some(b'$') => BulkString::expect_length(buf),
            Some(b'*') => RespArray::expect_length(buf),
            Some(b'_') => RespNull::expect_length(buf),
            Some(b',') => f64::expect_length(buf),
            Some(b'%') => RespMap::expect_length(buf),
            Some(b'~') => RespSet::expect_length(buf),
            _ => Err(RespError::InvalidFrameType(format!(
                "unknown frame type: {:?}",
                buf
            ))),
        }
    }
}
