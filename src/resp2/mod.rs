#![allow(dead_code)]
mod parser;
pub use crate::CRLF;
use crate::{RespError, RespFrame};
use bytes::BytesMut;
use parser::parse_frame_length;

pub trait RespDecodeV2: Sized {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}

impl RespDecodeV2 for RespFrame {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let len = Self::expect_length(buf)?;
        let buff = buf.split_to(len);
        let mut input = &buff[..];
        parser::parse_frame(&mut input).map_err(|_| RespError::ParseError)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        parse_frame_length(buf)
    }
}
