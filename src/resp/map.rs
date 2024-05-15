use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{
    frame::RespFrame, parse_aggregate_length, simple_string::SimpleString, CRLF_LEN,
    DEFAULT_FRAME_SIZE,
};

#[derive(Debug, PartialEq, Clone)]
pub struct RespMap(BTreeMap<SimpleString, RespFrame>);

impl RespEncode for RespMap {
    fn encode(self) -> Vec<u8> {
        let len = self.len();
        let mut result = Vec::with_capacity(len * DEFAULT_FRAME_SIZE);
        result.extend_from_slice(format!("%{}\r\n", len).as_bytes());
        self.0.into_iter().for_each(|(key, frame)| {
            result.extend_from_slice(&key.encode());
            result.extend_from_slice(&frame.encode());
        });
        result
    }
}

impl RespDecode for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Map(%), got {:?}",
                buf
            )));
        }

        let expect_length = RespMap::expect_length(buf)?;
        let data = buf.split_to(expect_length);

        let (end, frame_count) = parse_aggregate_length(&data, Self::PREFIX.as_bytes())?;
        if frame_count < 0 {
            return Err(RespError::InvalidFrameLength(frame_count));
        }

        let mut map = RespMap::new();
        let mut tmp_buf = BytesMut::from(&data[end + CRLF_LEN..]);
        for _ in 0..frame_count {
            map.insert(
                SimpleString::decode(&mut tmp_buf)?,
                RespFrame::decode(&mut tmp_buf)?,
            );
        }
        Ok(map)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, length) = parse_aggregate_length(buf, Self::PREFIX.as_bytes())?;
        if length <= 0 {
            return Ok(end + CRLF_LEN);
        }
        let mut cur_index = end + CRLF_LEN;
        for _ in 0..length {
            let length = RespFrame::expect_length(&buf[cur_index..])?;
            cur_index += length;
            let length = RespFrame::expect_length(&buf[cur_index..])?;
            cur_index += length;
        }
        Ok(cur_index)
    }
}

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

#[cfg(test)]
mod tests {
    use crate::resp::bulk_string::BulkString;

    use super::*;
    #[test]
    fn test_encode_map() {
        let mut map = RespMap::new();
        map.insert(
            SimpleString::new("hello"),
            SimpleString::new("world").into(),
        );
        map.insert(
            SimpleString::new("foo"),
            BulkString::new(Some("bar")).into(),
        );
        let frame: RespFrame = map.into();
        assert_eq!(
            frame.encode(),
            b"%2\r\n+foo\r\n$3\r\nbar\r\n+hello\r\n+world\r\n"
        );
    }

    #[test]
    fn test_decode_resp_map() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"%2\r\n+foo\r\n$3\r\nbar\r\n+hello\r\n+world\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();

        let mut map = RespMap::new();
        map.insert(
            SimpleString::new("hello"),
            SimpleString::new("world").into(),
        );
        map.insert(
            SimpleString::new("foo"),
            BulkString::new(Some("bar")).into(),
        );
        let frame1 = RespFrame::Map(map);
        assert_eq!(frame, frame1);
    }
}
