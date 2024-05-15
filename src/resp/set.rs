use std::{collections::BTreeSet, ops::Deref};

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{frame::RespFrame, parse_aggregate_length, CRLF_LEN, DEFAULT_FRAME_SIZE};

#[derive(Debug, PartialEq, Clone)]
pub struct RespSet(Vec<RespFrame>);

impl RespEncode for RespSet {
    fn encode(self) -> Vec<u8> {
        let len = self.len();
        let mut result = Vec::with_capacity(len * DEFAULT_FRAME_SIZE);
        result.extend_from_slice(format!("~{}\r\n", len).as_bytes());
        self.0.into_iter().for_each(|frame| {
            result.extend_from_slice(&frame.encode());
        });
        result
    }
}

impl RespDecode for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Set(~), got {:?}",
                buf
            )));
        }

        let expect_length = RespSet::expect_length(buf)?;
        let data = buf.split_to(expect_length);

        let (end, frame_count) = parse_aggregate_length(&data, Self::PREFIX.as_bytes())?;
        if frame_count < 0 {
            return Err(RespError::InvalidFrameLength(frame_count));
        }

        let mut existed = BTreeSet::new();
        let cur_index = end + CRLF_LEN;
        let mut tmp_buf = BytesMut::from(&data[cur_index..]);
        for _ in 0..frame_count {
            let frame = RespFrame::decode(&mut tmp_buf)?;
            let encode = frame.encode();
            if !existed.contains(&encode) {
                existed.insert(encode);
            }
        }

        let mut set = vec![];
        for encode in existed {
            let mut buf = BytesMut::from(&encode[..]);
            set.push(RespFrame::decode(&mut buf)?);
        }
        Ok(RespSet::new(set))
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
        }
        Ok(cur_index)
    }
}

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

#[cfg(test)]
mod tests {
    use crate::resp::{bulk_string::BulkString, simple_string::SimpleString};

    use super::*;

    #[test]
    fn test_encode_set() {
        let frame: RespFrame = RespSet::new(vec![
            10.into(),
            BulkString::new(Some("hello")).into(),
            SimpleString::new("world").into(),
        ])
        .into();
        assert_eq!(frame.encode(), b"~3\r\n:10\r\n$5\r\nhello\r\n+world\r\n");
    }

    #[test]
    fn test_decode_resp_set() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"~3\r\n:10\r\n$5\r\nhello\r\n+world\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();

        let set = RespSet::new(vec![
            BulkString::new(Some("hello")).into(),
            SimpleString::new("world").into(),
            10.into(),
        ]);
        let frame1 = RespFrame::Set(set);
        assert_eq!(frame, frame1);

        buf.extend_from_slice(
            b"~6\r\n:10\r\n$5\r\nhello\r\n+world\r\n:10\r\n$5\r\nhello\r\n+world\r\n",
        );
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, frame1);
    }
}
