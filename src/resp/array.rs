use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{frame::RespFrame, parse_aggregate_length, CRLF_LEN, DEFAULT_FRAME_SIZE};

#[derive(Debug, PartialEq, Clone)]
pub struct RespArray(pub(crate) Option<Vec<RespFrame>>);

impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let array = self.0.map(|array| {
            let len = array.len();
            let mut result = Vec::with_capacity(len * DEFAULT_FRAME_SIZE);
            result.extend_from_slice(format!("*{}\r\n", len).as_bytes());
            array
                .into_iter()
                .for_each(|frame| result.extend_from_slice(&frame.encode()));
            result
        });
        array.unwrap_or(b"*-1\r\n".to_vec())
    }
}
impl RespDecode for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Array(*), got {:?}",
                buf
            )));
        }

        let expect_length = RespArray::expect_length(buf)?;
        let data = buf.split_to(expect_length);

        let (end, frame_count) = parse_aggregate_length(&data, Self::PREFIX.as_bytes())?;
        if frame_count < -1 {
            return Err(RespError::InvalidFrameLength(frame_count));
        }

        if frame_count == -1 {
            return Ok(RespArray::new(None::<Vec<_>>));
        }
        let mut frames = vec![];
        let mut tmp_buf = BytesMut::from(&data[end + CRLF_LEN..]);
        for _ in 0..frame_count {
            frames.push(RespFrame::decode(&mut tmp_buf)?);
        }
        Ok(RespArray::new(Some(frames)))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, length) = parse_aggregate_length(buf, Self::PREFIX.as_bytes())?;
        if length < 0 {
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

#[cfg(test)]
mod tests {
    use crate::resp::bulk_string::BulkString;

    use super::*;

    #[test]
    fn test_encode_array() {
        let array: RespFrame = RespArray::new(Some(vec![
            BulkString::new(Some(b"set")).into(),
            BulkString::new(Some(b"hello")).into(),
            BulkString::new(Some(b"world")).into(),
        ]))
        .into();
        assert_eq!(
            array.encode(),
            b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_decode_resp_array() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(
            frame,
            RespFrame::Array(RespArray::new(Some(vec![
                BulkString::new(Some(b"set")).into(),
                BulkString::new(Some(b"hello")).into(),
                BulkString::new(Some(b"world")).into(),
            ])))
        );

        buf.extend_from_slice(b"*-1\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Array(RespArray::new(None::<Vec<_>>)));
    }
}
