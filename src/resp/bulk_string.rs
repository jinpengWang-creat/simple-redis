use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{parse_aggregate_length, CRLF_LEN, DEFAULT_FRAME_SIZE};

#[derive(Debug, PartialEq, Clone)]
pub struct BulkString(pub(crate) Option<Vec<u8>>);

impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let bulk = self.0.map(|bulk_string| {
            let len = bulk_string.len();
            let mut result = Vec::with_capacity(len + DEFAULT_FRAME_SIZE);
            result.extend_from_slice(format!("${}\r\n", len).as_bytes());
            result.extend_from_slice(&bulk_string);
            result.extend_from_slice(b"\r\n");
            result
        });
        bulk.unwrap_or(b"$-1\r\n".to_vec())
    }
}

impl RespDecode for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: BulkString($), got {:?}",
                buf
            )));
        }

        let expect_length = BulkString::expect_length(buf)?;
        let data = buf.split_to(expect_length);

        let (end, content_length) = parse_aggregate_length(&data, Self::PREFIX.as_bytes())?;
        if content_length < -1 {
            return Err(RespError::InvalidFrameLength(content_length));
        }
        if content_length == -1 {
            return Ok(BulkString::new(None::<Vec<_>>));
        }

        let content_begin = end + CRLF_LEN;
        let active_length = content_begin + content_length as usize + CRLF_LEN;
        if !data.ends_with(b"\r\n") || active_length != expect_length {
            return Err(RespError::InvalidFrameData(format!("{:?}", data)));
        }

        Ok(BulkString::new(Some(
            &data[content_begin..content_begin + content_length as usize],
        )))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, length) = parse_aggregate_length(buf, Self::PREFIX.as_bytes())?;
        if length < 0 {
            return Ok(end + CRLF_LEN);
        }
        Ok(length as usize + CRLF_LEN * 2 + end)
    }
}

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

#[cfg(test)]
mod tests {

    use crate::resp::frame::RespFrame;

    use super::*;

    #[test]
    fn test_encode_bulk_string() {
        let bulk_string: RespFrame = BulkString::new(Some(b"hello")).into();
        assert_eq!(bulk_string.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_decode_bulk_string() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$5\r\nhello\r\n$6\r\nworld\r\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(
            frame,
            RespFrame::BulkString(BulkString::new(Some(b"hello")))
        );
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(
            frame,
            RespFrame::BulkString(BulkString::new(Some(b"world\r")))
        );

        buf.extend_from_slice(b"$-1\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(
            frame,
            RespFrame::BulkString(BulkString::new(None::<Vec<_>>))
        );
    }
}
