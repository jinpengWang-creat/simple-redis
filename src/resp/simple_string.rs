use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::CRLF_LEN;

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
pub struct SimpleString(String);

impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl RespDecode for SimpleString {
    const PREFIX: &'static str = "+";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: SimpleString(+), got {:?}",
                buf
            )));
        }

        let expect_length = SimpleString::expect_length(buf)?;
        let data = buf.split_to(expect_length);
        Ok(
            String::from_utf8((&data[Self::PREFIX.len()..expect_length - CRLF_LEN]).into())
                .map(SimpleString::new)?,
        )
    }
}

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

#[cfg(test)]
mod tests {

    use crate::resp::frame::RespFrame;

    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let simple_string: RespFrame = SimpleString::new("hello").into();
        assert_eq!(simple_string.encode(), b"+hello\r\n")
    }

    #[test]
    fn test_decode_simple_string() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"+OK\r\n+hello");

        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::SimpleString(SimpleString::new("OK")));
        assert_eq!(buf.to_vec(), b"+hello");

        let frame = RespFrame::decode(&mut buf).unwrap_err();
        assert_eq!(frame, RespError::NotComplete);
        buf.extend_from_slice(b"\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::SimpleString(SimpleString::new("hello")));
    }
}
