use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::CRLF_LEN;

#[derive(Debug, PartialEq, Clone)]
pub struct SimpleError(String);

impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

impl RespDecode for SimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: SimpleError(-), got {:?}",
                buf
            )));
        }
        let expect_length = SimpleError::expect_length(buf)?;

        let data = buf.split_to(expect_length);
        Ok(
            String::from_utf8((&data[Self::PREFIX.len()..expect_length - CRLF_LEN]).into())
                .map(SimpleError::new)?,
        )
    }
}

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

#[cfg(test)]
mod tests {

    use crate::resp::frame::RespFrame;

    use super::*;

    #[test]
    fn test_encode_integer() {
        let i: RespFrame = 10.into();
        assert_eq!(i.encode(), b":10\r\n");

        let i: RespFrame = (-10).into();
        assert_eq!(i.encode(), b":-10\r\n");
    }

    #[test]
    fn test_decode_simple_error() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"-ERROR error\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(
            frame,
            RespFrame::SimpleError(SimpleError::new("ERROR error"))
        );
    }
}
