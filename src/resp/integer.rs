use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::CRLF_LEN;

impl RespEncode for i64 {
    fn encode(self) -> Vec<u8> {
        format!(":{}\r\n", self).into_bytes()
    }
}

impl RespDecode for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Integer(:), got {:?}",
                buf
            )));
        }

        let expect_length = i64::expect_length(buf)?;
        let data = buf.split_to(expect_length);
        Ok(
            String::from_utf8((&data[Self::PREFIX.len()..expect_length - CRLF_LEN]).into())?
                .parse()?,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::resp::{frame::RespFrame, simple_string::SimpleString};

    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let simple_string: RespFrame = SimpleString::new("hello").into();
        assert_eq!(simple_string.encode(), b"+hello\r\n")
    }

    #[test]
    fn test_decode_i64() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b":10\r\n:-30\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Integer(10));
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Integer(-30));
    }
}
