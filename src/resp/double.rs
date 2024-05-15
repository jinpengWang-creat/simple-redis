use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{CRLF_LEN, DEFAULT_FRAME_SIZE};

impl RespEncode for f64 {
    fn encode(self) -> Vec<u8> {
        let mut result = Vec::with_capacity(DEFAULT_FRAME_SIZE);
        let ret = if self.abs() > 1e+8 || self.abs() < 1e-8 {
            format!(",{:e}\r\n", self)
        } else {
            format!(",{}\r\n", self)
        };
        result.extend_from_slice(ret.as_bytes());
        result
    }
}

impl RespDecode for f64 {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Double(,), got {:?}",
                buf
            )));
        }

        let expect_length = f64::expect_length(buf)?;
        let data = buf.split_to(expect_length);
        Ok(
            String::from_utf8((&data[Self::PREFIX.len()..expect_length - CRLF_LEN]).into())?
                .parse()?,
        )
    }
}

#[cfg(test)]
mod tests {

    use crate::resp::frame::RespFrame;

    use super::*;
    #[test]
    fn test_encode_double() {
        let f: RespFrame = (123.456).into();
        assert_eq!(f.encode(), b",123.456\r\n");
        let f: RespFrame = (-123.456).into();
        assert_eq!(f.encode(), b",-123.456\r\n");
        let f: RespFrame = (1.23456e+8).into();
        assert_eq!(f.encode(), b",1.23456e8\r\n");
        let f: RespFrame = (-1.23456e-9).into();
        assert_eq!(f.encode(), b",-1.23456e-9\r\n");
    }

    #[test]
    fn test_decode_f64() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b",123.456\r\n");
        buf.extend_from_slice(b",-123.456\r\n");
        buf.extend_from_slice(b",1.23456e8\r\n");
        buf.extend_from_slice(b",-1.23456e-9\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Double(123.456));
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Double(-123.456));
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Double(1.23456e8));
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Double(-1.23456e-9));
    }
}
