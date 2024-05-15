use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::CRLF_LEN;

impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        let sign = if self { 't' } else { 'f' };
        format!("#{}\r\n", sign).into_bytes()
    }
}

impl RespDecode for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Bool(#), got {:?}",
                buf
            )));
        }

        let expect_length = bool::expect_length(buf)?;
        let data = buf.split_to(expect_length);
        match String::from_utf8((&data[Self::PREFIX.len()..expect_length - CRLF_LEN]).into())?
            .as_str()
        {
            "t" => Ok(true),
            "f" => Ok(false),
            val => Err(RespError::InvalidFrameData(val.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::resp::frame::RespFrame;

    use super::*;
    #[test]
    fn test_encode_bool() {
        let bo: RespFrame = true.into();
        assert_eq!(bo.encode(), b"#t\r\n");

        let bo: RespFrame = false.into();
        assert_eq!(bo.encode(), b"#f\r\n");
    }

    #[test]
    fn test_decode_bool() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"#t\r\n#f\r\n#d\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Boolean(true));
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Boolean(false));
        let frame = RespFrame::decode(&mut buf);
        assert_eq!(
            frame.unwrap_err(),
            RespError::InvalidFrameData("d".to_string())
        );
    }
}
