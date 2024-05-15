use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

#[derive(Debug, PartialEq, Clone)]
pub struct RespNull;

impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespDecode for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Null(_), got {:?}",
                buf
            )));
        }

        let expect_length = RespNull::expect_length(buf)?;
        let data = buf.split_to(expect_length);

        if !data.ends_with(b"\r\n") || data.len() != 3 {
            return Err(RespError::InvalidFrameData(format!("{:?}", data)));
        }
        Ok(RespNull::new())
    }
}

impl RespNull {
    pub fn new() -> Self {
        RespNull
    }
}

impl Default for RespNull {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use crate::resp::frame::RespFrame;

    use super::*;

    #[test]
    fn test_encode_null() {
        let null: RespFrame = RespNull::new().into();
        assert_eq!(null.encode(), b"_\r\n");
    }

    #[test]
    fn test_decode_null() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"_\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Null(RespNull::new()));

        buf.extend_from_slice(b"_1\r\n");
        let frame = RespFrame::decode(&mut buf);
        assert_eq!(
            frame.unwrap_err(),
            RespError::InvalidFrameData(format!("{:?}", BytesMut::from("_1\r\n".as_bytes())))
        );
    }
}
