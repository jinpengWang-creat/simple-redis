use bytes::BytesMut;

use crate::{RespDecode, RespError, RespFrame, SimpleError, SimpleString};

impl RespDecode for RespFrame {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => Ok(SimpleString::decode(buf)?.into()),
            Some(b'-') => Ok(SimpleError::decode(buf)?.into()),
            Some(b':') => Ok(i64::decode(buf)?.into()),
            Some(b'#') => Ok(bool::decode(buf)?.into()),
            _ => todo!(),
        }
    }
}

impl RespDecode for SimpleString {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"+")?;
        Ok(String::from_utf8((&data[1..end]).into()).map(SimpleString::new)?)
    }
}

impl RespDecode for SimpleError {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"-")?;
        Ok(String::from_utf8((&data[1..end]).into()).map(SimpleError::new)?)
    }
}

impl RespDecode for i64 {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b":")?;
        Ok(String::from_utf8((&data[1..end]).into())?.parse()?)
    }
}

impl RespDecode for bool {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"#")?;
        match String::from_utf8((&data[1..end]).into())?.as_str() {
            "t" => Ok(true),
            "f" => Ok(false),
            val => Err(RespError::InvalidFrameData(val.to_string())),
        }
    }
}

fn extract_simple_frame_data(
    buf: &mut BytesMut,
    prefix: &[u8],
) -> Result<(BytesMut, usize), RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(prefix) {
        return Err(RespError::InvalidFrameType(format!(
            "expect: FrameType({}), got {:?}",
            String::from_utf8_lossy(prefix),
            buf
        )));
    }

    let mut end = 0;
    for i in 0..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            end = i;
            break;
        }
    }
    if end == 0 {
        return Err(RespError::NotComplete);
    }
    Ok((buf.split_to(end + 2), end))
}
#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{RespDecode, RespError, RespFrame, SimpleError, SimpleString};

    #[test]
    fn test_decode_simple_frame() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"+OK\r\n-ERROR error\r\n+hello");

        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::SimpleString(SimpleString::new("OK")));
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(
            frame,
            RespFrame::SimpleError(SimpleError::new("ERROR error"))
        );
        assert_eq!(buf.to_vec(), b"+hello");
        let frame = RespFrame::decode(&mut buf).unwrap_err();
        assert_eq!(frame, RespError::NotComplete);
        buf.extend_from_slice(b"\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::SimpleString(SimpleString::new("hello")));
        buf.extend_from_slice(b":10\r\n:-30\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Integer(10));
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Integer(-30));

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
