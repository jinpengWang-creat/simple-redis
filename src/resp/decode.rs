use std::collections::BTreeSet;

use bytes::BytesMut;

use crate::{
    BulkString, RespArray, RespDecode, RespEncode, RespError, RespFrame, RespMap, RespNull,
    RespSet, SimpleError, SimpleString,
};

impl RespDecode for RespFrame {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => Ok(SimpleString::decode(buf)?.into()),
            Some(b'-') => Ok(SimpleError::decode(buf)?.into()),
            Some(b':') => Ok(i64::decode(buf)?.into()),
            Some(b'#') => Ok(bool::decode(buf)?.into()),
            Some(b'$') => Ok(BulkString::decode(buf)?.into()),
            Some(b'*') => Ok(RespArray::decode(buf)?.into()),
            Some(b'_') => Ok(RespNull::decode(buf)?.into()),
            Some(b',') => Ok(f64::decode(buf)?.into()),
            Some(b'%') => Ok(RespMap::decode(buf)?.into()),
            Some(b'~') => Ok(RespSet::decode(buf)?.into()),
            Some(val) => Err(RespError::InvalidFrameType(val.to_string())),
            None => Err(RespError::NotComplete),
        }
    }
}

impl RespDecode for SimpleString {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"+", "SimpleString(+)")?;
        Ok(String::from_utf8((&data[1..end]).into()).map(SimpleString::new)?)
    }
}

impl RespDecode for SimpleError {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"-", "SimpleError(-)")?;
        Ok(String::from_utf8((&data[1..end]).into()).map(SimpleError::new)?)
    }
}

impl RespDecode for i64 {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b":", "Integer(:)")?;
        Ok(String::from_utf8((&data[1..end]).into())?.parse()?)
    }
}

impl RespDecode for bool {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"#", "Bool(#)")?;
        match String::from_utf8((&data[1..end]).into())?.as_str() {
            "t" => Ok(true),
            "f" => Ok(false),
            val => Err(RespError::InvalidFrameData(val.to_string())),
        }
    }
}

impl RespDecode for BulkString {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"$", "BulkString($)")?;
        let length: isize = String::from_utf8((&data[1..end]).into())?.parse()?;
        if length < -1 {
            return Err(RespError::InvalidFrameLength(length));
        }
        if length == -1 {
            return Ok(BulkString::new(None::<Vec<_>>));
        }

        let content_size = (length + 2) as usize;
        if buf.len() < content_size {
            return Err(RespError::NotComplete);
        }
        let content = buf.split_to(content_size);
        if !content.ends_with(b"\r\n") {
            return Err(RespError::InvalidFrameData(format!("{:?}", content)));
        }
        Ok(BulkString::new(Some(&content[0..length as usize])))
    }
}

impl RespDecode for RespArray {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"*", "Array(*)")?;
        let length: isize = String::from_utf8((&data[1..end]).into())?.parse()?;
        if length < -1 {
            return Err(RespError::InvalidFrameLength(length));
        }
        if length == -1 {
            return Ok(RespArray::new(None::<Vec<_>>));
        }

        let mut copy_buf = buf.clone();
        let mut frames = vec![];
        for _ in 0..length {
            frames.push(RespFrame::decode(&mut copy_buf)?);
        }
        buf.clear();
        buf.extend_from_slice(&copy_buf[..]);
        Ok(RespArray::new(Some(frames)))
    }
}

impl RespDecode for RespNull {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"_", "Null(_)")?;
        if data.len() != 3 || end != 1 {
            return Err(RespError::InvalidFrameData(format!("{:?}", data)));
        }
        Ok(RespNull::new())
    }
}

impl RespDecode for f64 {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b",", "Double(,)")?;
        Ok(String::from_utf8((&data[1..end]).into())?.parse()?)
    }
}

impl RespDecode for RespMap {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"%", "Map(%)")?;
        let length: isize = String::from_utf8((&data[1..end]).into())?.parse()?;
        if length < 0 {
            return Err(RespError::InvalidFrameLength(length));
        }

        let mut copy_buf = buf.clone();
        let mut map = RespMap::new();
        for _ in 0..length {
            map.insert(
                SimpleString::decode(&mut copy_buf)?,
                RespFrame::decode(&mut copy_buf)?,
            );
        }

        buf.clear();
        buf.extend_from_slice(&copy_buf[..]);
        Ok(map)
    }
}

impl RespDecode for RespSet {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (data, end) = extract_simple_frame_data(buf, b"~", "Set(~)")?;
        let length: isize = String::from_utf8((&data[1..end]).into())?.parse()?;
        if length < 0 {
            return Err(RespError::InvalidFrameLength(length));
        }

        let mut copy_buf = buf.clone();
        let mut existed = BTreeSet::new();
        for _ in 0..length {
            let frame = RespFrame::decode(&mut copy_buf)?;
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

        buf.clear();
        buf.extend_from_slice(&copy_buf[..]);
        Ok(RespSet::new(set))
    }
}

fn extract_simple_frame_data(
    buf: &mut BytesMut,
    prefix: &[u8],
    expect_type: &str,
) -> Result<(BytesMut, usize), RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(prefix) {
        return Err(RespError::InvalidFrameType(format!(
            "expect: {}, got {:?}",
            expect_type, buf
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

    use crate::{
        BulkString, RespArray, RespDecode, RespError, RespFrame, RespMap, RespNull, RespSet,
        SimpleError, SimpleString,
    };

    #[test]
    fn test_decode_simple_frame() {
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

    #[test]
    fn test_decode_i64() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b":10\r\n:-30\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Integer(10));
        let frame = RespFrame::decode(&mut buf).unwrap();
        assert_eq!(frame, RespFrame::Integer(-30));
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

    #[test]
    fn test_decode_resp_map() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"%2\r\n+foo\r\n$3\r\nbar\r\n+hello\r\n+world\r\n");
        let frame = RespFrame::decode(&mut buf).unwrap();

        let mut map = RespMap::new();
        map.insert(
            SimpleString::new("hello"),
            SimpleString::new("world").into(),
        );
        map.insert(
            SimpleString::new("foo"),
            BulkString::new(Some("bar")).into(),
        );
        let frame1 = RespFrame::Map(map);
        assert_eq!(frame, frame1);
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
