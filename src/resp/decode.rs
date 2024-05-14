use std::collections::BTreeSet;

use bytes::BytesMut;
use tracing::info;

use crate::{
    BulkString, RespArray, RespDecode, RespEncode, RespError, RespFrame, RespMap, RespNull,
    RespSet, SimpleError, SimpleString,
};

use super::{find_crlf, AGGREGATE_FRAME_TYPE, CRLF_LEN};

impl RespDecode for RespFrame {
    const PREFIX: &'static str = "";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        info!("if iter peek: {:?}", iter.peek());
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
            Some(val) => Err(RespError::InvalidFrameType(format!(
                "unknown frame type: {:?}",
                val
            ))),
            None => Err(RespError::NotComplete),
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => SimpleString::expect_length(buf),
            Some(b'-') => SimpleError::expect_length(buf),
            Some(b':') => i64::expect_length(buf),
            Some(b'#') => bool::expect_length(buf),
            Some(b'$') => BulkString::expect_length(buf),
            Some(b'*') => RespArray::expect_length(buf),
            Some(b'_') => RespNull::expect_length(buf),
            Some(b',') => f64::expect_length(buf),
            Some(b'%') => RespMap::expect_length(buf),
            Some(b'~') => RespSet::expect_length(buf),
            _ => Err(RespError::InvalidFrameType(format!(
                "unknown frame type: {:?}",
                buf
            ))),
        }
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

impl RespDecode for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Map(%), got {:?}",
                buf
            )));
        }

        let expect_length = RespMap::expect_length(buf)?;
        let data = buf.split_to(expect_length);

        let (end, frame_count) = parse_aggregate_length(&data, Self::PREFIX.as_bytes())?;
        if frame_count < 0 {
            return Err(RespError::InvalidFrameLength(frame_count));
        }

        let mut map = RespMap::new();
        let mut tmp_buf = BytesMut::from(&data[end + CRLF_LEN..]);
        for _ in 0..frame_count {
            map.insert(
                SimpleString::decode(&mut tmp_buf)?,
                RespFrame::decode(&mut tmp_buf)?,
            );
        }
        Ok(map)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, length) = parse_aggregate_length(buf, Self::PREFIX.as_bytes())?;
        if length <= 0 {
            return Ok(end + CRLF_LEN);
        }
        let mut cur_index = end + CRLF_LEN;
        for _ in 0..length {
            let length = RespFrame::expect_length(&buf[cur_index..])?;
            cur_index += length;
            let length = RespFrame::expect_length(&buf[cur_index..])?;
            cur_index += length;
        }
        Ok(cur_index)
    }
}

impl RespDecode for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if !buf.starts_with(Self::PREFIX.as_bytes()) {
            return Err(RespError::InvalidFrameType(format!(
                "expect: Set(~), got {:?}",
                buf
            )));
        }

        let expect_length = RespSet::expect_length(buf)?;
        let data = buf.split_to(expect_length);

        let (end, frame_count) = parse_aggregate_length(&data, Self::PREFIX.as_bytes())?;
        if frame_count < 0 {
            return Err(RespError::InvalidFrameLength(frame_count));
        }

        let mut existed = BTreeSet::new();
        let cur_index = end + CRLF_LEN;
        let mut tmp_buf = BytesMut::from(&data[cur_index..]);
        for _ in 0..frame_count {
            let frame = RespFrame::decode(&mut tmp_buf)?;
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
        Ok(RespSet::new(set))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, length) = parse_aggregate_length(buf, Self::PREFIX.as_bytes())?;
        if length <= 0 {
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

fn parse_aggregate_length(buf: &[u8], prefix: &[u8]) -> Result<(usize, isize), RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }
    if !AGGREGATE_FRAME_TYPE.contains(&prefix) {
        return Err(RespError::InvalidFrameType(format!(
            "frame type {} is not a aggregate type",
            String::from_utf8_lossy(prefix)
        )));
    }
    let end = find_crlf(buf, 1).ok_or(RespError::NotComplete)?;
    Ok((
        end,
        String::from_utf8((&buf[prefix.len()..end]).into())?.parse()?,
    ))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{
        BulkString, RespArray, RespDecode, RespError, RespFrame, RespMap, RespNull, RespSet,
        SimpleError, SimpleString,
    };

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
