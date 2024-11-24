use std::collections::BTreeSet;

use bytes::BytesMut;
use winnow::{
    combinator::{alt, dispatch, fail, preceded, terminated},
    error::{ContextError, ErrMode, ErrorKind, FromExternalError, Needed},
    token::{any, take, take_until},
    PResult, Parser,
};

use crate::{
    BulkString, RespArray, RespEncode, RespError, RespFrame, RespMap, RespNull, RespSet,
    SimpleError, SimpleString,
};

use super::CRLF;

pub fn parse_frame(input: &mut &[u8]) -> PResult<RespFrame> {
    dispatch! { any;
        b'+' => parse_simple_string.map(RespFrame::SimpleString),
        b'-' => parse_simple_error.map(RespFrame::SimpleError),
        b':' => parse_integer.map(RespFrame::Integer),
        b'$' => parse_bulk_string.map(RespFrame::BulkString),
        b'*' => parse_array.map(RespFrame::Array),
        b'_' => parse_null.map(RespFrame::Null),
        b'#' => parse_boolean.map(RespFrame::Boolean),
        b',' => parse_double.map(RespFrame::Double),
        b'%' => parse_map.map(RespFrame::Map),
        b'~' => parse_set.map(RespFrame::Set),
        _ => fail::<_,_,_>,
    }
    .parse_next(input)
}

pub fn parse_frame_length(input: &[u8]) -> Result<usize, RespError> {
    let target = &mut (&*input);
    let ret = parse_frame_len(target);
    match ret {
        Ok(_) => {
            let start = input.as_ptr() as usize;
            let end = (*target).as_ptr() as usize;
            Ok(end - start)
        }
        Err(RespError::NotComplete) => Err(RespError::NotComplete),
        Err(RespError::InvalidFrame(_)) => Err(RespError::InvalidFrame(
            String::from_utf8_lossy(input).to_string(),
        )),
        Err(e) => Err(e),
    }
}

fn parse_frame_len(input: &mut &[u8]) -> Result<(), RespError> {
    if input.is_empty() {
        return Err(RespError::NotComplete);
    }
    dispatch! { any;
        b'+' => parse_simple_len,
        b'-' => parse_simple_len,
        b':' => parse_simple_len,
        b'$' => parse_bulk_string_len,
        b'*' => parse_array_len,
        b'_' => parse_simple_len,
        b'#' => parse_simple_len,
        b',' => parse_simple_len,
        b'%' => parse_map_len,
        b'~' => parse_array_len,
        _ => fail::<_,_,_>,
    }
    .parse_next(input)
    .map_err(|e| match e {
        ErrMode::Incomplete(_) => RespError::NotComplete,
        _ => RespError::InvalidFrame(e.to_string()),
    })
}

fn parse_simple_string(input: &mut &[u8]) -> PResult<SimpleString> {
    parse_str.map(SimpleString::new).parse_next(input)
}

fn parse_simple_error(input: &mut &[u8]) -> PResult<SimpleError> {
    parse_str.map(SimpleError::new).parse_next(input)
}

fn parse_integer(input: &mut &[u8]) -> PResult<i64> {
    parse_str
        .map(|s| {
            let val = s.parse::<i64>().map_err(|e| {
                ErrMode::Cut(ContextError::from_external_error(&s, ErrorKind::Fail, e))
            })?;
            Ok::<i64, ErrMode<ContextError>>(val)
        })
        .parse_next(input)?
}

fn parse_bulk_string(input: &mut &[u8]) -> PResult<BulkString> {
    let len_str = parse_str.parse_next(input)?;
    let len = len_str
        .parse::<isize>()
        .map_err(|e| ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)))?;
    if len < -1 {
        return Err(ErrMode::Cut(ContextError::from_external_error(
            input,
            ErrorKind::Fail,
            RespError::InvalidFrameLength(len),
        )));
    }
    if len == -1 {
        return Ok(BulkString::new(None::<Vec<_>>));
    }
    let data = parse_str.parse_next(input)?;
    if data.len() != len as usize {
        return Err(ErrMode::Cut(ContextError::from_external_error(
            input,
            ErrorKind::Fail,
            RespError::InvalidFrameData(format!("{:?}", data)),
        )));
    }
    Ok(BulkString::new(Some(data.into_bytes())))
}

fn parse_array(input: &mut &[u8]) -> PResult<RespArray> {
    let len_str = parse_str.parse_next(input)?;
    let len = len_str
        .parse::<isize>()
        .map_err(|e| ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)))?;
    if len < -1 {
        return Err(ErrMode::Cut(ContextError::from_external_error(
            input,
            ErrorKind::Fail,
            RespError::InvalidFrameLength(len),
        )));
    }
    if len == -1 {
        return Ok(RespArray::new(None::<Vec<_>>));
    }
    let mut frames = Vec::with_capacity(len as usize);
    for _ in 0..len {
        frames.push(parse_frame.parse_next(input).map_err(|_| {
            ErrMode::Cut(ContextError::from_external_error(
                input,
                ErrorKind::Fail,
                RespError::InvalidFrameData("".to_string()),
            ))
        })?);
    }
    Ok(RespArray::new(Some(frames)))
}

fn parse_null(input: &mut &[u8]) -> PResult<RespNull> {
    CRLF.value(RespNull::new()).parse_next(input)
}

fn parse_boolean(input: &mut &[u8]) -> PResult<bool> {
    let b = terminated(alt(('t', 'f')), CRLF).parse_next(input)?;
    Ok(b == 't')
}

fn parse_double(input: &mut &[u8]) -> PResult<f64> {
    parse_str
        .map(|s| {
            let val = s.parse::<f64>().map_err(|e| {
                ErrMode::Cut(ContextError::from_external_error(&s, ErrorKind::Fail, e))
            })?;
            Ok::<f64, ErrMode<ContextError>>(val)
        })
        .parse_next(input)?
}

fn parse_map(input: &mut &[u8]) -> PResult<RespMap> {
    let len_str = parse_str.parse_next(input)?;
    let len = len_str
        .parse::<isize>()
        .map_err(|e| ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)))?;
    if len < 0 {
        return Err(ErrMode::Cut(ContextError::from_external_error(
            input,
            ErrorKind::Fail,
            RespError::InvalidFrameLength(len),
        )));
    }
    let mut map = RespMap::new();
    for _ in 0..len {
        map.insert(
            preceded(any, parse_simple_string).parse_next(input)?,
            parse_frame.parse_next(input)?,
        );
    }
    Ok(map)
}

fn parse_set(input: &mut &[u8]) -> PResult<RespSet> {
    let len_str = parse_str.parse_next(input)?;
    let len = len_str
        .parse::<isize>()
        .map_err(|e| ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)))?;
    if len < 0 {
        return Err(ErrMode::Cut(ContextError::from_external_error(
            input,
            ErrorKind::Fail,
            RespError::InvalidFrameLength(len),
        )));
    }
    let mut existed = BTreeSet::new();
    for _ in 0..len {
        let frame = parse_frame.parse_next(input)?;
        let encode = frame.encode();
        if !existed.contains(&encode) {
            existed.insert(encode);
        }
    }
    let mut set = vec![];
    for encode in existed {
        let buf = BytesMut::from(&encode[..]);
        set.push(parse_frame.parse_next(&mut buf.as_ref())?);
    }
    Ok(RespSet::new(set))
}

fn parse_str(input: &mut &[u8]) -> PResult<String> {
    let content = terminated(take_until(0.., CRLF), CRLF)
        .parse_to::<String>()
        .parse_next(input);
    println!("parse_str: {:?}", content);
    content
}

fn parse_simple_len(input: &mut &[u8]) -> PResult<()> {
    terminated(take_until(0.., CRLF), CRLF)
        .value(())
        .parse_next(input)
        .map_err(|_: ErrMode<ContextError>| ErrMode::Incomplete(Needed::Unknown))
}

fn parse_bulk_string_len(input: &mut &[u8]) -> PResult<()> {
    let len_str = parse_str
        .parse_next(input)
        .map_err(|_: ErrMode<ContextError>| ErrMode::Incomplete(Needed::Unknown))?;

    let len = len_str
        .parse::<isize>()
        .map_err(|e| ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)))?;
    if len < -1 {
        return Err(ErrMode::Cut(ContextError::from_external_error(
            input,
            ErrorKind::Fail,
            RespError::InvalidFrameLength(len),
        )));
    }

    if len == -1 {
        return Ok(());
    }

    terminated(take(len as usize), CRLF)
        .parse_next(input)
        .map_err(|_e: ErrMode<ContextError>| ErrMode::Incomplete(Needed::new(len as usize)))?;
    Ok(())
}

fn parse_array_len(input: &mut &[u8]) -> PResult<()> {
    let len_str = parse_str
        .parse_next(input)
        .map_err(|_: ErrMode<ContextError>| ErrMode::Incomplete(Needed::Unknown))?;

    let len = len_str
        .parse::<isize>()
        .map_err(|e| ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)))?;
    if len < -1 {
        return Err(ErrMode::Cut(ContextError::from_external_error(
            input,
            ErrorKind::Fail,
            RespError::InvalidFrameLength(len),
        )));
    }

    if len == -1 {
        return Ok(());
    }

    for _ in 0..len {
        parse_frame_len(input).map_err(|e| match e {
            RespError::NotComplete => ErrMode::Incomplete(Needed::Unknown),
            _ => ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)),
        })?;
    }
    Ok(())
}

fn parse_map_len(input: &mut &[u8]) -> PResult<()> {
    let len_str = parse_str
        .parse_next(input)
        .map_err(|_: ErrMode<ContextError>| ErrMode::Incomplete(Needed::Unknown))?;

    let len = len_str
        .parse::<isize>()
        .map_err(|e| ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)))?;
    if len < 0 {
        return Err(ErrMode::Cut(ContextError::from_external_error(
            input,
            ErrorKind::Fail,
            RespError::InvalidFrameLength(len),
        )));
    }

    for _ in 0..len {
        parse_simple_len(input)?;
        parse_frame_len(input).map_err(|e| match e {
            RespError::NotComplete => ErrMode::Incomplete(Needed::Unknown),
            _ => ErrMode::Cut(ContextError::from_external_error(input, ErrorKind::Fail, e)),
        })?;
    }
    Ok(())
}
#[cfg(test)]
mod parser_tests {
    use super::*;
    use crate::RespFrame;

    #[test]
    fn test_parse_simple_string() {
        let mut input = b"+hello\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(frame, RespFrame::SimpleString(SimpleString::new("hello")));
    }

    #[test]
    fn test_parse_simple_error() {
        let mut input = b"-error message\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(
            frame,
            RespFrame::SimpleError(SimpleError::new("error message"))
        );
    }

    #[test]
    fn test_parse_integer() {
        let mut input = b":1234\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(frame, RespFrame::Integer(1234));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut input = b"$5\r\nhello\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(
            frame,
            RespFrame::BulkString(BulkString::new(Some(b"hello".to_vec())))
        );
    }

    #[test]
    fn test_parse_array() {
        let mut input = b"*3\r\n:1\r\n:2\r\n:3\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(
            frame,
            RespFrame::Array(RespArray::new(Some(vec![
                RespFrame::Integer(1),
                RespFrame::Integer(2),
                RespFrame::Integer(3)
            ])))
        );
    }

    #[test]
    fn test_parse_null() {
        let mut input = b"_\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(frame, RespFrame::Null(RespNull::new()));
    }

    #[test]
    fn test_parse_boolean() {
        let mut input = b"#t\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(frame, RespFrame::Boolean(true));

        let mut input = b"#f\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(frame, RespFrame::Boolean(false));
    }

    #[test]
    fn test_parse_double() {
        let mut input = b",-11.44\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(frame, RespFrame::Double(-11.44));
    }

    #[test]
    fn test_parse_map() {
        let mut input = b"%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        let mut map = RespMap::new();
        map.insert(SimpleString::new("key1"), RespFrame::Integer(1));
        map.insert(SimpleString::new("key2"), RespFrame::Integer(2));
        assert_eq!(frame, RespFrame::Map(map));
    }

    #[test]
    fn test_parse_set() {
        let mut input = b"~3\r\n:1\r\n:2\r\n:3\r\n".as_ref();
        let frame = parse_frame(&mut input).unwrap();
        assert_eq!(
            frame,
            RespFrame::Set(RespSet::new(vec![
                RespFrame::Integer(1),
                RespFrame::Integer(2),
                RespFrame::Integer(3)
            ]))
        );
    }

    #[test]
    fn test_parse_str() {
        let mut input = b"hello\r\n".as_ref();
        let s = parse_str(&mut input).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_parse_frame_length_for_simple_string() {
        let input = b"+hello\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_simple_error() {
        let input = b"-error message\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_integer() {
        let input = b":1234\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_bulk_string() {
        let input = b"$5\r\nhello\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_array() {
        let input = b"*3\r\n:1\r\n:2\r\n:3\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_null() {
        let input = b"_\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_boolean() {
        let input = b"#t\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());

        let input = b"#f\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_double() {
        let input = b",-3.14\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_map() {
        let input = b"%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_set() {
        let input = b"~3\r\n:1\r\n:2\r\n:3\r\n".as_ref();
        let len = parse_frame_length(input).unwrap();
        assert_eq!(len, input.len());
    }

    #[test]
    fn test_parse_frame_length_for_incomplete() {
        let input = b"+hello".as_ref();
        let len = parse_frame_length(input);
        assert_eq!(len, Err(RespError::NotComplete));
    }

    #[test]
    fn test_parse_frame_length_for_invalid_frame() {
        let input = b"hello\r\n".as_ref();
        let len = parse_frame_length(input);
        assert_eq!(len, Err(RespError::InvalidFrame("hello\r\n".to_string())));
    }
}
