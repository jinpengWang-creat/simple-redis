const DEFAULT_FRAME_SIZE: usize = 16;

use crate::{
    BulkString, RespArray, RespEncode, RespMap, RespNull, RespSet, SimpleError, SimpleString,
};

impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

impl RespEncode for i64 {
    fn encode(self) -> Vec<u8> {
        format!(":{}\r\n", self).into_bytes()
    }
}

impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let bulk = self.0.map(|bulk_string| {
            let len = bulk_string.len();
            let mut result = Vec::with_capacity(len + DEFAULT_FRAME_SIZE);
            result.extend_from_slice(format!("${}\r\n", len).as_bytes());
            result.extend_from_slice(&bulk_string);
            result.extend_from_slice(b"\r\n");
            result
        });
        bulk.unwrap_or(b"$-1\r\n".to_vec())
    }
}

impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let array = self.0.map(|array| {
            let len = array.len();
            let mut result = Vec::with_capacity(len * DEFAULT_FRAME_SIZE);
            result.extend_from_slice(format!("*{}\r\n", len).as_bytes());
            array
                .into_iter()
                .for_each(|frame| result.extend_from_slice(&frame.encode()));
            result
        });
        array.unwrap_or(b"*-1\r\n".to_vec())
    }
}

impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        let sign = if self { 't' } else { 'f' };
        format!("#{}\r\n", sign).into_bytes()
    }
}

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

impl RespEncode for RespMap {
    fn encode(self) -> Vec<u8> {
        let len = self.len();
        let mut result = Vec::with_capacity(len * DEFAULT_FRAME_SIZE);
        result.extend_from_slice(format!("%{}\r\n", len).as_bytes());
        self.0.into_iter().for_each(|(key, frame)| {
            result.extend_from_slice(&key.encode());
            result.extend_from_slice(&frame.encode());
        });
        result
    }
}

impl RespEncode for RespSet {
    fn encode(self) -> Vec<u8> {
        let len = self.len();
        let mut result = Vec::with_capacity(len * DEFAULT_FRAME_SIZE);
        result.extend_from_slice(format!("~{}\r\n", len).as_bytes());
        self.0.into_iter().for_each(|frame| {
            result.extend_from_slice(&frame.encode());
        });
        result
    }
}

#[cfg(test)]
mod tests {

    use crate::RespFrame;

    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let simple_string: RespFrame = SimpleString::new("hello").into();
        assert_eq!(simple_string.encode(), b"+hello\r\n")
    }

    #[test]
    fn test_encode_simple_error() {
        let simple_error: RespFrame = SimpleError::new("ERR some error!").into();
        assert_eq!(simple_error.encode(), b"-ERR some error!\r\n")
    }

    #[test]
    fn test_encode_integer() {
        let i: RespFrame = 10.into();
        assert_eq!(i.encode(), b":10\r\n");

        let i: RespFrame = (-10).into();
        assert_eq!(i.encode(), b":-10\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let bulk_string: RespFrame = BulkString::new(Some(b"hello")).into();
        assert_eq!(bulk_string.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_array() {
        let array: RespFrame = RespArray::new(Some(vec![
            BulkString::new(Some(b"set")).into(),
            BulkString::new(Some(b"hello")).into(),
            BulkString::new(Some(b"world")).into(),
        ]))
        .into();
        assert_eq!(
            array.encode(),
            b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_encode_null() {
        let null: RespFrame = RespNull::new().into();
        assert_eq!(null.encode(), b"_\r\n");
    }

    #[test]
    fn test_encode_bool() {
        let bo: RespFrame = true.into();
        assert_eq!(bo.encode(), b"#t\r\n");

        let bo: RespFrame = false.into();
        assert_eq!(bo.encode(), b"#f\r\n");
    }

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
    fn test_encode_map() {
        let mut map = RespMap::new();
        map.insert(
            SimpleString::new("hello"),
            SimpleString::new("world").into(),
        );
        map.insert(
            SimpleString::new("foo"),
            BulkString::new(Some("bar")).into(),
        );
        let frame: RespFrame = map.into();
        assert_eq!(
            frame.encode(),
            b"%2\r\n+foo\r\n$3\r\nbar\r\n+hello\r\n+world\r\n"
        );
    }

    #[test]
    fn test_encode_set() {
        let frame: RespFrame = RespSet::new(vec![
            10.into(),
            BulkString::new(Some("hello")).into(),
            SimpleString::new("world").into(),
        ])
        .into();
        assert_eq!(frame.encode(), b"~3\r\n:10\r\n$5\r\nhello\r\n+world\r\n");
    }
}
