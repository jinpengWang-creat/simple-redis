const DEFAULT_FRAME_SIZE: usize = 16;

use crate::{
    BulkString, RespArray, RespEncode, RespFrame, RespMap, RespNull, RespNullArray,
    RespNullBulkString, RespSet, SimpleError, SimpleString,
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
        let len = self.len();
        let mut result = Vec::with_capacity(len + DEFAULT_FRAME_SIZE);
        result.extend_from_slice(format!("${}\r\n", len).as_bytes());
        result.extend_from_slice(&self);
        result.extend_from_slice(b"\r\n");
        result
    }
}

impl RespEncode for RespNullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let len = self.len();
        let mut result = Vec::with_capacity(len * DEFAULT_FRAME_SIZE);
        result.extend_from_slice(format!("*{}\r\n", len).as_bytes());
        self.0
            .into_iter()
            .for_each(|frame| result.extend_from_slice(&frame.encode()));
        result
    }
}

impl RespEncode for RespNullArray {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        let sign = if self { 't' } else { 'n' };
        format!("#{}\r\n", sign).into_bytes()
    }
}

impl RespEncode for f64 {
    fn encode(self) -> Vec<u8> {
        let mut result = Vec::with_capacity(DEFAULT_FRAME_SIZE);
        let ret = if self.abs() > 1e+8 {
            format!(",{:+e}\r\n", self)
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

impl RespEncode for RespFrame {
    fn encode(self) -> Vec<u8> {
        match self {
            RespFrame::SimpleString(inner) => inner.encode(),
            RespFrame::SimpleError(inner) => inner.encode(),
            RespFrame::Integer(inner) => inner.encode(),
            RespFrame::BulkString(inner) => inner.encode(),
            RespFrame::NullBulkString(inner) => inner.encode(),
            RespFrame::Array(inner) => inner.encode(),
            RespFrame::NullArray(inner) => inner.encode(),
            RespFrame::Null(inner) => inner.encode(),
            RespFrame::Boolean(inner) => inner.encode(),
            RespFrame::Double(inner) => inner.encode(),
            RespFrame::Map(inner) => inner.encode(),
            RespFrame::Set(inner) => inner.encode(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer() {
        let i = 10i64;
        assert_eq!(b":10\r\n".to_vec(), i.encode());

        let i = -20i64;
        assert_eq!(b":-20\r\n".to_vec(), i.encode());
    }
}
