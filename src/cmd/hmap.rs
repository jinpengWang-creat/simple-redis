use crate::RespFrame;

use super::{
    extract_frame, extract_string, validate_nums_of_argument, CommandError, CommandExecutor,
    RET_NULL, RET_NULL_ARRAY,
};

#[derive(Debug, PartialEq)]
pub struct HGet {
    key: String,
    field: String,
}

impl CommandExecutor for HGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend
            .hget(&self.key, &self.field)
            .unwrap_or(RET_NULL.clone())
    }
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value)
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hgetall(&self.key).unwrap_or(RET_NULL_ARRAY.clone())
    }
}

impl TryFrom<Vec<RespFrame>> for HGet {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "hget", 2)?;

        let mut frame_iter = value.into_iter();
        let key = extract_string(frame_iter.next())?;
        let field = extract_string(frame_iter.next())?;
        Ok(HGet::new(key, field))
    }
}

impl HGet {
    pub fn new(key: String, field: String) -> Self {
        HGet { key, field }
    }
}

#[derive(Debug, PartialEq)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

impl TryFrom<Vec<RespFrame>> for HSet {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "hset", 3)?;

        let mut frame_iter = value.into_iter();
        let key = extract_string(frame_iter.next())?;
        let field = extract_string(frame_iter.next())?;
        let frame_value = extract_frame(frame_iter.next())?;
        Ok(HSet::new(key, field, frame_value))
    }
}

impl HSet {
    pub fn new(key: String, field: String, value: RespFrame) -> Self {
        HSet { key, field, value }
    }
}

#[derive(Debug, PartialEq)]
pub struct HGetAll {
    key: String,
}

impl TryFrom<Vec<RespFrame>> for HGetAll {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "hgetall", 1)?;
        Ok(HGetAll::new(extract_string(value.into_iter().next())?))
    }
}

impl HGetAll {
    pub fn new(key: String) -> Self {
        HGetAll { key }
    }
}

impl TryFrom<Vec<u8>> for HGetAll {
    type Error = CommandError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(HGetAll::new(String::from_utf8(value)?))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, BulkString, RespArray, RespDecode};

    use super::*;

    #[test]
    fn test_hget_try_from() {
        let mut buf =
            BytesMut::from(b"*3\r\n$4\r\nhget\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let hget = Command::try_from(array).unwrap();
        assert_eq!(
            hget,
            Command::HGet(HGet::new("hello".to_string(), "world".to_string()))
        )
    }

    #[test]
    fn test_hset_try_from() {
        let mut buf = BytesMut::from(
            b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let hset = Command::try_from(array).unwrap();
        assert_eq!(
            hset,
            Command::HSet(HSet::new(
                "map".to_string(),
                "hello".to_string(),
                RespFrame::BulkString(BulkString::new(Some(b"world")))
            ))
        )
    }

    #[test]
    fn test_hgetall_try_from() {
        let mut buf = BytesMut::from(b"*2\r\n$7\r\nhgetall\r\n$5\r\nhello\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let hgetall = Command::try_from(array).unwrap();
        assert_eq!(hgetall, Command::HGetAll(HGetAll::new("hello".to_string())))
    }
}
