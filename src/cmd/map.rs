use crate::{cmd::CommandError, RespFrame, RespNull, SimpleString};

use super::{extract_frame, extract_string, validate_nums_of_argument, CommandExecutor};

#[derive(Debug, PartialEq)]
pub struct Get {
    key: String,
}

impl CommandExecutor for Get {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.map.get(&self.key) {
            Some(val) => val.value().clone(),
            None => RespFrame::Null(RespNull::new()),
        }
    }
}

impl TryFrom<Vec<RespFrame>> for Get {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "get", 1)?;
        Ok(Get::new(extract_string(value.into_iter().next())?))
    }
}

impl Get {
    pub fn new(key: String) -> Self {
        Get { key }
    }
}

impl From<String> for Get {
    fn from(value: String) -> Self {
        Get::new(value)
    }
}

impl TryFrom<Vec<u8>> for Get {
    type Error = CommandError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Get::new(String::from_utf8(value).map_err(|e| {
            CommandError::InvalidArgument(format!("{:?}", e))
        })?))
    }
}

#[derive(Debug, PartialEq)]
pub struct Set {
    key: String,
    value: RespFrame,
}

impl CommandExecutor for Set {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.map.entry(self.key).or_insert(self.value);
        RespFrame::SimpleString(SimpleString::new(String::from("OK")))
    }
}
impl TryFrom<Vec<RespFrame>> for Set {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "set", 2)?;
        let mut frame_iter = value.into_iter();
        let key = extract_string(frame_iter.next())?;
        let frame_value = extract_frame(frame_iter.next())?;
        Ok(Set::new(key, frame_value))
    }
}

impl Set {
    pub fn new(key: String, value: RespFrame) -> Self {
        Set { key, value }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, BulkString, RespArray, RespDecode};

    use super::*;

    #[test]
    fn test_get_try_from() {
        let mut buf = BytesMut::from(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        assert_eq!(get, Command::Get(Get::new("hello".to_string())))
    }

    #[test]
    fn test_set_try_from() {
        let mut buf =
            BytesMut::from(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let set = Command::try_from(array).unwrap();
        let mut buf = BytesMut::from(b"$5\r\nworld\r\n".as_slice());
        let value = BulkString::decode(&mut buf).unwrap();
        assert_eq!(
            set,
            Command::Set(Set::new("hello".to_string(), RespFrame::BulkString(value)))
        )
    }
}
