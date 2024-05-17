use crate::RespFrame;

use super::{extract_string, validate_nums_of_argument, CommandError, CommandExecutor, RET_NULL};

#[derive(Debug, PartialEq)]
pub struct Get {
    key: String,
}

impl CommandExecutor for Get {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(val) => val,
            None => RET_NULL.clone(),
        }
    }
}

impl TryFrom<Vec<RespFrame>> for Get {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "get", 1, 1)?;
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

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, Backend, BulkString, RespArray, RespDecode, SimpleString};

    use super::*;

    #[test]
    fn test_get_try_from() {
        let mut buf = BytesMut::from(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        assert_eq!(get, Command::Get(Get::new("hello".to_string())))
    }

    #[test]
    fn test_cmd_get_set() {
        let backend = Backend::new();
        let mut buf =
            BytesMut::from(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::SimpleString(SimpleString::new("OK")));

        let mut buf = BytesMut::from(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        assert_eq!(ret, RespFrame::BulkString(BulkString::new(Some(b"world"))));
    }
}
