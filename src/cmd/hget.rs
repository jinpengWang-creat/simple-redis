use crate::RespFrame;

use super::{extract_string, validate_nums_of_argument, CommandError, CommandExecutor, RET_NULL};

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

impl TryFrom<Vec<RespFrame>> for HGet {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "hget", 2, 2)?;

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

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, Backend, BulkString, RespArray, RespDecode};

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
    fn test_cmd_hget_hset() {
        let backend = Backend::new();
        let mut buf = BytesMut::from(
            b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let mut buf =
            BytesMut::from(b"*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        assert_eq!(ret, RespFrame::BulkString(BulkString::new(Some(b"world"))));
    }
}
