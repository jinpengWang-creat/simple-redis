use crate::RespFrame;

use super::{
    extract_string, validate_nums_of_argument, CommandError, CommandExecutor, RET_NULL_ARRAY,
};

#[derive(Debug, PartialEq)]
pub struct HGetAll {
    key: String,
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hgetall(&self.key).unwrap_or(RET_NULL_ARRAY.clone())
    }
}

impl TryFrom<Vec<RespFrame>> for HGetAll {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "hgetall", 1, 1)?;
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

    use crate::{cmd::Command, Backend, BulkString, RespArray, RespDecode};

    use super::*;

    #[test]
    fn test_hgetall_try_from() {
        let mut buf = BytesMut::from(b"*2\r\n$7\r\nhgetall\r\n$5\r\nhello\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let hgetall = Command::try_from(array).unwrap();
        assert_eq!(hgetall, Command::HGetAll(HGetAll::new("hello".to_string())))
    }

    #[test]
    fn test_cmd_hgetall() {
        let backend = Backend::new();
        let mut buf = BytesMut::from(
            b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let mut buf = BytesMut::from(
            b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$4\r\nname\r\n$3\r\ntom\r\n".as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let mut buf = BytesMut::from(b"*2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        let vec = vec![
            BulkString::new(Some("hello")).into(),
            RespFrame::BulkString(BulkString::new(Some("world"))),
            BulkString::new(Some("name")).into(),
            RespFrame::BulkString(BulkString::new(Some("tom"))),
        ];

        assert_eq!(ret, RespFrame::Array(RespArray::new(Some(vec))));
    }
}
