use crate::RespFrame;

use super::{extract_string, validate_nums_of_argument, CommandError, CommandExecutor};

#[derive(Debug, PartialEq)]
pub struct Sismember {
    key: String,
    field: String,
}

impl CommandExecutor for Sismember {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.sismember(&self.key, &self.field)
    }
}

impl TryFrom<Vec<RespFrame>> for Sismember {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "sismember", 2, 2)?;
        let mut frame_iter = value.into_iter();
        let key = extract_string(frame_iter.next())?;
        let field = extract_string(frame_iter.next())?;
        Ok(Sismember::new(key, field))
    }
}

impl Sismember {
    pub fn new(key: String, field: String) -> Self {
        Sismember { key, field }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, Backend, RespArray, RespDecode};

    use super::*;

    #[test]
    fn test_sismember_try_from() {
        let mut buf =
            BytesMut::from(b"*3\r\n$9\r\nsismember\r\n$3\r\nset\r\n$3\r\none\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let sadd = Command::try_from(array).unwrap();
        assert_eq!(
            sadd,
            Command::Sismember(Sismember::new("set".to_string(), "one".to_string()))
        )
    }

    #[test]
    fn test_cmd_sismember_sadd() {
        let backend = Backend::new();
        let mut buf = BytesMut::from(b"*3\r\n$4\r\nsadd\r\n$3\r\nset\r\n$3\r\none\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let mut buf = BytesMut::from(b"*3\r\n$4\r\nsadd\r\n$3\r\nset\r\n$3\r\none\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(0));

        let mut buf =
            BytesMut::from(b"*3\r\n$9\r\nsismember\r\n$3\r\nset\r\n$3\r\none\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let mut buf =
            BytesMut::from(b"*3\r\n$9\r\nsismember\r\n$3\r\nset\r\n$3\r\ntwo\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(0));
    }
}
