use crate::{Backend, BulkString, RespFrame};

use super::{extract_string, validate_nums_of_argument, CommandError, CommandExecutor};

#[derive(Debug, PartialEq)]

pub struct Echo {
    message: String,
}

impl CommandExecutor for Echo {
    fn execute(self, _backend: &Backend) -> RespFrame {
        RespFrame::BulkString(BulkString::new(Some(Into::<Vec<_>>::into(self.message))))
    }
}

impl TryFrom<Vec<RespFrame>> for Echo {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "echo", 1, 1)?;
        Ok(Echo::new(extract_string(value.into_iter().next())?))
    }
}

impl Echo {
    pub fn new(message: String) -> Self {
        Echo { message }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, RespArray, RespDecode};

    use super::*;

    #[test]
    fn test_echo_try_from() {
        let mut buf = BytesMut::from(b"*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let hget = Command::try_from(array).unwrap();
        assert_eq!(hget, Command::Echo(Echo::new("hello world".to_string())))
    }
}
