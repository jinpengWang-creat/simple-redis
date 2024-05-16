use crate::RespFrame;

use super::{
    extract_frame, extract_string, validate_nums_of_argument, CommandError, CommandExecutor, RET_OK,
};

#[derive(Debug, PartialEq)]
pub struct Set {
    key: String,
    value: RespFrame,
}

impl CommandExecutor for Set {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.set(self.key, self.value);
        RET_OK.clone()
    }
}

impl TryFrom<Vec<RespFrame>> for Set {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "set", 2, 2)?;
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
