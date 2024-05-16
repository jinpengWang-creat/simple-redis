use crate::RespFrame;

use super::{
    extract_frame, extract_string, validate_nums_of_argument, CommandError, CommandExecutor,
};

#[derive(Debug, PartialEq)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value)
    }
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

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, BulkString, RespArray, RespDecode};

    use super::*;

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
}
