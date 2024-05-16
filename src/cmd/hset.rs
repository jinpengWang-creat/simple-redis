use crate::RespFrame;

use super::{
    extract_frame, extract_string, validate_nums_of_argument, CommandError, CommandExecutor,
};

#[derive(Debug, PartialEq)]
pub struct HSet {
    key: String,
    fields: Vec<String>,
    values: Vec<RespFrame>,
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hmset(self.key, self.fields, self.values)
    }
}

impl TryFrom<Vec<RespFrame>> for HSet {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        let mut expect_len = value.len() - 1;
        if expect_len % 2 == 1 {
            expect_len += 1;
        }
        validate_nums_of_argument(&value, "hset", expect_len + 1, 3)?;
        let mut frame_iter = value.into_iter();
        let mut fields = Vec::with_capacity(expect_len / 2);
        let mut values = Vec::with_capacity(expect_len / 2);
        let key = extract_string(frame_iter.next())?;
        for _ in 0..expect_len / 2 {
            fields.push(extract_string(frame_iter.next())?);
            values.push(extract_frame(frame_iter.next())?);
        }

        Ok(HSet::new(key, fields, values))
    }
}

impl HSet {
    pub fn new(key: String, fields: Vec<String>, values: Vec<RespFrame>) -> Self {
        HSet {
            key,
            fields,
            values,
        }
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
                vec!["hello".to_string()],
                vec![RespFrame::BulkString(BulkString::new(Some(b"world")))]
            ))
        )
    }
}
