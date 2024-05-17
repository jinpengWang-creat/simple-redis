use crate::RespFrame;

use super::{extract_string, validate_nums_of_argument, CommandError, CommandExecutor};

#[derive(Debug, PartialEq)]
pub struct Sadd {
    key: String,
    fields: Vec<String>,
}

impl CommandExecutor for Sadd {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.sadd(self.key, self.fields)
    }
}

impl TryFrom<Vec<RespFrame>> for Sadd {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "sadd", value.len(), 2)?;
        let mut frame_iter = value.into_iter();
        let key = extract_string(frame_iter.next())?;
        let mut fields = Vec::with_capacity(frame_iter.len());
        for frame in frame_iter {
            let field = extract_string(Some(frame))?;
            fields.push(field);
        }
        Ok(Sadd::new(key, fields))
    }
}

impl Sadd {
    pub fn new(key: String, fields: Vec<String>) -> Self {
        Sadd { key, fields }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, RespArray, RespDecode};

    use super::*;

    #[test]
    fn test_sadd_try_from() {
        let mut buf = BytesMut::from(b"*3\r\n$4\r\nsadd\r\n$3\r\nset\r\n$3\r\none\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let sadd = Command::try_from(array).unwrap();
        assert_eq!(
            sadd,
            Command::Sadd(Sadd::new("set".to_string(), vec!["one".to_string()]))
        )
    }
}
