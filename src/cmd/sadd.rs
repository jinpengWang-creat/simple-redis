use crate::RespFrame;

use super::{extract_string, validate_nums_of_argument, CommandError, CommandExecutor, RET_NULL};

#[derive(Debug, PartialEq)]
pub struct Sadd {
    key: String,
    field: String,
}

impl CommandExecutor for Sadd {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.sadd(self.key, self.field, RET_NULL.clone())
    }
}

impl TryFrom<Vec<RespFrame>> for Sadd {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "sadd", 2, 2)?;
        let mut frame_iter = value.into_iter();
        let key = extract_string(frame_iter.next())?;
        let field = extract_string(frame_iter.next())?;
        Ok(Sadd::new(key, field))
    }
}

impl Sadd {
    pub fn new(key: String, field: String) -> Self {
        Sadd { key, field }
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
            Command::Sadd(Sadd::new("set".to_string(), "one".to_string()))
        )
    }
}
