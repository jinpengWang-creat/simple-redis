use crate::{cmd::RET_NULL, Backend, RespArray, RespFrame};

use super::{extract_string, validate_nums_of_argument, CommandError, CommandExecutor};

#[derive(Debug, PartialEq)]
pub struct Hmget {
    key: String,
    fields: Vec<String>,
}

impl CommandExecutor for Hmget {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend
            .hmget(&self.key, &self.fields)
            .map(|values| {
                let array = values
                    .into_iter()
                    .map(|value| value.unwrap_or(RET_NULL.clone()))
                    .collect();
                RespFrame::Array(RespArray(Some(array)))
            })
            .unwrap_or_else(|| {
                let array: Vec<_> = self.fields.iter().map(|_| RET_NULL.clone()).collect();
                RespFrame::Array(RespArray::new(Some(array)))
            })
    }
}

impl TryFrom<Vec<RespFrame>> for Hmget {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "hmget", value.len(), 2)?;

        let mut frame_iter = value.into_iter();
        let key = extract_string(frame_iter.next())?;

        let mut fields = Vec::with_capacity(frame_iter.len());
        for field in frame_iter {
            fields.push(extract_string(Some(field))?);
        }
        Ok(Hmget::new(key, fields))
    }
}

impl Hmget {
    pub fn new(key: String, fields: Vec<String>) -> Self {
        Self { key, fields }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{cmd::Command, BulkString, RespArray, RespDecode};

    use super::*;

    #[test]
    fn test_hmget_try_from() {
        let mut buf = BytesMut::from(
            b"*4\r\n$5\r\nhmget\r\n$3\r\nmap\r\n$4\r\nname\r\n$3\r\nage\r\n".as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let hget = Command::try_from(array).unwrap();
        assert_eq!(
            hget,
            Command::Hmget(Hmget::new(
                "map".to_string(),
                vec!["name".to_string(), "age".to_string()]
            ))
        )
    }

    #[test]
    fn test_cmd_hmget_hset() {
        let backend = Backend::new();
        let mut buf = BytesMut::from(
            b"*6\r\n$4\r\nhset\r\n$3\r\nmap\r\n$4\r\nname\r\n$3\r\ntom\r\n$3\r\nage\r\n$2\r\n11\r\n".as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(2));

        let mut buf = BytesMut::from(
            b"*4\r\n$5\r\nhmget\r\n$3\r\nmap\r\n$4\r\nname\r\n$3\r\nage\r\n".as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        assert_eq!(
            ret,
            RespFrame::Array(RespArray::new(Some(vec![
                RespFrame::BulkString(BulkString::new(Some("tom".to_string()))),
                RespFrame::BulkString(BulkString::new(Some("11".to_string())))
            ])))
        );

        let mut buf = BytesMut::from(
            b"*5\r\n$5\r\nhmget\r\n$3\r\nmap\r\n$4\r\nname\r\n$3\r\nage\r\n$3\r\nsex\r\n"
                .as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        assert_eq!(
            ret,
            RespFrame::Array(RespArray::new(Some(vec![
                RespFrame::BulkString(BulkString::new(Some("tom".to_string()))),
                RespFrame::BulkString(BulkString::new(Some("11".to_string()))),
                RET_NULL.clone()
            ])))
        );
    }
}
