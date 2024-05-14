mod hmap;
mod map;
use std::string::FromUtf8Error;

use crate::{Backend, RespNull, SimpleError, SimpleString};
use crate::{BulkString, RespArray, RespError, RespFrame};
use enum_dispatch::enum_dispatch;
use thiserror::Error;

use self::hmap::*;
use self::map::*;
use lazy_static::lazy_static;

lazy_static! {
    static ref DB: Backend = Backend::new();
    static ref RET_NULL: RespFrame = RespFrame::Null(RespNull::new());
    static ref RET_OK: RespFrame = RespFrame::SimpleString(SimpleString::new("OK"));
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0}")]
    RespError(#[from] RespError),
    #[error("{0}")]
    FromUtf8Error(#[from] FromUtf8Error),
}

#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(self, backend: &Backend) -> RespFrame;
}

#[enum_dispatch(CommandExecutor)]
#[derive(Debug, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
}

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;

    fn try_from(value: RespFrame) -> Result<Self, Self::Error> {
        match value {
            RespFrame::Array(array) => Ok(array.try_into()?),
            _ => Err(CommandError::InvalidCommand(format!(
                "unsupported frame: {:?}",
                value
            ))),
        }
    }
}

impl TryFrom<RespArray> for Command {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value)?;
        let (cmd, frames) = extract_cmd_and_argument(value);
        println!("{:?}", frames);
        match cmd.as_slice() {
            b"get" => Ok(Command::Get(Get::try_from(frames)?)),
            b"set" => Ok(Command::Set(Set::try_from(frames)?)),
            b"hget" => Ok(Command::HGet(HGet::try_from(frames)?)),
            b"hset" => Ok(Command::HSet(HSet::try_from(frames)?)),
            b"hgetall" => Ok(Command::HGetAll(HGetAll::try_from(frames)?)),
            _ => Err(CommandError::InvalidCommand(format!(
                "unsupported command: {}",
                String::from_utf8(cmd)?
            ))),
        }
    }
}

fn validate_command(resp_array: &RespArray) -> Result<(), CommandError> {
    // test if the array is a null array
    let frames = resp_array.as_deref().ok_or(CommandError::InvalidCommand(
        "This array is null array!".to_string(),
    ))?;

    // test if the array have at least one argument
    if frames.is_empty() {
        return Err(CommandError::InvalidCommand(
            "Command must have at least one argument!".to_string(),
        ));
    }

    // test if all of RespFrame in array are a BulkString
    for frame in frames.iter() {
        match frame {
            RespFrame::BulkString(BulkString(Some(_))) => continue,
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must have a BulkString as the first argument".to_string(),
                ))
            }
        }
    }
    Ok(())
}

fn extract_cmd_and_argument(array: RespArray) -> (Vec<u8>, Vec<RespFrame>) {
    let mut array_iter = match array {
        RespArray(Some(array)) => array.into_iter(),
        _ => unreachable!(),
    };
    let cmd = array_iter.next().expect("unexpected error");

    let cmd = match cmd {
        RespFrame::BulkString(BulkString(Some(cmd))) => cmd,
        _ => unreachable!(),
    };
    (cmd.to_ascii_lowercase(), array_iter.collect())
}

fn extract_string(frame: Option<RespFrame>) -> Result<String, CommandError> {
    frame
        .map(|f| match f {
            RespFrame::BulkString(BulkString(Some(key))) => String::from_utf8(key).ok(),
            _ => None,
        })
        .ok_or(CommandError::InvalidCommand("None".to_string()))?
        .ok_or(CommandError::InvalidCommand("None".to_string()))
}

fn extract_frame(frame: Option<RespFrame>) -> Result<RespFrame, CommandError> {
    frame.ok_or(CommandError::InvalidCommand("None".to_string()))
}

fn validate_nums_of_argument(
    frames: &Vec<RespFrame>,
    validate_type: &str,
    expect_num: usize,
) -> Result<(), CommandError> {
    if frames.len() != expect_num {
        return Err(CommandError::InvalidArgument(format!(
            "wrong number of arguments for '{}' command, expect: {}, got: {}",
            validate_type,
            expect_num,
            frames.len()
        )));
    }
    Ok(())
}

pub fn cmd(array: RespArray) -> RespFrame {
    let cmd = match Command::try_from(array) {
        Ok(cmd) => cmd,
        Err(e) => return RespFrame::SimpleError(SimpleError::new(format!("{:?}", e))),
    };
    cmd.execute(&DB)
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::{RespDecode, RespMap, SimpleString};

    use super::*;

    #[test]
    fn test_cmd_get_set() {
        let backend = Backend::new();
        let mut buf =
            BytesMut::from(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::SimpleString(SimpleString::new("OK")));

        let mut buf = BytesMut::from(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        assert_eq!(ret, RespFrame::BulkString(BulkString::new(Some(b"world"))));
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
        assert_eq!(ret, RespFrame::SimpleString(SimpleString::new("OK")));

        let mut buf =
            BytesMut::from(b"*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        assert_eq!(ret, RespFrame::BulkString(BulkString::new(Some(b"world"))));
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
        assert_eq!(ret, RespFrame::SimpleString(SimpleString::new("OK")));

        let mut buf = BytesMut::from(
            b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$4\r\nname\r\n$3\r\ntom\r\n".as_slice(),
        );
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let cmd = Command::try_from(array).unwrap();
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::SimpleString(SimpleString::new("OK")));

        let mut buf = BytesMut::from(b"*2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n".as_slice());
        let array = RespArray::decode(&mut buf).expect("error in decode resp array");
        let get = Command::try_from(array).unwrap();
        let ret = get.execute(&backend);
        let mut map = RespMap::new();
        map.insert(
            SimpleString::new("hello"),
            RespFrame::BulkString(BulkString::new(Some("world"))),
        );
        map.insert(
            SimpleString::new("name"),
            RespFrame::BulkString(BulkString::new(Some("tom"))),
        );
        assert_eq!(ret, RespFrame::Map(map));
    }
}
