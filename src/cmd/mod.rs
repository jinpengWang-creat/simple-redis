mod echo;
mod get;
mod hget;
mod hgetall;
mod hmget;
mod hset;
mod sadd;
mod set;
mod sismember;
use std::string::FromUtf8Error;

use crate::Backend;
use crate::BulkString;
use crate::RespArray;
use crate::RespError;
use crate::RespFrame;
use crate::SimpleString;
use enum_dispatch::enum_dispatch;
use thiserror::Error;

use self::echo::*;
use self::get::Get;
use self::hget::HGet;
use self::hgetall::HGetAll;
use self::hmget::Hmget;
use self::hset::HSet;
use self::sadd::Sadd;
use self::set::Set;
use self::sismember::Sismember;
use lazy_static::lazy_static;

lazy_static! {
    static ref RET_NULL: RespFrame = RespFrame::BulkString(BulkString::new(None::<Vec<_>>));
    static ref RET_NULL_ARRAY: RespFrame = RespFrame::Array(RespArray::new(Some([])));
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
    Hmget(Hmget),
    HSet(HSet),
    HGetAll(HGetAll),
    Sadd(Sadd),
    Sismember(Sismember),
    Echo(Echo),
    Unrecognized(Unrecognized),
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
            b"get" => Ok(Get::try_from(frames)?.into()),
            b"set" => Ok(Set::try_from(frames)?.into()),
            b"hget" => Ok(HGet::try_from(frames)?.into()),
            b"hmget" => Ok(Hmget::try_from(frames)?.into()),
            b"hset" => Ok(HSet::try_from(frames)?.into()),
            b"hgetall" => Ok(HGetAll::try_from(frames)?.into()),
            b"sadd" => Ok(Sadd::try_from(frames)?.into()),
            b"sismember" => Ok(Sismember::try_from(frames)?.into()),
            b"echo" => Ok(Echo::try_from(frames)?.into()),
            _ => Ok(Unrecognized.into()),
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
    min_num: usize,
) -> Result<(), CommandError> {
    if frames.len() < min_num {
        return Err(CommandError::InvalidArgument(format!(
            "wrong number of arguments for '{}' command, at least: {}, got: {}",
            validate_type,
            min_num,
            frames.len()
        )));
    }
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

#[derive(Debug, PartialEq)]
pub struct Unrecognized;
impl CommandExecutor for Unrecognized {
    fn execute(self, _backend: &Backend) -> RespFrame {
        RET_OK.clone()
    }
}
