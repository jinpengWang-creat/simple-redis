mod hmap;
mod map;
use std::string::FromUtf8Error;

use crate::{Backend, SimpleError};
use crate::{BulkString, RespArray, RespError, RespFrame};
use enum_dispatch::enum_dispatch;
use thiserror::Error;

use self::hmap::*;
use self::map::*;
use lazy_static::lazy_static;

lazy_static! {
    static ref DB: Backend = Backend::new();
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
    if frames.len() < 1 {
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
    let cmd = array_iter.next().expect("unexpect error");

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
