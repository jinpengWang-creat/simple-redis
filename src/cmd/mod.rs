mod hmap;
mod map;
use crate::{BulkString, RespArray, RespError, RespFrame};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0}")]
    REspError(#[from] RespError),
}

pub trait CommandExecutor {
    fn execute(self) -> RespFrame;
}
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
}

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Self {
        Get { key }
    }
}

impl TryFrom<Vec<u8>> for Get {
    type Error = CommandError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Get::new(String::from_utf8(value).map_err(|e| {
            CommandError::InvalidArgument(format!("{:?}", e))
        })?))
    }
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGet {
    key: String,
    field: String,
}

#[derive(Debug)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,
}

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;

    fn try_from(_value: RespFrame) -> Result<Self, Self::Error> {
        todo!()
    }
}

fn validate_command(
    resp_array: &RespArray,
    cmds: &[&'static str],
    n_args: usize,
) -> Result<(), CommandError> {
    let frames = resp_array.as_deref().ok_or(CommandError::InvalidCommand(
        "This array is null array".to_string(),
    ))?;
    if frames.len() != cmds.len() + n_args {
        return Err(CommandError::InvalidArgument(format!(
            "{} command must have exactly {} arguments",
            cmds.join(" "),
            n_args
        )));
    }

    for (cmd, frame) in cmds.iter().zip(frames.iter()) {
        match frame {
            RespFrame::BulkString(BulkString(Some(frame_cmd))) => {
                if frame_cmd.to_ascii_lowercase() != cmd.as_bytes() {
                    return Err(CommandError::InvalidCommand(format!(
                        "Invalid command: expected {}, got {}",
                        cmd,
                        String::from_utf8_lossy(frame_cmd)
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must have a BulkString as the first argument".to_string(),
                ))
            }
        }
    }
    Ok(())
}
