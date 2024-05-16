use crate::{Backend, BulkString, RespFrame};

use super::{extract_string, validate_nums_of_argument, CommandError, CommandExecutor};

#[derive(Debug, PartialEq)]

pub struct Echo {
    message: String,
}

impl CommandExecutor for Echo {
    fn execute(self, _backend: &Backend) -> RespFrame {
        RespFrame::BulkString(BulkString::new(Some(Into::<Vec<_>>::into(self.message))))
    }
}

impl TryFrom<Vec<RespFrame>> for Echo {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_nums_of_argument(&value, "echo", 1)?;
        Ok(Echo::new(extract_string(value.into_iter().next())?))
    }
}

impl Echo {
    pub fn new(message: String) -> Self {
        Echo { message }
    }
}
