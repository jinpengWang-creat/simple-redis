use crate::RespFrame;

use super::{CommandError, HGet};

impl TryFrom<RespFrame> for HGet {
    type Error = CommandError;

    fn try_from(_value: RespFrame) -> Result<Self, Self::Error> {
        todo!()
    }
}
