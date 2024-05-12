use crate::{
    cmd::{CommandError, Get},
    RespArray,
};

impl TryFrom<RespArray> for Get {
    type Error = CommandError;

    fn try_from(_value: RespArray) -> Result<Self, Self::Error> {
        todo!()
    }
}
