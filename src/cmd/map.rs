use crate::{
    cmd::{CommandError, Get},
    RespArray, RespFrame,
};

impl TryFrom<RespArray> for Get {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let frames = value
            .0
            .ok_or(CommandError::InvalidCommand("None".to_string()))?;

        if frames.len() != 1 {
            return Err(CommandError::InvalidCommand(format!("{:?}", frames)));
        }
        let frame = frames.into_iter().next().expect("unexpected error!");

        let bulk_string = match frame {
            RespFrame::BulkString(bulk_string) => bulk_string,
            frame => return Err(CommandError::InvalidCommand(format!("{:?}", frame))),
        };

        let content = bulk_string
            .0
            .ok_or(CommandError::InvalidCommand("None".to_string()))?;
        Get::try_from(content)
    }
}
