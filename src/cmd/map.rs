use crate::{
    cmd::{CommandError, Get},
    RespArray, RespFrame,
};

impl TryFrom<RespArray> for Get {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        // test if the array is null array
        let frames = value.0.ok_or(CommandError::InvalidCommand(
            "This array is null array".to_string(),
        ))?;

        if frames.len() != 2 {
            return Err(CommandError::InvalidArgument(
                "GET command must have exactly 1 argument!".to_string(),
            ));
        }
        let mut frame_iter = frames.into_iter();

        let cmd = match frame_iter.next() {
            Some(RespFrame::BulkString(cmd)) => cmd,
            frame => return Err(CommandError::InvalidCommand(format!("{:?}", frame))),
        };

        let content = cmd
            .0
            .ok_or(CommandError::InvalidCommand("None".to_string()))?;
        Get::try_from(content)
    }
}
