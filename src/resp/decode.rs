use bytes::BytesMut;

use crate::{RespDecode, RespFrame};

impl RespDecode for BytesMut {
    fn decode(self) -> Result<RespFrame, String> {
        let (symbol, _content) = self.split_at(1);
        match symbol {
            b"+" => todo!(),
            _ => todo!(),
        }
    }
}
