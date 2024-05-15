use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{error, info};

use crate::{
    cmd::{Command, CommandExecutor},
    Backend, RespDecode, RespEncode, RespError, RespFrame, SimpleError,
};

#[derive(Debug)]
pub struct RedisRequest {
    frame: RespFrame,
    backend: Backend,
}

#[derive(Debug)]
pub struct RedisResponse {
    frame: RespFrame,
}

pub struct RespFrameCodec;

impl Encoder<RespFrame> for RespFrameCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut bytes::BytesMut) -> Result<()> {
        let encoded = item.encode();
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;

    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>> {
        info!("decode {:?}", src);
        match RespFrame::decode(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(RespError::NotComplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

pub async fn stream_handler(stream: TcpStream, backend: Backend) -> Result<()> {
    let mut framed = Framed::new(stream, RespFrameCodec);

    loop {
        let cloned_backend = backend.clone();
        match framed.next().await {
            Some(Ok(frame)) => {
                info!("Receive frame: {:?}", frame);
                let request = RedisRequest {
                    frame,
                    backend: cloned_backend,
                };
                info!("Executing request: {:?}", request);
                let response = redis_request_handler(request).await?;
                info!("get response: {:?}", response);
                framed.send(response.frame).await?;
            }
            Some(Err(e)) => {
                error!("error for {:?}", e);
                continue;
            }
            None => return Ok(()),
        }
    }
}

async fn redis_request_handler(request: RedisRequest) -> Result<RedisResponse> {
    let frame = match TryInto::<Command>::try_into(request.frame) {
        Ok(cmd) => cmd.execute(&request.backend),
        Err(e) => RespFrame::SimpleError(SimpleError::new(e.to_string())),
    };
    Ok(RedisResponse { frame })
}
