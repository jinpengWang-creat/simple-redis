use std::net::SocketAddr;

use anyhow::Result;
use tokio::{
    io::{self, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};

const BUFF_SIZE: usize = 4096;
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("Dredis listener on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("accepted connection from: {:?}", addr);
        tokio::spawn(async move {
            if let Err(e) = process_redis_conn(stream, addr).await {
                warn!("Error processing connection: {:?}", e);
            }
        });
    }
}

async fn process_redis_conn(mut stream: TcpStream, addr: SocketAddr) -> Result<()> {
    loop {
        stream.readable().await?;
        let mut buff = Vec::with_capacity(BUFF_SIZE);

        match stream.try_read_buf(&mut buff) {
            Ok(0) => break,
            Ok(n) => {
                info!("read {} bytes", n);
                let line = String::from_utf8_lossy(&buff);
                info!("{:?}", line);
                stream.write_all(b"+OK\r\n").await?;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    warn!("Connection {} closed", addr);
    Ok(())
}
