use anyhow::Result;
use simple_redis::{network, Backend};
use tokio::net::TcpListener;
use tracing::{info, warn};
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "0.0.0.0:6379";
    info!("Simple-redis-server is Listening on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    let backend = Backend::new();
    loop {
        let (stream, raddr) = listener.accept().await?;
        let cloned_backend = backend.clone();
        info!("Accepted connection from: {}", raddr);
        tokio::spawn(async move {
            if let Err(e) = network::stream_handler(stream, cloned_backend).await {
                warn!("handle error for {}: {:?}", raddr, e);
            }
        });
    }
}
