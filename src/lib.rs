#[allow(dead_code)]
pub mod cmd;
pub mod network;
mod resp;
pub use resp::*;
mod resp2;
pub use resp2::*;
mod backend;
pub use backend::*;

pub const CRLF: &[u8] = b"\r\n";
