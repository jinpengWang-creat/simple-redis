use bytes::BytesMut;

use crate::{RespDecode, RespFrame, RespNull, SimpleError, SimpleString};

impl RespDecode for BytesMut {
    fn decode(self) -> Result<RespFrame, String> {
        let (symbol, content) = self.split_at(1);
        match symbol {
            b"+" => parse_simple_string(content.into()),
            b"-" => parse_simple_error(content.into()),
            b":" => parse_integer(content.into()),
            b"_" => parse_null(content.into()),
            b"#" => parse_bool(content.into()),
            b"," => parse_double(content.into()),
            b"$" => todo!(),
            b"*" => todo!(),
            b"%" => todo!(),
            b"~" => todo!(),
            _ => Err(format!(
                "unsupported type: {:?}",
                String::from_utf8_lossy(symbol)
            )),
        }
    }
}

fn parse_simple_string(bytes: BytesMut) -> Result<RespFrame, String> {
    let bytes = bytes.strip_suffix(b"\r\n").ok_or_else(|| "error format!")?;
    let content = String::from_utf8(bytes.to_vec()).map_err(|e| format!("error: {:?}", e))?;
    Ok(RespFrame::SimpleString(SimpleString::new(content)))
}

fn parse_simple_error(bytes: BytesMut) -> Result<RespFrame, String> {
    let bytes = bytes.strip_suffix(b"\r\n").ok_or_else(|| "error format!")?;
    let content = String::from_utf8(bytes.to_vec()).map_err(|e| format!("error: {:?}", e))?;
    Ok(RespFrame::SimpleError(SimpleError::new(content)))
}

fn parse_integer(bytes: BytesMut) -> Result<RespFrame, String> {
    let bytes = bytes.strip_suffix(b"\r\n").ok_or_else(|| "error format!")?;
    let content = String::from_utf8(bytes.to_vec()).map_err(|e| format!("error: {:?}", e))?;
    Ok(RespFrame::Integer(
        content.parse().map_err(|e| format!("error: {:?}", e))?,
    ))
}

fn parse_null(_bytes: BytesMut) -> Result<RespFrame, String> {
    Ok(RespFrame::Null(RespNull))
}

fn parse_bool(bytes: BytesMut) -> Result<RespFrame, String> {
    let bytes = bytes.strip_suffix(b"\r\n").ok_or_else(|| "error format!")?;
    let content = String::from_utf8(bytes.to_vec()).map_err(|e| format!("error: {:?}", e))?;
    Ok(RespFrame::Boolean(if content == "t" {
        true
    } else {
        false
    }))
}

fn parse_double(bytes: BytesMut) -> Result<RespFrame, String> {
    let bytes = bytes.strip_suffix(b"\r\n").ok_or_else(|| "error format!")?;
    let content = String::from_utf8(bytes.to_vec()).map_err(|e| format!("error: {:?}", e))?;
    Ok(RespFrame::Double(
        content.parse().map_err(|e| format!("error: {:?}", e))?,
    ))
}

fn parse_bulk_string(bytes: BytesMut) -> Result<RespFrame, String> {
    todo!()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let s: BytesMut = "-hello\r\n".into();
        let ss = s.decode();
        println!("{:?}", ss);
    }

    #[test]
    fn test_slice_chunk() {
        let s = b"hello\r\nworld\r\nthe\r\n";

        let ss = s.chunk_by(|a, b| *a != b'\r' || *b != b'\n');

        for s in ss {
            println!("{:?}", String::from_utf8_lossy(s));
        }
    }
}
