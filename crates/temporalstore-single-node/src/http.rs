use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bad response: {0}")]
    BadResponse(String),
}

#[derive(Debug)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
}

pub fn serve(
    addr: &str,
    handler: impl Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
) -> Result<(), HttpError> {
    let listener = TcpListener::bind(addr)?;
    let handler = Arc::new(handler);
    for stream in listener.incoming() {
        let stream = stream?;
        let handler = Arc::clone(&handler);
        thread::spawn(move || {
            let _ = handle_stream(stream, &*handler);
        });
    }
    Ok(())
}

pub fn json_response<T: Serialize>(status: u16, value: &T) -> (u16, Vec<u8>) {
    (
        status,
        serde_json::to_vec(value).unwrap_or_else(|_| b"{\"ok\":false}".to_vec()),
    )
}

pub fn parse_json<T: DeserializeOwned>(body: &[u8]) -> Result<T, HttpError> {
    Ok(serde_json::from_slice(body)?)
}

pub fn post_json<Req: Serialize, Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    request: &Req,
) -> Result<Res, HttpError> {
    let body = serde_json::to_vec(request)?;
    let raw = request_raw(
        addr,
        "POST",
        path,
        &body,
        "Content-Type: application/json\r\n",
    )?;
    Ok(serde_json::from_slice(&raw)?)
}

pub fn get_json<Res: DeserializeOwned>(addr: &str, path: &str) -> Result<Res, HttpError> {
    let raw = request_raw(addr, "GET", path, &[], "")?;
    Ok(serde_json::from_slice(&raw)?)
}

fn handle_stream(
    mut stream: TcpStream,
    handler: &dyn Fn(HttpRequest) -> (u16, Vec<u8>),
) -> Result<(), HttpError> {
    let buffer = read_http_request(&mut stream)?;
    let request = parse_request(&buffer)?;
    let (status, body) = handler(request);
    let status_text = if status == 200 { "OK" } else { "ERROR" };
    write!(
        stream,
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn read_http_request(stream: &mut TcpStream) -> Result<Vec<u8>, HttpError> {
    let marker = b"\r\n\r\n";
    let mut buffer = Vec::new();
    let mut chunk = [0; 4096];
    let mut expected_len = None;

    loop {
        let size = stream.read(&mut chunk)?;
        if size == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..size]);
        if expected_len.is_none() {
            if let Some(header_end) = buffer.windows(marker.len()).position(|w| w == marker) {
                let headers = String::from_utf8_lossy(&buffer[..header_end]);
                let content_length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                    .unwrap_or_default();
                expected_len = Some(header_end + marker.len() + content_length);
            }
        }
        if expected_len.is_some_and(|len| buffer.len() >= len) {
            break;
        }
    }
    Ok(buffer)
}

fn parse_request(bytes: &[u8]) -> Result<HttpRequest, HttpError> {
    let marker = b"\r\n\r\n";
    let Some(header_end) = bytes.windows(marker.len()).position(|w| w == marker) else {
        return Err(HttpError::BadResponse(
            "missing header terminator".to_string(),
        ));
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = headers.lines();
    let Some(request_line) = lines.next() else {
        return Err(HttpError::BadResponse("missing request line".to_string()));
    };
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or_default();
    let body_start = header_end + marker.len();
    let body_end = body_start.saturating_add(content_length).min(bytes.len());
    Ok(HttpRequest {
        method,
        path,
        body: bytes[body_start..body_end].to_vec(),
    })
}

fn request_raw(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra_headers: &str,
) -> Result<Vec<u8>, HttpError> {
    let mut stream = TcpStream::connect(addr)?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{extra_headers}Connection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let marker = b"\r\n\r\n";
    let Some(header_end) = response.windows(marker.len()).position(|w| w == marker) else {
        return Err(HttpError::BadResponse(
            "missing response header".to_string(),
        ));
    };
    let status_line = String::from_utf8_lossy(&response[..header_end])
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    if !status_line.contains(" 200 ") {
        return Err(HttpError::BadResponse(status_line));
    }
    Ok(response[header_end + marker.len()..].to_vec())
}
