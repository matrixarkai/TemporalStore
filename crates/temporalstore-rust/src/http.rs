use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpRequestOptions {
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub max_retries: usize,
}

impl Default for HttpRequestOptions {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 200,
            io_timeout_ms: 200,
            max_retries: 0,
        }
    }
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
    post_json_with_options(addr, path, request, HttpRequestOptions::default())
}

pub fn post_json_with_options<Req: Serialize, Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    request: &Req,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    post_json_with_options_and_headers(addr, path, request, "", options)
}

pub fn post_json_with_options_and_headers<Req: Serialize, Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    request: &Req,
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    let body = serde_json::to_vec(request)?;
    let headers = format!("Content-Type: application/json\r\n{extra_headers}");
    let raw = request_raw_with_options(addr, "POST", path, &body, &headers, options)?;
    Ok(serde_json::from_slice(&raw)?)
}

pub fn get_json<Res: DeserializeOwned>(addr: &str, path: &str) -> Result<Res, HttpError> {
    get_json_with_options(addr, path, HttpRequestOptions::default())
}

pub fn get_json_with_options<Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    let raw = request_raw_with_options(addr, "GET", path, &[], "", options)?;
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
    let content_type = if body.starts_with(b"# HELP ") || body.starts_with(b"# TYPE ") {
        "text/plain; version=0.0.4"
    } else {
        "application/json"
    };
    write!(
        stream,
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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

fn request_raw_with_options(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    let mut last_error = None;
    for _attempt in 0..=options.max_retries {
        match request_raw_once(addr, method, path, body, extra_headers, options) {
            Ok(response) => return Ok(response),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or_else(|| HttpError::BadResponse("request failed".to_string())))
}

fn request_raw_once(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    let connect_timeout = Duration::from_millis(options.connect_timeout_ms);
    let mut addrs = addr.to_socket_addrs()?;
    let socket_addr = addrs
        .next()
        .ok_or_else(|| HttpError::BadResponse(format!("cannot resolve address {addr}")))?;
    let mut stream = TcpStream::connect_timeout(&socket_addr, connect_timeout)?;
    let io_timeout = Some(Duration::from_millis(options.io_timeout_ms));
    stream.set_read_timeout(io_timeout)?;
    stream.set_write_timeout(io_timeout)?;
    let header = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{extra_headers}Connection: close\r\n\r\n",
        body.len()
    );
    write_all_with_would_block_retry(&mut stream, header.as_bytes(), options.io_timeout_ms)?;
    write_all_with_would_block_retry(&mut stream, body, options.io_timeout_ms)?;
    flush_with_would_block_retry(&mut stream, options.io_timeout_ms)?;

    let response = read_http_response(&mut stream, options.io_timeout_ms)?;
    let (header_end, content_length) = parse_response_header(&response)?;
    if response.len() < header_end + b"\r\n\r\n".len() + content_length {
        return Err(HttpError::BadResponse(
            "incomplete response body".to_string(),
        ));
    }
    let marker = b"\r\n\r\n";
    let status_line = String::from_utf8_lossy(&response[..header_end])
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    if !status_line.contains(" 200 ") {
        return Err(HttpError::BadResponse(status_line));
    }
    let body_start = header_end + marker.len();
    Ok(response[body_start..body_start + content_length].to_vec())
}

fn read_http_response(stream: &mut TcpStream, timeout_ms: u64) -> Result<Vec<u8>, HttpError> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms.max(1));
    let marker = b"\r\n\r\n";
    let mut response = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return Ok(response),
            Ok(read) => {
                response.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = response.windows(marker.len()).position(|w| w == marker) {
                    let (_, content_length) = parse_response_header(&response)?;
                    let expected_len = header_end + marker.len() + content_length;
                    if response.len() >= expected_len {
                        response.truncate(expected_len);
                        return Ok(response);
                    }
                }
            }
            Err(err)
                if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
                    && std::time::Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(2));
            }
            Err(err) => return Err(HttpError::Io(err)),
        }
    }
}

fn write_all_with_would_block_retry(
    stream: &mut TcpStream,
    mut bytes: &[u8],
    timeout_ms: u64,
) -> Result<(), HttpError> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms.max(1));
    while !bytes.is_empty() {
        match stream.write(bytes) {
            Ok(0) => {
                return Err(HttpError::BadResponse(
                    "socket closed while writing request".to_string(),
                ));
            }
            Ok(written) => bytes = &bytes[written..],
            Err(err)
                if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
                    && std::time::Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(2));
            }
            Err(err) => return Err(HttpError::Io(err)),
        }
    }
    Ok(())
}

fn flush_with_would_block_retry(stream: &mut TcpStream, timeout_ms: u64) -> Result<(), HttpError> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms.max(1));
    loop {
        match stream.flush() {
            Ok(()) => return Ok(()),
            Err(err)
                if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
                    && std::time::Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(2));
            }
            Err(err) => return Err(HttpError::Io(err)),
        }
    }
}

fn parse_response_header(response: &[u8]) -> Result<(usize, usize), HttpError> {
    let marker = b"\r\n\r\n";
    let Some(header_end) = response.windows(marker.len()).position(|w| w == marker) else {
        return Err(HttpError::BadResponse(
            "missing response header".to_string(),
        ));
    };
    let headers = String::from_utf8_lossy(&response[..header_end]);
    let content_length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or_default();
    Ok((header_end, content_length))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::net::TcpListener;
    use std::time::Instant;

    #[test]
    fn client_returns_after_content_length_without_waiting_for_close() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_http_request(&mut stream).unwrap();
            let body = br#"{"ok":true}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(body).unwrap();
            stream.flush().unwrap();
            thread::sleep(Duration::from_millis(250));
        });

        let started = Instant::now();
        let response: Value = post_json_with_options(
            &addr,
            "/raft/propose",
            &serde_json::json!({"command":"test"}),
            HttpRequestOptions {
                connect_timeout_ms: 100,
                io_timeout_ms: 100,
                max_retries: 0,
            },
        )
        .unwrap();

        assert_eq!(response["ok"], true);
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "client waited for connection close instead of returning after Content-Length"
        );
    }
}
