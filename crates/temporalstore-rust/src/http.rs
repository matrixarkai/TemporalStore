// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Idle read timeout for a kept-alive server connection. A persistent client
/// connection parks here between requests; if nothing arrives for this long the
/// server reaps the connection (the client transparently reconnects).
const SERVER_KEEPALIVE_IDLE_MS: u64 = 120_000;
/// Max idle client sockets pooled per destination address, per thread.
const CLIENT_POOL_MAX_PER_ADDR: usize = 8;

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

impl HttpError {
    /// Whether this error proves the request never reached the server.
    ///
    /// Only a refused connection proves it: the peer rejected the TCP handshake, so not one
    /// byte of the request was sent. Everything else -- a read timeout above all -- means
    /// the server stopped answering, which is not the same as never having heard the
    /// request. That distinction is what decides whether a write may be sent again.
    ///
    /// Deliberately conservative. A connect timeout is indistinguishable from a read
    /// timeout here (both surface as `TimedOut`), so it is treated as "unknown" and a write
    /// is not repeated after one.
    pub fn request_never_reached_the_server(&self) -> bool {
        matches!(self, HttpError::Io(err) if err.kind() == ErrorKind::ConnectionRefused)
    }
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

/// Parsed HTTP request head (request line + headers), read off the socket
/// *before* the body. A streaming handler inspects this to decide whether to
/// take a request without the body ever being buffered.
#[derive(Debug, Clone)]
pub struct RequestHead {
    pub method: String,
    pub path: String,
    pub content_length: usize,
    pub keep_alive: bool,
    /// True when the request arrived carrying the blob peer-fetch loop-guard
    /// header (`X-Ts-Blob-Peer-Fetch: 0`). Such a request is itself a
    /// cross-peer blob fetch hop and MUST be served local-only — never
    /// re-forwarded to another peer — otherwise peers would fetch from each
    /// other forever. Parsed generically here so the streaming blob handler can
    /// enforce the guard without re-reading the socket headers.
    pub blob_peer_fetch_loop_guard: bool,
    /// The `Authorization: Bearer <token>` credential, when the request carried
    /// one. Parsed generically here so a serving layer that requires a token
    /// (the metaserver's admin surface) can enforce it at the head stage,
    /// before any body byte is read.
    pub bearer_token: Option<String>,
}

/// What a streaming handler did with a request.
pub enum StreamAction {
    /// Fully handled: the streaming handler consumed the entire request body and
    /// wrote the complete response itself. The connection then stays alive (or
    /// closes) per `RequestHead::keep_alive`.
    Handled,
    /// Declined WITHOUT reading any body byte or writing any response; the serve
    /// loop buffers the body and dispatches to the ordinary buffered handler.
    Declined,
}

/// Handle passed to a streaming handler. Reads the request body straight off the
/// socket in caller-sized chunks (no full-body `Vec` buffering) and writes the
/// response straight back to the socket. Body bytes that arrived in the same
/// read as the head are served first, transparently.
pub struct StreamTransfer<'a> {
    stream: &'a mut TcpStream,
    prebuffered: Vec<u8>,
    prebuffered_pos: usize,
    content_length: usize,
    consumed: usize,
    keep_alive: bool,
}

impl<'a> StreamTransfer<'a> {
    fn new(
        stream: &'a mut TcpStream,
        mut prebuffered: Vec<u8>,
        content_length: usize,
        keep_alive: bool,
    ) -> Self {
        // Never treat more than content_length bytes as this request's body
        // (matches the buffered path, which ignores any pipelined trailer).
        if prebuffered.len() > content_length {
            prebuffered.truncate(content_length);
        }
        Self {
            stream,
            prebuffered,
            prebuffered_pos: 0,
            content_length,
            consumed: 0,
            keep_alive,
        }
    }

    /// Declared request-body length in bytes.
    pub fn body_len(&self) -> usize {
        self.content_length
    }

    /// Whether the connection is to be kept alive after this request.
    pub fn keep_alive(&self) -> bool {
        self.keep_alive
    }

    /// Read the next slice of request body into `buf`. Returns 0 at end of body.
    /// Serves any bytes already read while parsing the head, then reads from the
    /// socket, so a giant upload never lands in a single buffer.
    pub fn read_body(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.prebuffered_pos < self.prebuffered.len() {
            let n = buf.len().min(self.prebuffered.len() - self.prebuffered_pos);
            buf[..n]
                .copy_from_slice(&self.prebuffered[self.prebuffered_pos..self.prebuffered_pos + n]);
            self.prebuffered_pos += n;
            self.consumed += n;
            return Ok(n);
        }
        if self.consumed >= self.content_length {
            return Ok(0);
        }
        let want = buf.len().min(self.content_length - self.consumed);
        loop {
            match self.stream.read(&mut buf[..want]) {
                Ok(n) => {
                    self.consumed += n;
                    return Ok(n);
                }
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Drain and discard any unread request body so a kept-alive connection stays
    /// framed after an early (pre-body) error response.
    pub fn drain_body(&mut self) -> std::io::Result<()> {
        let mut scratch = [0u8; 8192];
        loop {
            if self.read_body(&mut scratch)? == 0 {
                return Ok(());
            }
        }
    }

    /// Write the response status line + headers. Call once, before the body.
    pub fn send_head(
        &mut self,
        status: u16,
        content_type: &str,
        content_length: usize,
    ) -> std::io::Result<()> {
        let status_text = if status == 200 { "OK" } else { "ERROR" };
        let connection = if self.keep_alive { "keep-alive" } else { "close" };
        let header = format!(
            "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {content_length}\r\nConnection: {connection}\r\n\r\n"
        );
        self.stream.write_all(header.as_bytes())
    }

    /// Write a slice of the response body straight to the socket.
    pub fn write_chunk(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(buf)
    }

    /// Flush the socket after the response has been written.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }

    /// Consume the transfer, returning the full request body (any prebuffered
    /// bytes + the rest read from the socket). Used by the buffered fallback when
    /// the streaming handler declines; taking `self` by value releases the socket
    /// borrow so the caller can write the buffered response.
    fn into_buffered_body(self) -> Result<Vec<u8>, HttpError> {
        let mut body = Vec::with_capacity(self.content_length);
        body.extend_from_slice(&self.prebuffered[self.prebuffered_pos..]);
        let mut chunk = [0u8; 8192];
        while body.len() < self.content_length {
            let want = chunk.len().min(self.content_length - body.len());
            match self.stream.read(&mut chunk[..want]) {
                Ok(0) => {
                    return Err(HttpError::BadResponse(
                        "incomplete request body".to_string(),
                    ))
                }
                Ok(n) => body.extend_from_slice(&chunk[..n]),
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => return Err(HttpError::Io(err)),
            }
        }
        Ok(body)
    }
}

pub fn serve(
    addr: &str,
    handler: impl Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
) -> Result<(), HttpError> {
    // Plain serve: every request goes through the buffered handler (the streaming
    // pre-handler declines everything).
    serve_with_stream_handler(addr, |_head, _transfer| StreamAction::Declined, handler)
}

/// Like [`serve`], but a `stream_handler` gets first refusal on each request with
/// the socket exposed (via [`StreamTransfer`]) so it can read the body and write
/// the response incrementally — no full-body buffering. It returns
/// [`StreamAction::Declined`] (without touching the body) to fall through to the
/// buffered `handler`. Used by the datanode to stream `/blob/<key>` bodies
/// straight between the socket and the object store.
pub fn serve_with_stream_handler<S, H>(
    addr: &str,
    stream_handler: S,
    handler: H,
) -> Result<(), HttpError>
where
    S: Fn(&RequestHead, &mut StreamTransfer) -> StreamAction + Send + Sync + 'static,
    H: Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
{
    serve_listener_with_stream_handler(TcpListener::bind(addr)?, stream_handler, handler)
}

/// Like [`serve`], but on a listener the caller has already bound.
///
/// Choosing a port by binding, reading the address back, dropping the listener
/// and handing the address on leaves the port owned by nobody until the server
/// binds it again. On a machine running more than one server process, something
/// else can take it inside that window, and the server then fails on `AddrInUse`
/// long after the address was decided -- far from where the address was chosen,
/// and usually reported as whatever the missing server broke downstream. Handing
/// the bound listener straight over closes the window: the port is never unowned.
pub fn serve_listener(
    listener: TcpListener,
    handler: impl Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
) -> Result<(), HttpError> {
    serve_listener_with_stream_handler(
        listener,
        |_head, _transfer| StreamAction::Declined,
        handler,
    )
}

/// [`serve_with_stream_handler`] on an already-bound listener. See [`serve_listener`].
pub fn serve_listener_with_stream_handler<S, H>(
    listener: TcpListener,
    stream_handler: S,
    handler: H,
) -> Result<(), HttpError>
where
    S: Fn(&RequestHead, &mut StreamTransfer) -> StreamAction + Send + Sync + 'static,
    H: Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
{
    let stream_handler = Arc::new(stream_handler);
    let handler = Arc::new(handler);
    for stream in listener.incoming() {
        let stream = stream?;
        // Disable Nagle's algorithm: request/response bodies are small and are
        // written in a couple of segments, so Nagle + delayed-ACK otherwise adds
        // a ~40ms stall per round-trip on loopback/LAN.
        let _ = stream.set_nodelay(true);
        let stream_handler = Arc::clone(&stream_handler);
        let handler = Arc::clone(&handler);
        thread::spawn(move || {
            let _ = handle_stream(stream, &*stream_handler, &*handler);
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
    // Borrowed when there is nothing to append, which is every caller but meta sync. The
    // sibling of this line in `request_bytes_with_options` was given a `Cow` and this one was
    // not, so the two allocations `format!` costs stayed on the JSON POST path -- the path the
    // proxy forwards execute, batch and the context routes through.
    let headers: std::borrow::Cow<'static, str> = if extra_headers.is_empty() {
        std::borrow::Cow::Borrowed("Content-Type: application/json\r\n")
    } else {
        std::borrow::Cow::Owned(format!("Content-Type: application/json\r\n{extra_headers}"))
    };
    let raw = request_raw_with_options(addr, "POST", path, &body, &headers, options)?;
    Ok(serde_json::from_slice(&raw)?)
}

/// Send a raw request body (e.g. a large binary attachment) and return the raw
/// response body. Used by the blob/attachment path where bodies are not JSON.
pub fn request_bytes_with_options(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    // The header line is only borrowed below, and every caller in this tree sends JSON, so
    // the common case needs no allocation. `format!` here cost two on every request the proxy
    // and the client make -- execute, batch, the context routes, meta sync, the heartbeat.
    let headers: std::borrow::Cow<'static, str> = match content_type {
        "application/json" => std::borrow::Cow::Borrowed("Content-Type: application/json\r\n"),
        other => std::borrow::Cow::Owned(format!("Content-Type: {other}\r\n")),
    };
    request_raw_with_options(addr, method, path, body, &headers, options)
}

/// Send a GET (empty body) and return the raw response body, with caller-supplied
/// extra request headers (each header line must end in `\r\n`). Non-200 responses
/// surface as `HttpError::BadResponse(status_line)`. Used by the cross-peer blob
/// availability path, which sets the `X-Ts-Blob-Peer-Fetch: 0` loop-guard header so
/// the queried peer serves local-only and never re-forwards.
pub fn get_bytes_with_headers(
    addr: &str,
    path: &str,
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    request_raw_with_options(addr, "GET", path, &[], extra_headers, options)
}

pub fn get_json<Res: DeserializeOwned>(addr: &str, path: &str) -> Result<Res, HttpError> {
    get_json_with_options(addr, path, HttpRequestOptions::default())
}

pub fn get_json_with_options<Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    get_json_with_options_and_headers(addr, path, "", options)
}

pub fn get_json_with_options_and_headers<Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    let raw = request_raw_with_options(addr, "GET", path, &[], extra_headers, options)?;
    Ok(serde_json::from_slice(&raw)?)
}

fn handle_stream(
    mut stream: TcpStream,
    stream_handler: &dyn Fn(&RequestHead, &mut StreamTransfer) -> StreamAction,
    handler: &dyn Fn(HttpRequest) -> (u16, Vec<u8>),
) -> Result<(), HttpError> {
    // HTTP/1.1 keep-alive: serve every request on this connection until the peer
    // closes it or goes idle. This removes the per-request TCP handshake + thread
    // spawn that otherwise caps throughput under concurrency.
    let _ = stream.set_read_timeout(Some(Duration::from_millis(SERVER_KEEPALIVE_IDLE_MS)));
    loop {
        // Read only through the end of the headers, keeping any body bytes that
        // arrived in the same read. This lets a streaming handler start writing
        // the body straight to its sink without the whole request being buffered.
        let (head_bytes, body_prefix) = match read_request_head(&mut stream)? {
            Some(parts) => parts,
            None => return Ok(()), // clean EOF / idle reap at a request boundary
        };
        let head = parse_request_head(&head_bytes)?;
        let mut transfer =
            StreamTransfer::new(&mut stream, body_prefix, head.content_length, head.keep_alive);
        match stream_handler(&head, &mut transfer) {
            StreamAction::Handled => {
                // The streaming handler already consumed the body and wrote the
                // full response; release the socket borrow and loop (or close).
                drop(transfer);
                if !head.keep_alive {
                    return Ok(());
                }
            }
            StreamAction::Declined => {
                // Fall back to the buffered handler: assemble the full body, run
                // the handler, write the coalesced header+body response.
                let body = transfer.into_buffered_body()?;
                let request = HttpRequest {
                    method: head.method,
                    path: head.path,
                    body,
                };
                let (status, body) = handler(request);
                write_buffered_response(&mut stream, status, &body, head.keep_alive)?;
                if !head.keep_alive {
                    return Ok(());
                }
            }
        }
    }
}

/// Write a fully-buffered response (coalesced header + body in a single write so
/// it leaves in one TCP segment where possible).
fn write_buffered_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    keep_alive: bool,
) -> Result<(), HttpError> {
    let status_text = if status == 200 { "OK" } else { "ERROR" };
    let content_type = if body.starts_with(b"# HELP ") || body.starts_with(b"# TYPE ") {
        "text/plain; version=0.0.4"
    } else {
        "application/json"
    };
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let header = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n",
        body.len()
    );
    let mut response = Vec::with_capacity(header.len() + body.len());
    response.extend_from_slice(header.as_bytes());
    response.extend_from_slice(body);
    stream.write_all(&response)?;
    stream.flush()?;
    Ok(())
}

/// Read one HTTP request's head (up to and including the `\r\n\r\n` terminator)
/// from a kept-alive connection. Returns the head bytes plus any body bytes that
/// arrived in the same read ("body prefix"). Returns `Ok(None)` when the peer
/// cleanly closes (or goes idle) at a request boundary.
fn read_request_head(stream: &mut TcpStream) -> Result<Option<(Vec<u8>, Vec<u8>)>, HttpError> {
    let marker = b"\r\n\r\n";
    let mut buffer = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => {
                if buffer.is_empty() {
                    return Ok(None); // clean close at boundary
                }
                return Err(HttpError::BadResponse("incomplete request".to_string()));
            }
            Ok(size) => {
                buffer.extend_from_slice(&chunk[..size]);
                if let Some(pos) = buffer.windows(marker.len()).position(|w| w == marker) {
                    let header_end = pos + marker.len();
                    let body_prefix = buffer[header_end..].to_vec();
                    buffer.truncate(header_end);
                    return Ok(Some((buffer, body_prefix)));
                }
            }
            Err(err) if buffer.is_empty() && is_idle_timeout(&err) => {
                // Idle keep-alive connection: reap it.
                return Ok(None);
            }
            Err(err) if matches!(err.kind(), ErrorKind::Interrupted) => continue,
            Err(err) => return Err(HttpError::Io(err)),
        }
    }
}

/// Parse method, path, content-length, and keep-alive intent from request-head
/// bytes (the request line + headers, including the trailing terminator).
fn parse_request_head(bytes: &[u8]) -> Result<RequestHead, HttpError> {
    let headers = String::from_utf8_lossy(bytes);
    let mut lines = headers.lines();
    let Some(request_line) = lines.next() else {
        return Err(HttpError::BadResponse("missing request line".to_string()));
    };
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut content_length = 0usize;
    let mut keep_alive = true;
    let mut blob_peer_fetch_loop_guard = false;
    let mut bearer_token = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse::<usize>().unwrap_or(0);
            } else if name.eq_ignore_ascii_case("connection") {
                keep_alive = !value.trim().eq_ignore_ascii_case("close");
            } else if name.eq_ignore_ascii_case("x-ts-blob-peer-fetch") {
                // A value of "0" marks the request as a peer-fetch hop: serve it
                // local-only, never re-forward (loop guard).
                blob_peer_fetch_loop_guard = value.trim() == "0";
            } else if name.eq_ignore_ascii_case("authorization") {
                let value = value.trim();
                // The scheme name is case-insensitive; the token is not.
                if value.len() > 7 && value[..7].eq_ignore_ascii_case("bearer ") {
                    bearer_token = Some(value[7..].trim().to_string());
                }
            }
        }
    }
    Ok(RequestHead {
        method,
        path,
        content_length,
        keep_alive,
        blob_peer_fetch_loop_guard,
        bearer_token,
    })
}

fn is_idle_timeout(err: &std::io::Error) -> bool {
    matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
}

#[cfg(test)]
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

fn request_raw_with_options(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    let mut last_error = None;
    let mut retry_sleep_ms = 2;
    for attempt in 0..=options.max_retries {
        match request_raw_once(addr, method, path, body, extra_headers, options) {
            Ok(response) => return Ok(response),
            Err(err) => {
                let retryable = is_retryable_request_error(&err);
                last_error = Some(err);
                if attempt < options.max_retries && retryable {
                    thread::sleep(Duration::from_millis(retry_sleep_ms));
                    retry_sleep_ms = (retry_sleep_ms * 2).min(50);
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| HttpError::BadResponse("request failed".to_string())))
}

/// Whether the exchange failed because the peer stopped answering, rather than because the
/// connection was already dead. The distinction decides whether the request may be sent
/// again: a dead socket carried nothing, a timeout may have carried everything.
fn timed_out(err: &HttpError) -> bool {
    matches!(
        err,
        HttpError::Io(err) if matches!(err.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock)
    )
}

fn is_retryable_request_error(err: &HttpError) -> bool {
    match err {
        HttpError::Io(err) => matches!(
            err.kind(),
            ErrorKind::WouldBlock
                | ErrorKind::TimedOut
                | ErrorKind::Interrupted
                | ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionReset
        ),
        HttpError::BadResponse(message) => {
            message.contains("incomplete response") || message.contains("missing response header")
        }
        HttpError::Json(_) => false,
    }
}

thread_local! {
    /// Per-thread pool of idle keep-alive sockets, keyed by destination address.
    /// Thread-local keeps the hot path lock-free; each worker reuses its own
    /// connections instead of paying a TCP handshake + server thread spawn per op.
    static CLIENT_CONN_POOL: RefCell<HashMap<String, Vec<TcpStream>>> =
        RefCell::new(HashMap::new());
}

fn pool_take(addr: &str) -> Option<TcpStream> {
    CLIENT_CONN_POOL.with(|pool| pool.borrow_mut().get_mut(addr).and_then(Vec::pop))
}

fn pool_put(addr: &str, stream: TcpStream) {
    CLIENT_CONN_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        let bucket = pool.entry(addr.to_string()).or_default();
        if bucket.len() < CLIENT_POOL_MAX_PER_ADDR {
            bucket.push(stream);
        }
        // else: drop the socket (bucket full)
    });
}

fn connect_fresh(addr: &str, options: HttpRequestOptions) -> Result<TcpStream, HttpError> {
    let connect_timeout = Duration::from_millis(options.connect_timeout_ms);
    let mut addrs = addr.to_socket_addrs()?;
    let socket_addr = addrs
        .next()
        .ok_or_else(|| HttpError::BadResponse(format!("cannot resolve address {addr}")))?;
    let stream = TcpStream::connect_timeout(&socket_addr, connect_timeout)?;
    // Disable Nagle: the request is header+small body written back-to-back; with
    // Nagle on, the body waits for an ACK of the header, colliding with the peer's
    // delayed-ACK timer for a ~40ms per-request stall.
    let _ = stream.set_nodelay(true);
    let io_timeout = Some(Duration::from_millis(options.io_timeout_ms));
    stream.set_read_timeout(io_timeout)?;
    stream.set_write_timeout(io_timeout)?;
    Ok(stream)
}

/// Perform one request/response exchange on `stream` (which must be freshly
/// readable). Returns the response body on success.
fn exchange_once(
    stream: &mut TcpStream,
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    // Coalesce header + body into a single buffer so the request leaves in one TCP segment
    // where possible, and format the header STRAIGHT INTO it. Building the header as its own
    // `String` first cost two allocations on every request the proxy and the client make, for
    // a line thrown away as soon as it is copied here.
    let mut framed = Vec::with_capacity(
        96 + method.len() + path.len() + addr.len() + extra_headers.len() + body.len(),
    );
    {
        use std::io::Write as _;
        // Writing into a `Vec` cannot fail, so there is no error case to handle.
        let _ = write!(
            framed,
            "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{extra_headers}Connection: keep-alive\r\n\r\n",
            body.len()
        );
    }
    framed.extend_from_slice(body);
    write_all_with_would_block_retry(stream, &framed, options.io_timeout_ms)?;
    flush_with_would_block_retry(stream, options.io_timeout_ms)?;

    let response = read_http_response(stream, options.io_timeout_ms)?;
    let (header_end, content_length) = parse_response_header(&response)?;
    if response.len() < header_end + b"\r\n\r\n".len() + content_length {
        return Err(HttpError::BadResponse(
            "incomplete response body".to_string(),
        ));
    }
    let marker = b"\r\n\r\n";
    // The status is decided on the BYTES. Rendering the line into a `String` first allocated on
    // every request to answer one `contains`, and the string is only wanted when it is the
    // error being returned -- so it is built there instead.
    let first_line_end = response[..header_end]
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(header_end);
    let status_bytes = &response[..first_line_end];
    if !status_bytes.windows(5).any(|window| window == b" 200 ") {
        let status_line = String::from_utf8_lossy(status_bytes)
            .trim_end_matches('\r')
            .to_string();
        return Err(HttpError::BadResponse(status_line));
    }
    // Hand the body back in the buffer it was read into rather than copying it out. `drain`
    // moves the bytes down inside the same allocation; `to_vec` made a second one per request.
    let body_start = header_end + marker.len();
    let mut response = response;
    response.truncate(body_start + content_length);
    response.drain(..body_start);
    Ok(response)
}

fn request_raw_once(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    // Fast path: reuse a pooled keep-alive socket. A pooled socket may have been
    // reaped by the server's idle timeout, so on any failure we discard it and
    // fall back to a fresh connection (transparent to the caller).
    if let Some(mut stream) = pool_take(addr) {
        match exchange_once(&mut stream, addr, method, path, body, extra_headers, options) {
            Ok(response) => {
                pool_put(addr, stream);
                return Ok(response);
            }
            // A pooled socket the server reaped fails immediately -- a reset, a broken pipe,
            // or an EOF that parses as a missing response header. Nothing was served on it,
            // so reconnecting and sending again is transparent, and that is what this
            // fallback is for.
            //
            // A timeout is not that. It means the peer accepted the request and did not
            // answer in time, so the request may well have been processed. Silently sending
            // it again on a fresh socket would apply a write twice, which is exactly the
            // thing the routing layer above refuses to do -- and doing it here would undo
            // that decision from underneath.
            Err(err) if timed_out(&err) => return Err(err),
            Err(_) => { /* stale/broken pooled connection: drop it, reconnect */ }
        }
    }
    let mut stream = connect_fresh(addr, options)?;
    let response = exchange_once(&mut stream, addr, method, path, body, extra_headers, options)?;
    pool_put(addr, stream);
    Ok(response)
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
    fn request_head_parses_a_bearer_token() {
        let head = parse_request_head(
            b"POST /meta/mute HTTP/1.1\r\nContent-Length: 2\r\nAuthorization: Bearer s3cret\r\n\r\n",
        )
        .unwrap();
        assert_eq!(head.bearer_token.as_deref(), Some("s3cret"));

        // Header name and scheme are case-insensitive; the token itself is not.
        let head = parse_request_head(
            b"GET /health HTTP/1.1\r\nauthorization: bearer MiXeD\r\n\r\n",
        )
        .unwrap();
        assert_eq!(head.bearer_token.as_deref(), Some("MiXeD"));

        // No credential, or a non-bearer scheme, parses as none.
        let head = parse_request_head(b"GET /health HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(head.bearer_token, None);
        let head = parse_request_head(
            b"GET /health HTTP/1.1\r\nAuthorization: Basic dXNlcg==\r\n\r\n",
        )
        .unwrap();
        assert_eq!(head.bearer_token, None);
    }

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

    #[test]
    fn keep_alive_reuses_a_single_connection_for_many_requests() {
        // The server accepts exactly ONE connection and serves every request on
        // it via the keep-alive loop in `handle_stream`. If client pooling +
        // server keep-alive work, all requests succeed over that one connection;
        // if either regressed, the 2nd request would need a 2nd (never-accepted)
        // connection and time out.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let pool_key = addr.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let _ = stream.set_nodelay(true);
            let _ = handle_stream(
                stream,
                &|_head, _transfer| StreamAction::Declined,
                &|request| json_response(200, &serde_json::json!({ "path": request.path })),
            );
        });

        for i in 0..5 {
            let response: Value = post_json_with_options(
                &addr,
                &format!("/p{i}"),
                &serde_json::json!({ "i": i }),
                HttpRequestOptions {
                    connect_timeout_ms: 200,
                    io_timeout_ms: 500,
                    max_retries: 1,
                },
            )
            .unwrap();
            assert_eq!(response["path"], format!("/p{i}"));
        }

        // Exactly one idle socket should be pooled for this destination.
        let pooled =
            CLIENT_CONN_POOL.with(|pool| pool.borrow().get(&pool_key).map(Vec::len).unwrap_or(0));
        assert_eq!(pooled, 1, "expected the keep-alive socket to be pooled for reuse");

        // Closing the pooled socket lets the server loop observe EOF and exit.
        CLIENT_CONN_POOL.with(|pool| {
            pool.borrow_mut().remove(&pool_key);
        });
        server.join().unwrap();
    }
}

/// Loopback ports for tests that stand a real server up.
///
/// The crate's test binary is regularly running in more than one copy at a time:
/// several worktrees share one machine, and a test run in one of them overlaps a
/// test run in another. Ports written into the tests as literals collide across
/// those processes -- whichever binary binds first wins, the other's server
/// thread dies with `AddrInUse`, and the test that needed that server reports
/// something else entirely: a counter that reads 0, a route that never resolves.
/// The bind error is inside a spawned thread, so the failure libtest prints names
/// only the consequence.
///
/// Ports here come from the OS instead, and the listener that reserved one is
/// kept and handed to [`serve_listener`], so a reserved port is owned by this
/// process from the moment it is chosen until the server accepts on it.
#[cfg(test)]
pub(crate) mod test_ports {
    use super::{serve_listener, HttpError, HttpRequest};
    use std::collections::HashMap;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};

    struct Reservation {
        addr: String,
        /// Taken by the first [`serve_reserved`] on this address; `None` after.
        listener: Option<TcpListener>,
    }

    fn table() -> &'static Mutex<HashMap<u64, Reservation>> {
        static TABLE: OnceLock<Mutex<HashMap<u64, Reservation>>> = OnceLock::new();
        TABLE.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// The address reserved for `key` in this process.
    ///
    /// Stable for the life of the process, so a test can name one address in as
    /// many places as it likes -- the way a literal port number used to read --
    /// while the port itself is whatever the OS had free.
    pub(crate) fn reserved_addr(key: u64) -> String {
        table()
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(|| {
                let listener =
                    TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
                let addr = listener
                    .local_addr()
                    .expect("read the reserved port")
                    .to_string();
                Reservation {
                    addr,
                    listener: Some(listener),
                }
            })
            .addr
            .clone()
    }

    /// A reserved address no other caller will be handed.
    ///
    /// For a test that needs an address but has no reason to name it twice.
    pub(crate) fn unique_addr() -> String {
        static NEXT: AtomicU64 = AtomicU64::new(1 << 32);
        reserved_addr(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// Serve on `addr`, taking over the listener that reserved it.
    ///
    /// An address this table did not reserve -- or one already being served -- is
    /// bound afresh, which is exactly what plain `serve` would have done.
    pub(crate) fn serve_reserved(
        addr: &str,
        handler: impl Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
    ) -> Result<(), HttpError> {
        serve_listener(claim(addr)?, handler)
    }

    fn claim(addr: &str) -> Result<TcpListener, HttpError> {
        if let Some(reserved) = table()
            .lock()
            .unwrap()
            .values_mut()
            .find(|reservation| reservation.addr == addr)
            .and_then(|reservation| reservation.listener.take())
        {
            return Ok(reserved);
        }
        Ok(TcpListener::bind(addr)?)
    }
}

#[cfg(test)]
mod content_type_header_tests {
    /// The borrowed JSON header is byte-identical to the `format!` it replaced.
    ///
    /// This line goes on the wire ahead of every forwarded body, so "close enough" is a protocol
    /// error rather than a cosmetic one. The non-JSON branch is checked too, because that is the
    /// one that still formats and would be the one to rot.
    /// The JSON POST header ends in a real CR LF, and matches what the `format!` produced.
    ///
    /// The sibling of this line in `request_bytes_with_options` was given a `Cow` and this one was
    /// not, so the JSON POST path -- the one the proxy forwards through -- kept allocating for a
    /// constant string.
    ///
    /// It asserts the BYTES, 13 and 10, not two source literals against each other. Writing this
    /// patch, an escaping slip turned the header into a real newline in the source; a test that
    /// compared one literal to another passed happily, because both sides were wrong in the same
    /// way. A header ending in a bare LF is a broken HTTP frame, not a slower one.
    /// The framed request is byte-identical to the `format!` it replaced.
    ///
    /// The header is now written straight into the send buffer instead of through its own
    /// `String`. That is worth two allocations per request, and it is worth nothing at all if a
    /// byte moved: this is the wire, and a header a peer parses differently is a broken request,
    /// not a slower one.
    #[test]
    fn the_framed_request_is_what_format_produced() {
        for (method, path, addr, extra, body) in [
            ("POST", "/execute", "127.0.0.1:9", "", &b"{}"[..]),
            ("GET", "/proxy/config", "host:1", "", &b""[..]),
            (
                "POST",
                "/tables/topology",
                "meta:7",
                "X-Ts-Topology-Version: 3\r\n",
                &b"{{\"a\":1}}"[..],
            ),
        ] {
            let expected_header = format!(
                "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{extra}Connection: keep-alive\r\n\r\n",
                body.len()
            );
            let mut expected = Vec::with_capacity(expected_header.len() + body.len());
            expected.extend_from_slice(expected_header.as_bytes());
            expected.extend_from_slice(body);

            let mut framed = Vec::with_capacity(
                96 + method.len() + path.len() + addr.len() + extra.len() + body.len(),
            );
            {
                use std::io::Write as _;
                let _ = write!(
                    framed,
                    "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{extra}Connection: keep-alive\r\n\r\n",
                    body.len()
                );
            }
            framed.extend_from_slice(body);

            assert_eq!(
                framed, expected,
                "the framed request moved for {method} {path}"
            );
        }
    }

    /// The status check reads the same answer off the bytes that the `String` gave.
    ///
    /// It used to render the first header line into a `String` on every request to answer one
    /// `contains(" 200 ")`, and only used that string when returning the error. The byte scan has
    /// to agree with it exactly -- including the trailing `\r` the old `.lines()` stripped, which is
    /// the part a hand-written version would get wrong.
    #[test]
    fn the_status_check_reads_what_the_string_read() {
        for header in [
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n",
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 500 Internal Server Error\r\n\r\n",
            "HTTP/1.1 200 \r\n\r\n",
            "garbage\r\n\r\n",
        ] {
            let response = header.as_bytes();
            let header_end = response
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .unwrap_or(response.len());

            let old_line = String::from_utf8_lossy(&response[..header_end])
                .lines()
                .next()
                .unwrap_or_default()
                .to_string();
            let old_ok = old_line.contains(" 200 ");

            let first_line_end = response[..header_end]
                .iter()
                .position(|byte| *byte == b'\n')
                .unwrap_or(header_end);
            let status_bytes = &response[..first_line_end];
            let new_ok = status_bytes.windows(5).any(|w| w == b" 200 ");
            let new_line = String::from_utf8_lossy(status_bytes)
                .trim_end_matches('\r')
                .to_string();

            assert_eq!(new_ok, old_ok, "the verdict moved for {header:?}");
            assert_eq!(new_line, old_line, "the error text moved for {header:?}");
        }
    }

    #[test]
    fn the_json_post_header_ends_in_a_real_crlf() {
        for extra in ["", "X-Ts-Topology-Version: 7\r\n", "A: 1\r\nB: 2\r\n"] {
            let actual: std::borrow::Cow<'static, str> = if extra.is_empty() {
                std::borrow::Cow::Borrowed("Content-Type: application/json\r\n")
            } else {
                std::borrow::Cow::Owned(format!("Content-Type: application/json\r\n{extra}"))
            };

            let bytes = actual.as_bytes();
            assert_eq!(
                &bytes[bytes.len() - 2..],
                &[13u8, 10u8],
                "the header must end in CR LF, not a bare newline: {actual:?}"
            );
            assert_eq!(
                actual.as_ref(),
                format!("Content-Type: application/json\r\n{extra}"),
                "extra headers: {extra:?}"
            );
            assert_eq!(
                matches!(actual, std::borrow::Cow::Borrowed(_)),
                extra.is_empty(),
                "the empty case must borrow and the non-empty case must own: {extra:?}"
            );
        }
    }

    #[test]
    fn the_content_type_header_is_what_format_produced() {
        for content_type in [
            "application/json",
            "application/octet-stream",
            "text/plain; charset=utf-8",
            "",
        ] {
            let borrowed: std::borrow::Cow<'static, str> = match content_type {
                "application/json" => {
                    std::borrow::Cow::Borrowed("Content-Type: application/json\r\n")
                }
                other => std::borrow::Cow::Owned(format!("Content-Type: {other}\r\n")),
            };
            assert_eq!(
                borrowed.as_ref(),
                format!("Content-Type: {content_type}\r\n"),
                "header line changed for {content_type:?}"
            );
        }
    }
}
