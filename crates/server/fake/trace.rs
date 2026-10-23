//! Example-only I/O observer. Protocol detection and dispatch still run inside
//! the real Server::serve_connection. Never changes bytes or response behavior.
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use triple_server::{Result, Server};

static CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

pub async fn serve<S: triple_rpc::Service + 'static, H: triple_http::UnaryHandler + 'static>(
    server: Arc<Server<S, H>>,
    listener: TcpListener,
    stop: triple_rpc::CancellationToken,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            accepted = listener.accept() => {
                let (io, _) = accepted?;
                let connection = CONNECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
                println!("TRIPLE_TRACE connection={connection} peer={}", io.peer_addr()?);
                let server = server.clone(); let shutdown = stop.clone();
                let started = Arc::new(Mutex::new(HashMap::new()));
                tasks.spawn(async move { server.serve_connection(Observed {
                    io, incoming: Parser::new(true, connection, started.clone()), outgoing: Parser::new(false, connection, started),
                }, shutdown).await });
            },
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(result) = joined { log_result(result); }
            }
        }
    }
    while let Some(result) = tasks.join_next().await {
        log_result(result);
    }
    Ok(())
}
fn log_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) {
    if let Err(error) = result.map_err(triple_server::Error::Task).and_then(|r| r) {
        eprintln!("TRIPLE_TRACE connection_error={error}");
    }
}
struct Observed {
    io: TcpStream,
    incoming: Parser,
    outgoing: Parser,
}
impl AsyncRead for Observed {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = result {
            self.incoming.feed(&buf.filled()[before..]);
        }
        result
    }
}
impl AsyncWrite for Observed {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(cx, bytes);
        if let Poll::Ready(Ok(n)) = result {
            self.outgoing.feed(&bytes[..n]);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
struct Parser {
    request: bool,
    connection: usize,
    protocol: Option<bool>,
    bytes: Vec<u8>,
    block: Vec<u8>,
    decoder: hpack::Decoder<'static>,
    status: HashMap<u32, String>,
    started: Arc<Mutex<HashMap<u32, Instant>>>,
}
impl Parser {
    fn new(request: bool, connection: usize, started: Arc<Mutex<HashMap<u32, Instant>>>) -> Self {
        Self {
            request,
            connection,
            protocol: None,
            bytes: Vec::new(),
            block: Vec::new(),
            decoder: hpack::Decoder::new(),
            status: HashMap::new(),
            started,
        }
    }
    fn feed(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
        if self.protocol.is_none() {
            if self.bytes.len() < 24 {
                return;
            }
            let h2 = if self.request {
                self.bytes.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            } else {
                !self.bytes.starts_with(b"HTTP/")
            };
            if h2 && self.request {
                self.bytes.drain(..24);
            }
            self.protocol = Some(h2);
        }
        if self.protocol == Some(false) {
            if let Some(end) = self.bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                println!(
                    "TRIPLE_TRACE http1 request={} headers={:?}",
                    self.request,
                    String::from_utf8_lossy(&self.bytes[..end])
                );
                self.bytes.clear();
            }
            return;
        }
        while self.bytes.len() >= 9 {
            let len = (usize::from(self.bytes[0]) << 16)
                | (usize::from(self.bytes[1]) << 8)
                | usize::from(self.bytes[2]);
            if self.bytes.len() < len + 9 {
                return;
            }
            let kind = self.bytes[3];
            let flags = self.bytes[4];
            let id = u32::from_be_bytes([
                self.bytes[5] & 0x7f,
                self.bytes[6],
                self.bytes[7],
                self.bytes[8],
            ]);
            let payload = self.bytes[9..9 + len].to_vec();
            self.bytes.drain(..9 + len);
            let timestamp_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let elapsed_us = self
                .started
                .lock()
                .ok()
                .and_then(|times| {
                    times
                        .get(&id)
                        .map(|start| start.elapsed().as_micros().to_string())
                })
                .unwrap_or_else(|| "unknown".into());
            if !self.request {
                let frame = match kind {
                    0 => "DATA",
                    1 => "HEADERS",
                    3 => "RST_STREAM",
                    4 => "SETTINGS",
                    6 => "PING",
                    7 => "GOAWAY",
                    8 => "WINDOW_UPDATE",
                    9 => "CONTINUATION",
                    _ => "UNKNOWN",
                };
                println!(
                    "TRIPLE_TRACE frame connection={} type={frame} stream={id} flags=0x{flags:02x} length={len} payload={payload:02x?} timestamp_unix_ms={timestamp_unix_ms} elapsed_us={elapsed_us}",
                    self.connection
                );
            }
            if kind == 1 || kind == 9 {
                let mut begin = 0;
                let mut end = payload.len();
                if kind == 1 && flags & 8 != 0 {
                    let Some(&pad) = payload.first() else {
                        continue;
                    };
                    begin += 1;
                    end = end.saturating_sub(usize::from(pad));
                }
                if kind == 1 && flags & 32 != 0 {
                    begin += 5;
                }
                if begin > end {
                    continue;
                }
                self.block.extend_from_slice(&payload[begin..end]);
                if flags & 4 != 0 {
                    match self.decoder.decode(&self.block) {
                        Ok(headers) => {
                            let fields: Vec<_> = headers
                                .into_iter()
                                .map(|(k, v)| {
                                    (
                                        String::from_utf8_lossy(&k).into_owned(),
                                        String::from_utf8_lossy(&v).into_owned(),
                                    )
                                })
                                .collect();
                            let field = |name| {
                                fields
                                    .iter()
                                    .find(|(k, _)| k == name)
                                    .map(|(_, v)| v.as_str())
                                    .unwrap_or("")
                            };
                            if self.request {
                                if let Ok(mut times) = self.started.lock() {
                                    times.entry(id).or_insert_with(Instant::now);
                                }
                                let names: Vec<_> =
                                    fields.iter().map(|(k, _)| k.as_str()).collect();
                                println!(
                                    "TRIPLE_TRACE connection={} stream={id} method={} path={} content-type={} names={names:?} headers={fields:?} timestamp_unix_ms={timestamp_unix_ms}",
                                    self.connection,
                                    field(":method"),
                                    field(":path"),
                                    field("content-type")
                                );
                            } else {
                                if !field(":status").is_empty() {
                                    self.status.insert(id, field(":status").to_owned());
                                }
                                println!(
                                    "TRIPLE_TRACE response connection={} stream={id} fields={fields:?} timestamp_unix_ms={timestamp_unix_ms} elapsed_us={elapsed_us}",
                                    self.connection
                                );
                            }
                        }
                        Err(error) => eprintln!("TRIPLE_TRACE decode_error={error:?}"),
                    }
                    self.block.clear();
                }
            } else if !self.request && kind == 0 && self.status.get(&id).is_some_and(|s| s != "200")
            {
                println!("TRIPLE_TRACE response_body stream={id} bytes={payload:?}");
            }
        }
    }
}
