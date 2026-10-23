//! Characterizes connection flow control before migrating the RPC server.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
    time::timeout,
};
use triple_rpc::{
    CancellationToken, Incoming, Limits, Outgoing, RpcKind, Service, serve_connection,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

struct UnusedService;

impl Service for UnusedService {
    fn contains(&self, _path: &str) -> bool {
        false
    }

    fn kind(&self, _path: &str) -> Option<RpcKind> {
        None
    }

    async fn call(
        &self,
        _path: &str,
        _request: &mut Incoming,
        _response: &mut Outgoing,
    ) -> triple_rpc::Result<()> {
        Err(triple_rpc::Status::new(triple_rpc::Code::Unimplemented, "no methods").into())
    }
}

struct Frame {
    kind: u8,
    flags: u8,
    stream_id: u32,
    payload: Vec<u8>,
}

async fn read_frame(socket: &mut TcpStream) -> TestResult<Frame> {
    let mut header = [0; 9];
    socket.read_exact(&mut header).await?;
    let length =
        (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    assert!(
        length <= 16_384,
        "frame exceeds this client's receive limit"
    );
    let mut payload = vec![0; length];
    socket.read_exact(&mut payload).await?;
    Ok(Frame {
        kind: header[3],
        flags: header[4],
        stream_id: u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff,
        payload,
    })
}

struct RawPeer {
    socket: TcpStream,
    shutdown: CancellationToken,
    server: JoinHandle<triple_rpc::Result<()>>,
}

impl RawPeer {
    async fn connect() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            serve_connection(
                socket,
                Arc::new(UnusedService),
                Limits::default(),
                server_shutdown,
                Duration::from_millis(100),
            )
            .await
        });
        let mut peer = Self {
            socket: TcpStream::connect(address).await?,
            shutdown,
            server,
        };
        peer.socket
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await?;
        peer.socket.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await?;
        Ok(peer)
    }

    async fn finish(mut self) -> TestResult {
        self.shutdown.cancel();
        timeout(IO_TIMEOUT, &mut self.server).await???;
        Ok(())
    }
}

impl Drop for RawPeer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.server.abort();
    }
}

#[tokio::test]
async fn connection_window_before_any_request_matches_existing_implementation() -> TestResult {
    let mut peer = RawPeer::connect().await?;
    let settings = timeout(IO_TIMEOUT, read_frame(&mut peer.socket)).await??;
    assert_eq!(settings.kind, 4, "the first server frame must be SETTINGS");
    assert_eq!(settings.stream_id, 0);
    assert_eq!(settings.flags & 1, 0);
    peer.socket.write_all(&[0, 0, 0, 4, 1, 0, 0, 0, 0]).await?;
    // No HEADERS are sent. PING ACK is an ordered barrier for initial server frames,
    // so measuring the initial window does not depend on an arbitrary idle sleep.
    let marker = *b"window01";
    peer.socket.write_all(&[0, 0, 8, 6, 0, 0, 0, 0, 0]).await?;
    peer.socket.write_all(&marker).await?;
    let increments = timeout(IO_TIMEOUT, async {
        let mut increments = 0u64;
        loop {
            let frame = read_frame(&mut peer.socket).await?;
            match frame.kind {
                8 if frame.stream_id == 0 => {
                    assert_eq!(frame.payload.len(), 4);
                    let increment = u32::from_be_bytes([
                        frame.payload[0],
                        frame.payload[1],
                        frame.payload[2],
                        frame.payload[3],
                    ]) & 0x7fff_ffff;
                    assert_ne!(increment, 0, "WINDOW_UPDATE increment cannot be zero");
                    println!("connection WINDOW_UPDATE increment: {increment}");
                    increments += u64::from(increment);
                }
                6 if frame.flags & 1 != 0 && frame.payload == marker => {
                    assert_eq!(frame.stream_id, 0);
                    break TestResult::Ok(increments);
                }
                _ => {}
            }
        }
    })
    .await??;
    let effective = 65_535 + increments;
    println!("initial connection window: 65535 + {increments} = {effective}");
    assert_eq!(effective, 65_535, "frozen pre-migration connection window");
    peer.finish().await
}

#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}

#[derive(Default)]
struct StalledService {
    attempts: AtomicUsize,
    completed: AtomicUsize,
    dropped: AtomicUsize,
    first_sent: Notify,
    drop_signal: Notify,
}

struct HandlerGuard(Arc<StalledService>);

impl Drop for HandlerGuard {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
        self.0.drop_signal.notify_one();
    }
}

struct StreamingService(Arc<StalledService>);

impl Service for StreamingService {
    fn contains(&self, path: &str) -> bool {
        path == "/test.Flow/Stall"
    }

    fn kind(&self, path: &str) -> Option<RpcKind> {
        self.contains(path).then_some(RpcKind::ServerStreaming)
    }

    async fn call(
        &self,
        _path: &str,
        request: &mut Incoming,
        response: &mut Outgoing,
    ) -> triple_rpc::Result<()> {
        let _guard = HandlerGuard(Arc::clone(&self.0));
        request
            .message::<Payload>()
            .await?
            .ok_or(triple_rpc::Error::Cardinality("request"))?;
        let message = Payload {
            data: vec![b'x'; 32 * 1024],
        };
        loop {
            self.0.attempts.fetch_add(1, Ordering::SeqCst);
            response.send(&message).await?;
            self.0.completed.fetch_add(1, Ordering::SeqCst);
            self.0.first_sent.notify_one();
        }
    }
}

struct ServerTask {
    shutdown: CancellationToken,
    task: JoinHandle<triple_rpc::Result<()>>,
}

impl ServerTask {
    async fn finish(mut self) -> TestResult {
        self.shutdown.cancel();
        timeout(IO_TIMEOUT, &mut self.task).await???;
        Ok(())
    }
}

impl Drop for ServerTask {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Terminal {
    DeadlineExceeded,
    Reset(h2::Reason),
}

#[tokio::test]
async fn stalled_response_deadline_drops_handler_and_preserves_wire_result() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let service = Arc::new(StalledService::default());
    let server_service = Arc::new(StreamingService(Arc::clone(&service)));
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        serve_connection(
            socket,
            server_service,
            Limits::default(),
            server_shutdown,
            Duration::from_secs(1),
        )
        .await
    });
    let server = ServerTask { shutdown, task };
    let limits = Limits::default();
    let mut client = triple_transport::Client::connect(&address.to_string(), limits).await?;
    client.ready().await?;
    let expiry = tokio::time::Instant::now() + Duration::from_millis(300);
    let deadline = triple_rpc::Deadline::after(Duration::from_millis(300))?;
    // Only the server receives a deadline: a local RPC-client deadline would mask
    // its wire result when reading resumes after expiry. This test-only use of
    // the public RPC helpers and transport client is explicitly authorized.
    let request = triple_rpc::prepare_request(
        &format!("http://{address}").parse()?,
        "/test.Flow/Stall",
        &triple_rpc::Metadata::new(),
        Some(&deadline.to_header()?),
        limits,
    )?;
    let (response, stream) = client.send_request(request, false)?;
    let mut requests = Outgoing::new(stream, limits, true, triple_rpc::SendRole::Request);
    requests.send(&Payload { data: Vec::new() }).await?;
    requests.close()?;
    let head = triple_rpc::parse_response(
        timeout(IO_TIMEOUT, response).await??,
        limits.max_header_size,
    )?;
    assert!(head.status.is_none());
    let mut incoming = Incoming::new(head.body, head.metadata, limits, false, true);
    assert!(
        incoming.deadline().is_none(),
        "must observe the remote wire result"
    );
    timeout(IO_TIMEOUT, service.first_sent.notified()).await?;
    assert!(
        service.attempts.load(Ordering::SeqCst) > service.completed.load(Ordering::SeqCst),
        "handler send must be pending while the client does not read"
    );
    tokio::time::timeout_at(
        expiry + Duration::from_secs(2),
        service.drop_signal.notified(),
    )
    .await?;
    assert_eq!(service.dropped.load(Ordering::SeqCst), 1);
    println!(
        "stalled handler dropped; sends attempted={}, completed={}",
        service.attempts.load(Ordering::SeqCst),
        service.completed.load(Ordering::SeqCst)
    );
    let (messages, terminal) = timeout(IO_TIMEOUT, async {
        let mut messages = 0usize;
        loop {
            match incoming.message::<Payload>().await {
                Ok(Some(message)) => {
                    assert_eq!(message.data.len(), 32 * 1024);
                    messages += 1;
                }
                Ok(None) => return Err("stream ended without deadline status or reset".into()),
                Err(error) => {
                    println!("resumed reader error: {error:?}");
                    let terminal = match error {
                        triple_rpc::Error::Rpc(status)
                            if status.code() == triple_rpc::Code::DeadlineExceeded =>
                        {
                            Terminal::DeadlineExceeded
                        }
                        triple_rpc::Error::Transport(triple_transport::Error::Transport(error))
                            if error.is_reset() =>
                        {
                            Terminal::Reset(error.reason().ok_or("reset has no reason")?)
                        }
                        error => return Err(Box::new(error) as Box<dyn Error + Send + Sync>),
                    };
                    return TestResult::Ok((messages, terminal));
                }
            }
        }
    })
    .await??;
    println!("resumed reader: messages={messages}, terminal={terminal:?}");
    // Freeze the pre-migration wire result after recording this observation.
    assert_eq!(messages, 1);
    assert_eq!(terminal, Terminal::Reset(h2::Reason::CANCEL));
    server.finish().await?;
    drop(incoming);
    drop(requests);
    drop(client);
    Ok(())
}
