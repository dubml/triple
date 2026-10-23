//! Raw TCP characterization of the server's HTTP/2 settings and graceful GOAWAY.
mod common;

use std::{error::Error, sync::Arc, time::Duration};

use common::EchoService;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use triple_rpc::{CancellationToken, Limits, serve_connection};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const EMPTY_SETTINGS: [u8; 9] = [0, 0, 0, 4, 0, 0, 0, 0, 0];
const SETTINGS_ACK: [u8; 9] = [0, 0, 0, 4, 1, 0, 0, 0, 0];
const IO_TIMEOUT: Duration = Duration::from_secs(5);

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
    // This raw client advertises no larger receive frame size than RFC's default.
    assert!(
        length <= 16_384,
        "unexpected frame payload length: {length}"
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

#[derive(Debug, Eq, PartialEq)]
struct EffectiveSettings {
    max_header_list_size: Option<u32>,
    max_frame_size: u32,
    initial_window_size: u32,
    max_concurrent_streams: Option<u32>,
}

fn parse_settings(frame: &Frame) -> EffectiveSettings {
    assert_eq!(frame.kind, 4, "the server's first frame must be SETTINGS");
    assert_eq!(frame.flags & 1, 0, "initial SETTINGS must not be an ACK");
    assert_eq!(frame.stream_id, 0, "SETTINGS is a connection-level frame");
    assert_eq!(
        frame.payload.len() % 6,
        0,
        "SETTINGS entries have six bytes"
    );
    // RFC 9113 section 6.5.2: None denotes an unlimited initial setting.
    // https://www.rfc-editor.org/rfc/rfc9113.html#section-6.5.2
    let mut settings = EffectiveSettings {
        max_header_list_size: None,
        max_frame_size: 16_384,
        initial_window_size: 65_535,
        max_concurrent_streams: None,
    };
    for entry in frame.payload.as_chunks::<6>().0 {
        let id = u16::from_be_bytes([entry[0], entry[1]]);
        let value = u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]);
        match id {
            3 => settings.max_concurrent_streams = Some(value),
            4 => settings.initial_window_size = value,
            5 => settings.max_frame_size = value,
            6 => settings.max_header_list_size = Some(value),
            _ => {} // Unknown settings are ignored; repeated settings apply in order.
        }
    }
    settings
}

struct RawPeer {
    socket: TcpStream,
    shutdown: CancellationToken,
    server: JoinHandle<TestResult>,
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
                Arc::new(EchoService),
                Limits::default(),
                server_shutdown,
                Duration::from_millis(100),
            )
            .await?;
            Ok(())
        });
        let mut peer = Self {
            socket: TcpStream::connect(address).await?,
            shutdown,
            server,
        };
        peer.socket.write_all(PREFACE).await?;
        peer.socket.write_all(&EMPTY_SETTINGS).await?;
        Ok(peer)
    }

    async fn initial_settings(&mut self) -> TestResult<EffectiveSettings> {
        let frame = timeout(IO_TIMEOUT, read_frame(&mut self.socket)).await??;
        let settings = parse_settings(&frame);
        println!("first SETTINGS payload: {:02x?}", frame.payload);
        println!("effective settings: {settings:?}");
        self.socket.write_all(&SETTINGS_ACK).await?;
        Ok(settings)
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
async fn server_settings_effective_values_match_existing_implementation() -> TestResult {
    let mut peer = RawPeer::connect().await?;
    let observed = peer.initial_settings().await?;
    // Frozen from a raw TCP observation of the implementation before migration.
    assert_eq!(
        observed,
        EffectiveSettings {
            max_header_list_size: Some(8192),
            max_frame_size: 16_384,
            initial_window_size: 65_535,
            max_concurrent_streams: Some(128),
        }
    );
    peer.finish().await
}

#[tokio::test]
async fn graceful_shutdown_sends_goaway_with_no_error_on_raw_connection() -> TestResult {
    let mut peer = RawPeer::connect().await?;
    peer.initial_settings().await?;
    peer.shutdown.cancel();
    let goaway = timeout(IO_TIMEOUT, async {
        loop {
            let frame = read_frame(&mut peer.socket).await?;
            match frame.kind {
                7 => break TestResult::Ok(frame),
                4 if frame.flags & 1 == 0 => {
                    peer.socket.write_all(&SETTINGS_ACK).await?;
                }
                6 if frame.flags & 1 == 0 => {
                    assert_eq!(frame.stream_id, 0);
                    assert_eq!(frame.payload.len(), 8);
                    peer.socket.write_all(&[0, 0, 8, 6, 1, 0, 0, 0, 0]).await?;
                    peer.socket.write_all(&frame.payload).await?;
                }
                _ => {}
            }
        }
    })
    .await??;
    assert_eq!(goaway.stream_id, 0, "GOAWAY is a connection-level frame");
    assert!(goaway.payload.len() >= 8, "GOAWAY has an eight-byte prefix");
    let code = u32::from_be_bytes([
        goaway.payload[4],
        goaway.payload[5],
        goaway.payload[6],
        goaway.payload[7],
    ]);
    println!("GOAWAY error code: {code}");
    assert_eq!(code, 0, "graceful shutdown must send NO_ERROR");
    peer.finish().await
}
