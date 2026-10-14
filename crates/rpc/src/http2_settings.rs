use crate::Limits;
use std::time::Duration;

/// Numeric HTTP/2 settings shared by native and single-port servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2Settings {
    /// Initial receive window per stream, in bytes.
    pub initial_stream_window_size: u32,
    /// Initial receive window per connection, in bytes.
    pub initial_connection_window_size: u32,
    /// Maximum received frame payload, in bytes.
    pub max_frame_size: u32,
    /// Maximum simultaneously accepted streams.
    pub max_concurrent_streams: u32,
    /// Maximum header list size; builders check conversion to `u32`.
    pub max_header_list_size: usize,
    /// Maximum per-stream h2 send buffer, in bytes.
    pub max_send_buf_size: usize,
    /// HPACK header table size, in bytes.
    pub header_table_size: u32,
    /// Maximum reset streams pending acceptance.
    pub max_pending_accept_reset_streams: usize,
    /// Maximum locally generated error resets before closing the connection.
    pub max_local_error_reset_streams: usize,
    /// Optional automatic PING interval; `None` disables keepalive.
    pub keep_alive_interval: Option<Duration>,
    /// PING acknowledgement timeout; inactive when the interval is `None`.
    pub keep_alive_timeout: Duration,
}

/// Returns the frozen RPC HTTP/2 settings, including configured request limits.
///
/// Adaptive windows, automatic Date headers and extended CONNECT remain disabled
/// separately; these are policies rather than numeric settings.
pub fn http2_settings(limits: Limits) -> Http2Settings {
    Http2Settings {
        initial_stream_window_size: 65_535,
        initial_connection_window_size: 65_535,
        max_frame_size: 16_384,
        max_concurrent_streams: limits.max_concurrent_streams,
        max_header_list_size: limits.max_header_size,
        max_send_buf_size: 65_535,
        header_table_size: 4096,
        max_pending_accept_reset_streams: 20,
        max_local_error_reset_streams: 1024,
        keep_alive_interval: None,
        keep_alive_timeout: Duration::from_secs(20),
    }
}
