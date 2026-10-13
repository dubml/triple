// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{future::Future, sync::Arc, time::Duration};

use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{Error, Limits, Result};

enum ConnectionState {
    Accepting,
    Draining(tokio::time::Instant),
}

/// Drives one HTTP/2 server connection and runs accepted stream handlers concurrently.
///
/// The callback owns stream-level error policy. Shutdown sends GOAWAY and allows
/// existing streams to drain until `shutdown_timeout`; every exit aborts and joins
/// unfinished handlers before returning, including connection failures and panics.
pub async fn serve_connection<T, H, F>(
    io: T,
    handler: H,
    limits: Limits,
    shutdown: CancellationToken,
    shutdown_timeout: Duration,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Fn(http::Request<h2::RecvStream>, h2::server::SendResponse<Bytes>) -> F
        + Send
        + Sync
        + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let header_limit =
        u32::try_from(limits.max_header_size).map_err(|_| Error::InvalidConfig("header limit"))?;
    let mut builder = h2::server::Builder::new();
    builder
        .max_header_list_size(header_limit)
        .max_concurrent_streams(limits.max_concurrent_streams);
    let mut connection = tokio::select! {
        _ = shutdown.cancelled() => return Ok(()),
        connection = builder.handshake(io) => connection?,
    };
    let mut calls = JoinSet::new();
    let handler = Arc::new(handler);
    let mut state = ConnectionState::Accepting;
    let result = loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled(), if matches!(state, ConnectionState::Accepting) => {
                let Some(deadline) = tokio::time::Instant::now().checked_add(shutdown_timeout) else {
                    break Err(Error::InvalidConfig("shutdown timeout overflow"));
                };
                state = ConnectionState::Draining(deadline);
                connection.graceful_shutdown();
            }
            _ = async {
                match state {
                    ConnectionState::Draining(deadline) => tokio::time::sleep_until(deadline).await,
                    ConnectionState::Accepting => std::future::pending().await,
                }
            } => break Ok(()),
            result = calls.join_next(), if !calls.is_empty() => {
                if let Some(Err(error)) = result { break Err(Error::HandlerTask(error)); }
            }
            accepted = connection.accept() => {
                let Some(accepted) = accepted else { break Ok(()); };
                let (request, response) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(Error::Transport(error)),
                };
                let handler = Arc::clone(&handler);
                calls.spawn(async move { handler(request, response).await });
            }
        }
    };
    // Dropping the connection stops further I/O. Cancel all remaining handlers
    // and join them before returning, including the shutdown-timeout path.
    calls.abort_all();
    while calls.join_next().await.is_some() {}
    result
}
