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

use std::future::poll_fn;

use bytes::Bytes;
use http::Request;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    task::JoinHandle,
};

use crate::{Error, Limits, Result};

/// One multiplexed HTTP/2 connection with an owned background driver.
///
/// Dropping the client aborts its driver and closes the connection.
pub struct Client {
    sender: h2::client::SendRequest<Bytes>,
    driver: JoinHandle<std::result::Result<(), h2::Error>>,
}

impl Client {
    /// Connects with plaintext HTTP/2 prior knowledge (`h2c`).
    pub async fn connect(address: &str, limits: Limits) -> Result<Self> {
        Self::from_io(TcpStream::connect(address).await?, limits).await
    }

    /// Performs an HTTP/2 handshake over an established stream.
    pub async fn from_io<T>(io: T, limits: Limits) -> Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let header_limit = u32::try_from(limits.max_header_size)
            .map_err(|_| Error::InvalidConfig("header limit"))?;
        let (sender, connection) = h2::client::Builder::new()
            .max_header_list_size(header_limit)
            .handshake(io)
            .await?;
        Ok(Self {
            sender,
            driver: tokio::spawn(connection),
        })
    }

    /// Waits for the connection to admit another stream, without replaying requests.
    pub async fn ready(&mut self) -> Result<()> {
        Ok(poll_fn(|cx| self.sender.poll_ready(cx)).await?)
    }

    /// Opens one stream after `ready`, returning independently driven halves.
    pub fn send_request(
        &mut self,
        request: Request<()>,
        end_stream: bool,
    ) -> Result<(h2::client::ResponseFuture, h2::SendStream<Bytes>)> {
        Ok(self.sender.send_request(request, end_stream)?)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
