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

use std::{sync::Arc, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::{
    TlsAcceptor, TlsConnector,
    rustls::{
        self, ClientConfig, RootCertStore, ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    },
};

use crate::{CancellationToken, Client, Error, Limits, Result, Service, serve_connection};

/// A certificate chain and its owned private key for TLS authentication.
pub struct Identity {
    /// Leaf-first certificate chain in DER format.
    pub certificates: Vec<CertificateDer<'static>>,
    /// Corresponding private key in DER format.
    pub private_key: PrivateKeyDer<'static>,
}

/// Builds a verifying TLS client configuration advertising only HTTP/2.
pub fn client_tls_config(
    roots: RootCertStore,
    identity: Option<Identity>,
) -> Result<Arc<ClientConfig>> {
    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots);
    let mut config = match identity {
        Some(identity) => {
            builder.with_client_auth_cert(identity.certificates, identity.private_key)?
        }
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

/// Builds a TLS server configuration, requiring mTLS when client roots are supplied.
pub fn server_tls_config(
    identity: Identity,
    client_roots: Option<RootCertStore>,
) -> Result<Arc<ServerConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?;
    let builder = match client_roots {
        Some(roots) => builder.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .map_err(|_| Error::InvalidHeader("client certificate verifier"))?,
        ),
        None => builder.with_no_client_auth(),
    };
    let mut config = builder.with_single_cert(identity.certificates, identity.private_key)?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

impl Client {
    /// Connects with trust-chain and server-name verification, optionally using mTLS.
    pub async fn connect_tls(
        address: &str,
        server_name: &str,
        config: Arc<ClientConfig>,
        limits: Limits,
    ) -> Result<Self> {
        let server_name = ServerName::try_from(server_name.to_owned())
            .map_err(|_| Error::InvalidHeader("TLS server name"))?;
        let io = TlsConnector::from(config)
            .connect(server_name, TcpStream::connect(address).await?)
            .await?;
        if io.get_ref().1.alpn_protocol() != Some(b"h2".as_slice()) {
            return Err(Error::InvalidHeader("TLS ALPN h2"));
        }
        Self::from_io(io, &format!("https://{address}"), limits).await
    }
}

/// Accepts a verified TLS stream, checks ALPN, then serves Triple RPCs.
pub async fn serve_tls_connection<T, S>(
    io: T,
    config: Arc<ServerConfig>,
    service: Arc<S>,
    limits: Limits,
    shutdown: CancellationToken,
    shutdown_timeout: Duration,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service + 'static,
{
    let acceptor = TlsAcceptor::from(config);
    let io = tokio::select! {
        _ = shutdown.cancelled() => return Ok(()),
        io = acceptor.accept(io) => io?,
    };
    if io.get_ref().1.alpn_protocol() != Some(b"h2".as_slice()) {
        return Err(Error::InvalidHeader("TLS ALPN h2"));
    }
    serve_connection(io, service, limits, shutdown, shutdown_timeout).await
}
