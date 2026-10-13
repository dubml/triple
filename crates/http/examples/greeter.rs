//! Caller-owned HTTP/1.1 and HTTP/2 listener for curl acceptance checks.
#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{convert::Infallible, error::Error};

use bytes::Bytes;
use hyper::service::service_fn;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use prost::Message;
use tokio::{net::TcpListener, task::JoinSet};
use triple_core::{Code, Path, Status};
use triple_http::{Binding, CallContext, Encoding, UnaryHandler};

#[derive(Clone, PartialEq, Message)]
struct HelloRequest {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct HelloReply {
    #[prost(string, tag = "1")]
    message: String,
}

struct Greeter;

impl UnaryHandler for Greeter {
    fn is_registered(&self, path: Path<'_>) -> bool {
        path.as_str() == "/demo.Greeter/SayHello"
    }

    async fn call(
        &self,
        _path: Path<'_>,
        encoding: Encoding,
        message: Bytes,
        _context: CallContext,
    ) -> Result<Bytes, Status> {
        let name = match encoding {
            Encoding::Json => {
                let value: serde_json::Value = serde_json::from_slice(&message)
                    .map_err(|_| Status::new(Code::InvalidArgument, "invalid JSON message"))?;
                value
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| Status::new(Code::InvalidArgument, "name must be a string"))?
                    .to_owned()
            }
            Encoding::Proto => {
                HelloRequest::decode(message)
                    .map_err(|_| Status::new(Code::InvalidArgument, "invalid Protobuf message"))?
                    .name
            }
        };
        let message = format!("Hello {name}");
        Ok(match encoding {
            Encoding::Json => Bytes::from(serde_json::json!({"message": message}).to_string()),
            Encoding::Proto => Bytes::from(HelloReply { message }.encode_to_vec()),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:50051").await?;
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined { eprintln!("connection task: {error}"); }
            }
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                connections.spawn(async move {
                    let service = service_fn(|request| async {
                        Ok::<_, Infallible>(Binding::default().handle(request, &Greeter).await)
                    });
                    if let Err(error) = Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(socket), service).await
                    { eprintln!("connection: {error}"); }
                });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}
