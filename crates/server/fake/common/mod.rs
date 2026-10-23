use bytes::Bytes;
use prost::Message;
use protos::echo::EchoMessage;
pub use protos::greeter::{HelloReply, HelloRequest};
use triple_core::{Code, Path, Status};
use triple_http::{CallContext, Encoding, UnaryHandler};

pub struct Greeter;
impl UnaryHandler for Greeter {
    fn is_registered(&self, path: Path<'_>) -> bool {
        path.as_str() == "/demo.Greeter/SayHello"
    }
    async fn call(
        &self,
        _: Path<'_>,
        encoding: Encoding,
        message: Bytes,
        _: CallContext,
    ) -> Result<Bytes, Status> {
        let name = match encoding {
            Encoding::Json => {
                let value: serde_json::Value = serde_json::from_slice(&message)
                    .map_err(|_| Status::new(Code::InvalidArgument, "invalid JSON"))?;
                value
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| Status::new(Code::InvalidArgument, "name must be a string"))?
                    .to_owned()
            }
            Encoding::Proto => {
                HelloRequest::decode(message)
                    .map_err(|_| Status::new(Code::InvalidArgument, "invalid Protobuf"))?
                    .name
            }
            _ => return Err(Status::new(Code::Unimplemented, "unsupported encoding")),
        };
        let reply = HelloReply {
            message: format!("Hello {name}"),
        };
        match encoding {
            Encoding::Json => Ok(Bytes::from(
                serde_json::json!({"message":reply.message}).to_string(),
            )),
            Encoding::Proto => Ok(Bytes::from(reply.encode_to_vec())),
            _ => Err(Status::new(Code::Unimplemented, "unsupported encoding")),
        }
    }
}
pub struct Echo;
impl triple_rpc::Service for Echo {
    fn contains(&self, path: &str) -> bool {
        self.kind(path).is_some()
    }
    fn kind(&self, path: &str) -> Option<triple_rpc::RpcKind> {
        if path == "/demo.Greeter/SayHello" {
            return Some(triple_rpc::RpcKind::Unary);
        }
        Some(match path.strip_prefix("/test.Echo/")? {
            "Unary" | "Fail" | "FailPlain" | "FailNoDetails" | "FailNoDetailsPlain" | "Slow" => {
                triple_rpc::RpcKind::Unary
            }
            "ServerStreaming" => triple_rpc::RpcKind::ServerStreaming,
            "ClientStreaming" => triple_rpc::RpcKind::ClientStreaming,
            "Bidirectional" => triple_rpc::RpcKind::Bidirectional,
            _ => return None,
        })
    }
    async fn call(
        &self,
        path: &str,
        request: &mut triple_rpc::Incoming,
        response: &mut triple_rpc::Outgoing,
    ) -> triple_rpc::Result<()> {
        match path {
            "/demo.Greeter/SayHello" => {
                let request = request
                    .message::<HelloRequest>()
                    .await?
                    .ok_or(triple_rpc::Error::Cardinality("request"))?;
                response
                    .send(&HelloReply {
                        message: format!("Hello {}", request.name),
                    })
                    .await
            }
            "/test.Echo/Fail"
            | "/test.Echo/FailPlain"
            | "/test.Echo/FailNoDetails"
            | "/test.Echo/FailNoDetailsPlain" => {
                let message = if matches!(
                    path,
                    "/test.Echo/FailPlain" | "/test.Echo/FailNoDetailsPlain"
                ) {
                    "denied: plain"
                } else {
                    "denied: 测试 %"
                };
                let mut status =
                    triple_rpc::Status::new(triple_rpc::Code::PermissionDenied, message);
                if matches!(path, "/test.Echo/Fail" | "/test.Echo/FailPlain") {
                    status = status.with_details(&[8, 7])?;
                }
                Err(status.into())
            }
            "/test.Echo/Slow" => {
                request.message::<EchoMessage>().await?;
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                response
                    .send(&EchoMessage {
                        text: "late".into(),
                    })
                    .await
            }
            "/test.Echo/Unary" | "/test.Echo/ServerStreaming" => {
                let message = request
                    .message::<EchoMessage>()
                    .await?
                    .ok_or(triple_rpc::Error::Cardinality("request"))?;
                if path.ends_with("Unary") {
                    response.send(&message).await
                } else {
                    for suffix in 0..3 {
                        response
                            .send(&EchoMessage {
                                text: format!("{}:{suffix}", message.text),
                            })
                            .await?;
                    }
                    Ok(())
                }
            }
            "/test.Echo/ClientStreaming" => {
                let mut text = String::new();
                while let Some(message) = request.message::<EchoMessage>().await? {
                    text.push_str(&message.text);
                }
                response.send(&EchoMessage { text }).await
            }
            "/test.Echo/Bidirectional" => {
                while let Some(message) = request.message::<EchoMessage>().await? {
                    response.send(&message).await?;
                }
                Ok(())
            }
            _ => Err(
                triple_rpc::Status::new(triple_rpc::Code::Unimplemented, "unknown method").into(),
            ),
        }
    }
}
