use bytes::Bytes;
use prost::Message;
use rpc_interop::proto::EchoMessage;
use triple_core::{Code, Path, Status};
use triple_http::{CallContext, Encoding, UnaryHandler};

#[derive(Clone, PartialEq, Message)]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct HelloReply {
    #[prost(string, tag = "1")]
    pub message: String,
}
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
impl rpc::Service for Echo {
    fn contains(&self, path: &str) -> bool {
        self.kind(path).is_some()
    }
    fn kind(&self, path: &str) -> Option<rpc::RpcKind> {
        Some(match path.strip_prefix("/test.Echo/")? {
            "Unary" | "Fail" | "Slow" => rpc::RpcKind::Unary,
            "ServerStreaming" => rpc::RpcKind::ServerStreaming,
            "ClientStreaming" => rpc::RpcKind::ClientStreaming,
            "Bidirectional" => rpc::RpcKind::Bidirectional,
            _ => return None,
        })
    }
    async fn call(
        &self,
        path: &str,
        request: &mut rpc::Incoming,
        response: &mut rpc::Outgoing,
    ) -> rpc::Result<()> {
        match path {
            "/test.Echo/Fail" => Err(rpc::Status::new(
                rpc::Code::PermissionDenied,
                "denied: 测试 %",
            )
            .with_details(&[8, 7])?
            .into()),
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
                    .ok_or(rpc::Error::Cardinality("request"))?;
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
            _ => Err(rpc::Status::new(rpc::Code::Unimplemented, "unknown method").into()),
        }
    }
}
