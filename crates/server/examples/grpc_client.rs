//! Calls the independent tonic-generated Echo client against the single-port example.
use rpc_interop::proto::{EchoMessage, echo_client::EchoClient};

#[tokio::main]
async fn main() -> Result<(), triple_server::BoxError> {
    let mut client = EchoClient::connect("http://127.0.0.1:50051").await?;
    let response = client
        .unary(EchoMessage {
            text: "hello".into(),
        })
        .await?;
    println!("Echo Unary: {}", response.into_inner().text);
    Ok(())
}
