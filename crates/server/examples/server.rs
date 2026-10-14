//! Serves demo.Greeter and the interoperable test.Echo Unary method on one port.
mod common;
use std::sync::Arc;
use triple_server::{BoxError, Config, Server};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:50051").await?;
    let shutdown = rpc::CancellationToken::new();
    let signal = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        let result = tokio::signal::ctrl_c().await;
        signal.cancel();
        result
    });
    println!("listening on 127.0.0.1:50051");
    let result = Arc::new(Server::new(
        Arc::new(common::Echo),
        Arc::new(common::Greeter),
        Config::default(),
    ))
    .serve(listener, shutdown)
    .await;
    signal_task.abort();
    let _ = signal_task.await;
    result?;
    Ok(())
}
