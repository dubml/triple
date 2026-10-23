//! Serves demo.Greeter and the interoperable test.Echo Unary method on one port.
mod common;
#[path = "../interop-dubbo/trace.rs"]
mod trace;
use std::sync::Arc;
use triple_server::{BoxError, Config, Server};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let address = std::env::var("TRIPLE_LISTEN").unwrap_or_else(|_| "127.0.0.1:50051".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    let shutdown = triple_rpc::CancellationToken::new();
    let signal = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        let result = tokio::signal::ctrl_c().await;
        signal.cancel();
        result
    });
    println!("listening on {}", listener.local_addr()?);
    let server = Arc::new(Server::new(
        Arc::new(common::Echo),
        Arc::new(common::Greeter),
        Config::default(),
    ));
    let result = if std::env::var_os("TRIPLE_TRACE").is_some() {
        trace::serve(server, listener, shutdown).await
    } else {
        server.serve(listener, shutdown).await
    };
    signal_task.abort();
    let _ = signal_task.await;
    result?;
    Ok(())
}
