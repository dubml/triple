//! Independent tonic client observes INTERNAL after a native partial response.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use protos::echo::{EchoMessage, echo_client::EchoClient};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::TcpListener;
use triple_rpc::{
    CancellationToken, HandlerPanicPhase, Incoming, Limits, Outgoing, RpcKind, Service,
};

const PATH: &str = "/test.Echo/ServerStreaming";

struct PanicService(Arc<Mutex<Vec<(String, HandlerPanicPhase)>>>);

impl Service for PanicService {
    fn contains(&self, path: &str) -> bool {
        path == PATH
    }

    fn kind(&self, path: &str) -> Option<RpcKind> {
        self.contains(path).then_some(RpcKind::ServerStreaming)
    }

    fn on_handler_panic(&self, path: &str, phase: HandlerPanicPhase) {
        self.0.lock().unwrap().push((path.to_owned(), phase));
    }

    async fn call(
        &self,
        _: &str,
        request: &mut Incoming,
        response: &mut Outgoing,
    ) -> triple_rpc::Result<()> {
        let request = request.message::<EchoMessage>().await?.unwrap();
        response
            .send(&EchoMessage {
                text: format!("before panic: {}", request.text),
            })
            .await?;
        panic!("intentional native fixture panic after first message");
    }
}

#[tokio::test]
async fn panic_is_reported_as_internal_to_tonic_client() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let events = Arc::new(Mutex::new(Vec::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let server = tokio::spawn({
            let service = Arc::new(PanicService(events.clone()));
            let stop = stop.clone();
            async move {
                let (io, _) = listener.accept().await.unwrap();
                triple_rpc::serve_connection(
                    io,
                    service,
                    Limits::default(),
                    stop,
                    Duration::from_millis(100),
                )
                .await
            }
        });
        let mut client = EchoClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let mut response = client
            .server_streaming(EchoMessage {
                text: "Alice".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.message().await.unwrap().unwrap().text,
            "before panic: Alice"
        );
        let error = response.message().await.unwrap_err();
        println!(
            "tonic: first=before panic: Alice code={:?} message={:?}",
            error.code(),
            error.message()
        );
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(error.message(), "handler panicked");
        assert_eq!(
            *events.lock().unwrap(),
            [(PATH.to_owned(), HandlerPanicPhase::Future)]
        );
        stop.cancel();
        server.await.unwrap().unwrap();
    })
    .await
    .unwrap();
}
