use crate::{
    CancellationToken, Deadline, Error, Incoming, Limits, Metadata, Outgoing, Result, Service,
    Status,
};
use bytes::Bytes;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body::{Body, Frame};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use tokio_util::task::TaskTracker;

tokio::task_local! { static STREAM_ABORT: CancellationToken; }

#[derive(Clone)]
struct Executor {
    tracker: TaskTracker,
    shutdown: CancellationToken,
}
impl<F> hyper::rt::Executor<F> for Executor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        let shutdown = self.shutdown.clone();
        let abort = CancellationToken::new();
        self.tracker.spawn(STREAM_ABORT.scope(abort.clone(), async move {
            tokio::select! { biased; _ = shutdown.cancelled() => {}, _ = abort.cancelled() => {}, _ = future => {} }
        }));
    }
}

/// A bounded DATA stream followed by one final trailers frame.
pub struct ResponseBody {
    data: mpsc::Receiver<Bytes>,
    final_status: oneshot::Receiver<HeaderMap>,
    cancellation: CancellationToken,
    reset: CancellationToken,
    reset_wait: Pin<Box<dyn Future<Output = ()> + Send>>,
    ended: bool,
}
impl Body for ResponseBody {
    type Data = Bytes;
    type Error = h2::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, h2::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if self.reset.is_cancelled() || self.reset_wait.as_mut().poll(cx).is_ready() {
            self.ended = true;
            return Poll::Ready(Some(Err(h2::Reason::CANCEL.into())));
        }
        match self.data.poll_recv(cx) {
            Poll::Ready(Some(data)) => return Poll::Ready(Some(Ok(Frame::data(data)))),
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {}
        }
        match Pin::new(&mut self.final_status).poll(cx) {
            Poll::Ready(Ok(trailers)) => {
                self.ended = true;
                Poll::Ready(Some(Ok(Frame::trailers(trailers))))
            }
            Poll::Ready(Err(_)) => {
                self.ended = true;
                Poll::Ready(Some(Err(h2::Reason::CANCEL.into())))
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.ended
    }
}
impl Drop for ResponseBody {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

fn body_pair() -> (ResponseBody, crate::backend::Sender) {
    let (tx, data) = mpsc::channel(1);
    let (final_status, rx) = oneshot::channel();
    let cancellation = CancellationToken::new();
    let reset = CancellationToken::new();
    let waiter = reset.clone();
    (
        ResponseBody {
            data,
            final_status: rx,
            cancellation,
            reset: reset.clone(),
            reset_wait: Box::pin(async move { waiter.cancelled().await }),
            ended: false,
        },
        crate::backend::Sender::Hyper {
            data: tx,
            final_status: Some(final_status),
            reset,
        },
    )
}

fn immediate(status: &Status, limits: Limits) -> Result<Response<ResponseBody>> {
    let (mut body, sender) = body_pair();
    drop(sender);
    body.ended = true;
    let mut response = Response::new(body);
    let mut fields = headers::status_headers(status, &Metadata::new(), limits.max_header_size)?;
    fields.extend(headers::response_headers(
        &Metadata::new(),
        limits.max_header_size,
    )?);
    headers::check_response_headers(&fields, limits.max_header_size)?;
    *response.headers_mut() = fields;
    Ok(response)
}

/// Dispatches one hyper HTTP/2 request; the caller owns and drains the task tracker.
pub async fn handle<S: Service + 'static>(
    request: Request<hyper::body::Incoming>,
    service: Arc<S>,
    limits: Limits,
    tracker: TaskTracker,
) -> Result<Response<ResponseBody>> {
    let path = request.uri().path_and_query().map_or("", |v| v.as_str());
    let deadline = match headers::validate_request(
        request.method(),
        path,
        request.headers(),
        limits.max_header_size,
    )
    .map_err(Error::from)
    .and_then(|value| value.map(Deadline::from_header).transpose())
    {
        Ok(deadline) => deadline,
        Err(error) => {
            if matches!(
                error,
                Error::Headers(headers::Error::InvalidHeader("content-type"))
            ) {
                let mut response = immediate(&Status::ok(), limits)?;
                response.headers_mut().clear();
                *response.status_mut() = StatusCode::UNSUPPORTED_MEDIA_TYPE;
                return Ok(response);
            }
            return immediate(&crate::CallError::into_status(error), limits);
        }
    };
    let kind = match crate::resolve_method(service.kind(request.uri().path())) {
        Ok(kind) => kind,
        Err(status) => return immediate(&status, limits),
    };
    let metadata = match Metadata::from_headers_with_reserved(
        request.headers(),
        headers::is_grpc_reserved_header,
    ) {
        Ok(metadata) => metadata,
        Err(error) => return immediate(&crate::CallError::into_status(Error::from(error)), limits),
    };
    if deadline.is_some_and(|deadline| deadline.instant() <= tokio::time::Instant::now()) {
        return immediate(
            &crate::CallError::into_status(Error::DeadlineExceeded),
            limits,
        );
    }
    let path = request.uri().path().to_owned();
    let (body, sender) = body_pair();
    let context = crate::context::Context {
        deadline,
        cancellation: body.cancellation.clone(),
    };
    let reset = body.reset.clone();
    let abort = STREAM_ABORT.try_with(Clone::clone).ok();
    let mut incoming =
        Incoming::for_hyper(request.into_body(), metadata, limits, kind.single_request())
            .with_context(context.clone());
    let response_metadata = match service.response_metadata(&path, incoming.metadata()) {
        Ok(metadata) => metadata,
        Err(error) => return immediate(&crate::CallError::into_status(error), limits),
    };
    let mut response = Response::new(body);
    *response.headers_mut() =
        headers::response_headers(&response_metadata, limits.max_header_size)?;
    let mut outgoing =
        Outgoing::for_hyper(sender, limits, kind.single_response()).with_context(context.clone());
    tracker.spawn(async move {
        let result = context
            .run(crate::invoke_service(
                &*service,
                &path,
                kind,
                &mut incoming,
                &mut outgoing,
            ))
            .await;
        // A reset from a cancelled pending send must interrupt hyper's capacity wait,
        // not wait for another body poll. Dropping its tracked stream task sends CANCEL.
        if reset.is_cancelled() {
            // The one-shot terminal slot never waits for channel capacity.
            let _ = crate::finish_call(&mut outgoing, result);
            if let Some(abort) = abort {
                abort.cancel();
            }
        } else {
            let _ = crate::finish_call(&mut outgoing, result);
        }
    });
    Ok(response)
}

/// Serves one connection and joins every hyper stream and RPC handler task.
pub async fn serve_connection<T, S>(
    io: T,
    service: Arc<S>,
    limits: Limits,
    shutdown: CancellationToken,
    shutdown_timeout: Duration,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service + 'static,
{
    serve_tracked_connection(
        io,
        service,
        limits,
        shutdown,
        shutdown_timeout,
        TaskTracker::new(),
    )
    .await
}

async fn serve_tracked_connection<T, S>(
    io: T,
    service: Arc<S>,
    limits: Limits,
    shutdown: CancellationToken,
    shutdown_timeout: Duration,
    tracker: TaskTracker,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service + 'static,
{
    let stop = CancellationToken::new();
    let executor = Executor {
        tracker: tracker.clone(),
        shutdown: stop.clone(),
    };
    let mut builder = hyper::server::conn::http2::Builder::new(executor);
    let header_limit = u32::try_from(limits.max_header_size)
        .map_err(|_| transport::Error::InvalidConfig("header limit"))?;
    // Match transport::server: header limit and concurrent streams are Limits;
    // its fixed h2 defaults are 16384-byte frames and 65535-byte windows.
    builder
        .max_header_list_size(header_limit)
        .max_concurrent_streams(limits.max_concurrent_streams)
        .max_frame_size(16_384)
        .initial_stream_window_size(65_535)
        .initial_connection_window_size(65_535)
        .adaptive_window(false)
        .header_table_size(4096)
        .keep_alive_interval(None)
        .keep_alive_timeout(Duration::from_secs(20))
        .max_send_buf_size(65_535)
        .auto_date_header(false);
    // Preserve h2/hyper's rapid-reset protection; never disable either limit.
    builder
        .max_pending_accept_reset_streams(20)
        .max_local_error_reset_streams(1024);
    let calls = tracker.clone();
    let connection = builder.serve_connection(
        hyper_util::rt::TokioIo::new(io),
        hyper::service::service_fn(move |request| {
            handle(request, service.clone(), limits, calls.clone())
        }),
    );
    tokio::pin!(connection);
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            match tokio::time::Instant::now().checked_add(shutdown_timeout) {
                Some(deadline) => match tokio::time::timeout_at(deadline, &mut connection).await {
                    Ok(result) => connection_result(result),
                    Err(_) => Ok(()),
                },
                None => Err(transport::Error::InvalidConfig("shutdown timeout overflow").into()),
            }
        },
        result = &mut connection => connection_result(result),
    };
    stop.cancel();
    tracker.close();
    tracker.wait().await;
    result
}

fn connection_result(result: std::result::Result<(), hyper::Error>) -> Result<()> {
    let Err(error) = result else {
        return Ok(());
    };
    // Only the connection driver's I/O cause is eligible. Service/body failures
    // and HTTP/2 protocol errors must never become successful termination.
    if !error.is_user() {
        let mut cause = std::error::Error::source(&error);
        while let Some(current) = cause {
            if let Some(io) = current.downcast_ref::<std::io::Error>()
                && matches!(
                    io.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                )
            {
                return Ok(());
            }
            cause = current.source();
        }
    }
    Err(Error::Io(std::io::Error::other(error)))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Blocked {
        started: tokio::sync::Notify,
        dropped: Arc<AtomicUsize>,
    }
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Service for Blocked {
        fn contains(&self, _: &str) -> bool {
            true
        }
        fn kind(&self, _: &str) -> Option<crate::RpcKind> {
            Some(crate::RpcKind::ServerStreaming)
        }
        async fn call(&self, _: &str, _: &mut Incoming, _: &mut Outgoing) -> Result<()> {
            let _guard = Guard(self.dropped.clone());
            self.started.notify_one();
            std::future::pending().await
        }
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct Empty {}
    #[tokio::test]
    async fn shutdown_timeout_tracker_contains_and_joins_streams_and_handlers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(Blocked {
            started: tokio::sync::Notify::new(),
            dropped: dropped.clone(),
        });
        let calls = service.clone();
        let tracker = TaskTracker::new();
        let tracked = tracker.clone();
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            serve_tracked_connection(
                io,
                calls,
                Limits::default(),
                shutdown,
                Duration::from_millis(30),
                tracked,
            )
            .await
        });
        let mut client = crate::Client::connect(&address.to_string(), Limits::default())
            .await
            .unwrap();
        let mut bodies = Vec::new();
        for _ in 0..3 {
            let crate::Call {
                mut requests,
                response,
            } = client
                .call_stream(
                    "/test.Tracker/Block",
                    crate::RpcKind::ServerStreaming,
                    &Metadata::new(),
                )
                .await
                .unwrap();
            requests.send(&Empty {}).await.unwrap();
            requests.close().unwrap();
            bodies.push(response.receive().await.unwrap());
            tokio::time::timeout(Duration::from_secs(2), service.started.notified())
                .await
                .unwrap();
        }
        assert_eq!(
            tracker.len(),
            6,
            "three hyper stream futures plus three handler tasks"
        );
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(tracker.is_closed());
        assert!(tracker.is_empty());
        assert_eq!(dropped.load(Ordering::SeqCst), 3);
    }
}
