use crate::{CancellationToken, Error, Result};
use bytes::Bytes;
use http::HeaderMap;
use http_body_util::BodyExt;
use tokio::sync::{mpsc, oneshot};

struct SendGuard(Option<CancellationToken>);
impl Drop for SendGuard {
    fn drop(&mut self) {
        if let Some(reset) = self.0.take() {
            reset.cancel();
        }
    }
}

pub(crate) enum Sender {
    H2(triple_transport::Sender),
    Hyper {
        data: mpsc::Sender<Bytes>,
        final_status: Option<oneshot::Sender<HeaderMap>>,
        reset: CancellationToken,
    },
}

impl Sender {
    pub(crate) async fn send(&mut self, payload: &[u8], limit: usize) -> Result<()> {
        match self {
            Self::H2(sender) => Ok(sender.send(payload).await?),
            Self::Hyper { data, reset, .. } => {
                let frame = triple_codec::encode_frame(payload, limit)
                    .map_err(triple_transport::Error::from)?;
                let mut guard = SendGuard(Some(reset.clone()));
                data.send(frame).await.map_err(|_| Error::Cancelled)?;
                guard.0 = None;
                Ok(())
            }
        }
    }
    pub(crate) fn close(&mut self) -> Result<()> {
        match self {
            Self::H2(sender) => Ok(sender.close()?),
            Self::Hyper { .. } => Err(Error::InvalidHeader("response requires final trailers")),
        }
    }
    pub(crate) fn finish(&mut self, trailers: HeaderMap) -> Result<()> {
        match self {
            Self::H2(sender) => Ok(sender.finish(trailers)?),
            Self::Hyper { final_status, .. } => final_status
                .take()
                .ok_or(Error::Cancelled)?
                .send(trailers)
                .map_err(|_| Error::Cancelled),
        }
    }
    pub(crate) fn reset(&mut self) {
        match self {
            Self::H2(sender) => sender.reset(),
            Self::Hyper { reset, .. } => reset.cancel(),
        }
    }
    pub(crate) fn reset_signal(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        match self {
            Self::H2(sender) => {
                let reset = sender.reset_signal();
                Box::pin(async move { Ok(reset.await?) })
            }
            Self::Hyper { reset, .. } => {
                let reset = reset.clone();
                Box::pin(async move {
                    reset.cancelled().await;
                    Ok(())
                })
            }
        }
    }
}

pub(crate) enum Receiver {
    H2(triple_transport::Receiver),
    Hyper {
        body: hyper::body::Incoming,
        input: Bytes,
        decoder: triple_codec::FrameDecoder,
        trailers: Option<HeaderMap>,
        closed: bool,
    },
}

impl Receiver {
    pub(crate) async fn message(&mut self) -> Result<Option<Bytes>> {
        match self {
            Self::H2(receiver) => Ok(receiver.message().await?),
            Self::Hyper {
                body,
                input,
                decoder,
                trailers,
                closed,
            } => {
                if *closed {
                    return Ok(None);
                }
                loop {
                    if let Some(payload) = decoder
                        .decode(input)
                        .map_err(triple_transport::Error::from)?
                    {
                        return Ok(Some(payload));
                    }
                    match body.frame().await {
                        Some(Ok(frame)) => match frame.into_data() {
                            Ok(data) => *input = data,
                            Err(frame) => {
                                if let Ok(fields) = frame.into_trailers() {
                                    *trailers = Some(fields);
                                }
                            }
                        },
                        Some(Err(error)) => return Err(Error::Io(std::io::Error::other(error))),
                        None => {
                            decoder.finish().map_err(triple_transport::Error::from)?;
                            *closed = true;
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }
    pub(crate) fn trailers(&self) -> Option<&HeaderMap> {
        match self {
            Self::H2(receiver) => receiver.trailers(),
            Self::Hyper { trailers, .. } => trailers.as_ref(),
        }
    }
}
