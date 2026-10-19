//! TCP control channel: one JSON request per line, one JSON response per line.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};
use tokio_util::sync::CancellationToken;

use crate::api::{Api, ErrorCode, Response};

pub const MAX_LINE: usize = 64 * 1024;

/// Accepts control connections until `shutdown` fires, then waits for open connections to
/// finish the request they are running.
pub async fn serve(listener: TcpListener, api: Arc<Api>, shutdown: CancellationToken) {
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    conns.spawn(connection(stream, peer, api.clone(), shutdown.clone()));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "control accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
    drop(listener);
    while conns.join_next().await.is_some() {}
}

async fn connection(stream: TcpStream, peer: SocketAddr, api: Arc<Api>, shutdown: CancellationToken) {
    tracing::debug!(%peer, "control connection opened");
    let _ = stream.set_nodelay(true);
    let (reader, mut writer) = stream.into_split();
    let mut lines = FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_LINE));
    loop {
        let line = tokio::select! {
            _ = shutdown.cancelled() => break,
            line = lines.next() => line,
        };
        let (response, close) = match line {
            None => break,
            Some(Ok(line)) if line.trim().is_empty() => continue,
            Some(Ok(line)) => (api.handle_bytes(line.as_bytes()).await, false),
            Some(Err(LinesCodecError::MaxLineLengthExceeded)) => {
                let message = format!("request longer than {MAX_LINE} bytes");
                (api.error(None, ErrorCode::BadRequest, message), true)
            }
            Some(Err(LinesCodecError::Io(e))) => {
                tracing::debug!(%peer, error = %e, "control read failed");
                break;
            }
        };
        if write_line(&mut writer, &response).await.is_err() || close {
            break;
        }
    }
    tracing::debug!(%peer, "control connection closed");
}

async fn write_line(writer: &mut (impl AsyncWriteExt + Unpin), response: &Response) -> std::io::Result<()> {
    let mut out = serde_json::to_vec(response).map_err(std::io::Error::other)?;
    out.push(b'\n');
    writer.write_all(&out).await
}
