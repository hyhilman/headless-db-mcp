//! `GET /mcp/stream` — placeholder proving the SSE wiring, not a
//! finished feature.
//!
//! This is **not** an implementation of the MCP "Streamable HTTP"
//! transport's SSE-upgrade/session-id/resumability semantics. There is no
//! real tool yet whose output needs to stream, so designing that
//! correctly now would be guesswork. What this endpoint proves instead:
//! `db_headless_mcp_wire::{SseEvent, encode_sse_event}` can be framed onto
//! a real `axum` streaming response body and read back by an SSE client
//! end to end. It intentionally bypasses `axum::response::sse::Sse` (which
//! has its own encoder) so the bytes on the wire are actually produced by
//! this crate's wire-format layer, not axum's.
//!
//! When a real tool needs to stream progress or partial results, replace
//! `DemoPingStream` with one that yields real events; the route,
//! content-type, and framing plumbing here can stay as is.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use db_headless_mcp_wire::{encode_sse_event, SseEvent};
use futures_core::Stream;
use tokio::time::Interval;

const DEMO_EVENT_COUNT: usize = 5;
const DEMO_EVENT_PERIOD: Duration = Duration::from_secs(2);

/// Emits `DEMO_EVENT_COUNT` `ping` events spaced `DEMO_EVENT_PERIOD` apart,
/// then ends the stream. The first tick fires immediately
/// (`tokio::time::interval`'s documented behavior), so a client sees the
/// first event without waiting a full period.
struct DemoPingStream {
    interval: Interval,
    remaining: usize,
    sequence: u64,
}

impl DemoPingStream {
    fn new(count: usize, period: Duration) -> Self {
        Self {
            interval: tokio::time::interval(period),
            remaining: count,
            sequence: 0,
        }
    }
}

impl Stream for DemoPingStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }

        match this.interval.poll_tick(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => {
                this.remaining -= 1;
                this.sequence += 1;
                let event = SseEvent {
                    event: Some("ping".to_string()),
                    data: format!("{{\"seq\":{}}}", this.sequence),
                    id: Some(this.sequence.to_string()),
                };
                Poll::Ready(Some(Ok(Bytes::from(encode_sse_event(&event)))))
            }
        }
    }
}

pub(crate) async fn stream_demo() -> impl IntoResponse {
    let stream = DemoPingStream::new(DEMO_EVENT_COUNT, DEMO_EVENT_PERIOD);
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        Body::from_stream(stream),
    )
}
