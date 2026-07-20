#![forbid(unsafe_code)]

//! Pure message-format layer for the Model Context Protocol (MCP).
//!
//! This crate implements two independent wire formats:
//!
//! - [`jsonrpc`]: JSON-RPC 2.0 request/notification/response types and a
//!   panic-free decoder for arbitrary incoming bytes.
//! - [`sse`]: Server-Sent Events framing (encode and streaming decode).
//!
//! Neither module touches a socket or an HTTP stack. Wiring these formats
//! onto stdio or streamable HTTP+SSE transports is a later phase; this
//! crate only guarantees that both directions of the wire format are
//! correct and that decoding untrusted bytes never panics.
//!
//! # Threat model
//!
//! [`jsonrpc::decode_message`] and [`sse::SseDecoder::feed`] sit directly on
//! network input from a possibly hostile or buggy MCP client. Every parse
//! path reachable from those two entry points returns a typed error (or an
//! empty result) instead of panicking, unwrapping, or recursing without a
//! bound. See the doc comment on [`jsonrpc::decode_message`] for the specific
//! guards this crate applies and what is intentionally out of scope for now
//! (adversarial fuzzing).

pub mod jsonrpc;
pub mod sse;

pub use jsonrpc::{
    decode_message, JsonRpcError, JsonRpcErrorResponse, JsonRpcId, JsonRpcMessage,
    JsonRpcNotification, JsonRpcRequest, JsonRpcSuccessResponse,
};
pub use sse::{encode_sse_event, SseDecoder, SseEvent};
