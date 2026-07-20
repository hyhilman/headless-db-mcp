//! `POST /mcp`: the single JSON-RPC endpoint.
//!
//! Reads the raw request body and decodes it with
//! `db_headless_mcp_wire::decode_message` directly, the same panic-free
//! decoder a socket-level transport (stdio) would use, rather than
//! trusting `axum::Json<T>` to validate JSON-RPC shape. A body that isn't
//! valid JSON-RPC is not an HTTP-level error: it is a valid JSON-RPC-level
//! error response (`id: null`), returned with `200 OK` like any other
//! reply.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use db_headless_mcp_wire::{decode_message, JsonRpcErrorResponse};
use serde::Serialize;

use crate::state::AppState;

pub(crate) async fn handle_mcp(State(state): State<AppState>, body: Bytes) -> Response {
    match decode_message(&body) {
        Ok(message) => match state.session.handle(message).await {
            Some(reply) => json_response(StatusCode::OK, &reply),
            None => StatusCode::ACCEPTED.into_response(),
        },
        Err(error) => json_response(StatusCode::OK, &JsonRpcErrorResponse::new(None, error)),
    }
}

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> Response {
    match serde_json::to_vec(body) {
        Ok(bytes) => (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response(),
        Err(error) => {
            tracing::error!(%error, "http transport: failed to serialize a json-rpc response");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
