//! JSON-RPC 2.0 message types and a panic-free decoder.
//!
//! # Type shape
//!
//! The JSON-RPC spec (<https://www.jsonrpc.org/specification>) describes
//! four distinct wire shapes that share the same envelope. This module
//! models each shape as its own struct rather than one struct with optional
//! fields, because the presence or absence of a field is meaningful on the
//! wire and an `Option<T>` field with `skip_serializing_if` cannot express
//! "notification has no `id` key at all" versus "error response has
//! `id: null`" as two different defaults for the same field name:
//!
//! - [`JsonRpcRequest`]: has `id` (required) and `method`.
//! - [`JsonRpcNotification`]: has `method` but NO `id` field, not even
//!   `id: null`. This is the one case the spec calls out explicitly: "A
//!   Notification is a Request object without an id member."
//! - [`JsonRpcSuccessResponse`]: has `id` (required) and `result`.
//! - [`JsonRpcErrorResponse`]: has `id` (present but nullable, used when the
//!   id of the failed request could not be determined, e.g. on a parse
//!   error) and `error`.
//!
//! [`JsonRpcMessage`] is the dispatch enum used only for *incoming* traffic,
//! where the shape is not known ahead of time and must be recovered from
//! the JSON object's keys. It also implements [`serde::Serialize`] (by
//! delegating to whichever variant is active) so a caller that already has
//! a `JsonRpcMessage` can re-encode it without matching on it first.
//!
//! [`JsonRpcId`] is deliberately narrower than the spec allows: the spec
//! permits any JSON `Number` (including fractional) for an id, but this
//! crate only accepts integers and strings and rejects everything else
//! (including fractional numbers, booleans, objects, and arrays) as
//! `INVALID_REQUEST`. The spec itself says an id "SHOULD NOT contain
//! fractional parts", so this is a deliberate narrowing of legal-but-
//! discouraged wire input, not a spec violation.

use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use thiserror::Error;

/// The only JSON-RPC version this crate understands. Any other value in the
/// `jsonrpc` field is rejected as `INVALID_REQUEST`.
pub const JSONRPC_VERSION: &str = "2.0";

/// Nesting depth (count of open `{`/`[` at any point) beyond which
/// [`decode_message`] refuses to even attempt a full JSON parse.
///
/// This guards against a stack overflow in `serde_json`'s recursive-descent
/// parser when handed pathological input such as `[[[[[...]]]]]` nested
/// tens of thousands of levels deep: `serde_json::Value` has no built-in
/// recursion limit. The guard itself is an iterative byte scan (see
/// [`exceeds_max_nesting`]), so it is safe to run on input of any depth
/// before the recursive parser ever sees it. No legitimate JSON-RPC message
/// used by this codebase nests anywhere near this deep.
const MAX_JSON_NESTING_DEPTH: usize = 128;

/// A JSON-RPC request or response id.
///
/// Per spec this is a JSON `String` or `Number`; this crate further
/// restricts `Number` to values that fit in `i64` (see the module doc
/// comment). `null` is not a variant here on purpose: `null` only ever
/// appears as the *absence* of a determinable id on an error response,
/// which is represented as `Option<JsonRpcId>` at the response level
/// instead of a third enum variant here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcId {
    Number(i64),
    String(String),
}

/// A JSON-RPC 2.0 error object, also used as the `Err` type of
/// [`decode_message`] for malformed input.
///
/// Wire shape: `{"code": <integer>, "message": <string>, "data"?: <any>}`.
/// `data` is omitted entirely when absent (not serialized as `null`),
/// matching the spec's "MAY be omitted" wording for that field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Error)]
#[error("JSON-RPC error {code}: {message}")]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<Value>,
}

impl JsonRpcError {
    pub const PARSE_ERROR: i64 = -32700;
    pub const INVALID_REQUEST: i64 = -32600;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
    pub const INTERNAL_ERROR: i64 = -32603;

    fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn parse_error(message: impl Into<String>) -> Self {
        Self::new(Self::PARSE_ERROR, message)
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(Self::INVALID_REQUEST, message)
    }

    pub fn method_not_found(message: impl Into<String>) -> Self {
        Self::new(Self::METHOD_NOT_FOUND, message)
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(Self::INVALID_PARAMS, message)
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(Self::INTERNAL_ERROR, message)
    }

    #[must_use]
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}

/// A JSON-RPC request: expects a reply correlated by `id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: JsonRpcId,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcRequest {
    pub fn new(id: JsonRpcId, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            method: method.into(),
            params,
        }
    }
}

/// A JSON-RPC notification: a request with no `id` field, so the sender
/// never expects and the receiver must never send a reply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcNotification {
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            method: method.into(),
            params,
        }
    }
}

/// A successful JSON-RPC response, replying to a specific request `id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcSuccessResponse {
    pub jsonrpc: String,
    pub id: JsonRpcId,
    pub result: Value,
}

impl JsonRpcSuccessResponse {
    pub fn new(id: JsonRpcId, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            result,
        }
    }
}

/// A failed JSON-RPC response.
///
/// `id` is `None` (serialized as `id: null`) exactly when the id of the
/// request that triggered the error could not be recovered, e.g. the input
/// was not valid JSON at all, or was a JSON value with no usable `id` key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcErrorResponse {
    pub jsonrpc: String,
    pub id: Option<JsonRpcId>,
    pub error: JsonRpcError,
}

impl JsonRpcErrorResponse {
    pub fn new(id: Option<JsonRpcId>, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            error,
        }
    }
}

/// Dispatch enum for an incoming message whose shape is not known ahead of
/// time. Produced by [`decode_message`]; see the module doc comment for why
/// this is a separate type from the four concrete structs rather than one
/// struct with optional fields.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonRpcMessage {
    /// Has `method` and a required `id`; the sender expects a reply.
    Request(JsonRpcRequest),
    /// Has `method` and NO `id` key at all; never reply to this.
    Notification(JsonRpcNotification),
    /// Has a required `id` and a `result`; replies to a prior request.
    SuccessResponse(JsonRpcSuccessResponse),
    /// Has `error` and an `id` that is `null` when the failed request's id
    /// could not be recovered (e.g. the input was not valid JSON at all).
    ErrorResponse(JsonRpcErrorResponse),
}

impl Serialize for JsonRpcMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            JsonRpcMessage::Request(v) => v.serialize(serializer),
            JsonRpcMessage::Notification(v) => v.serialize(serializer),
            JsonRpcMessage::SuccessResponse(v) => v.serialize(serializer),
            JsonRpcMessage::ErrorResponse(v) => v.serialize(serializer),
        }
    }
}

/// Iteratively scans raw JSON bytes for `{`/`[` nesting deeper than
/// `max_depth`, without recursing and without fully parsing the input.
///
/// This is intentionally not a JSON validator: malformed input (unbalanced
/// brackets, bad escapes) is left to `serde_json` to reject after this
/// check passes. Its only job is to bound recursion depth before the real
/// parser runs, and it does that in a single forward pass over the bytes
/// regardless of how deep the (possibly bogus) nesting claims to be.
fn exceeds_max_nesting(bytes: &[u8], max_depth: usize) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;

    for &byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max_depth {
                    return true;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }

    false
}

fn parse_id(value: &Value) -> Option<JsonRpcId> {
    match value {
        Value::Number(n) => n.as_i64().map(JsonRpcId::Number),
        Value::String(s) => Some(JsonRpcId::String(s.clone())),
        _ => None,
    }
}

/// Decodes a raw byte stream into one of the four JSON-RPC message shapes.
///
/// Never panics on any input, including empty input, non-UTF-8 or non-JSON
/// garbage, JSON that is syntactically valid but semantically not a
/// JSON-RPC message (a bare scalar, wrong `jsonrpc` version, wrong field
/// types), and pathologically deep nesting (guarded by
/// [`exceeds_max_nesting`] before the recursive JSON parser runs at all).
/// Every failure path returns a typed [`JsonRpcError`]: `PARSE_ERROR` for
/// input that is not valid JSON, `INVALID_REQUEST` for valid JSON that does
/// not match any known message shape.
///
/// Full adversarial fuzzing of this function (e.g. via `cargo-fuzz`) is out
/// of scope for this crate today. It is planned as a follow-up once the
/// crate is wired into an actual transport; this function's current
/// coverage is a hand-picked battery of malformed-input cases, not a fuzz
/// corpus.
pub fn decode_message(bytes: &[u8]) -> Result<JsonRpcMessage, JsonRpcError> {
    if exceeds_max_nesting(bytes, MAX_JSON_NESTING_DEPTH) {
        return Err(JsonRpcError::invalid_request(format!(
            "JSON nesting depth exceeds the limit of {MAX_JSON_NESTING_DEPTH}"
        )));
    }

    let value: Value =
        serde_json::from_slice(bytes).map_err(|e| JsonRpcError::parse_error(e.to_string()))?;

    decode_value(value)
}

fn decode_value(value: Value) -> Result<JsonRpcMessage, JsonRpcError> {
    let Some(obj) = value.as_object() else {
        return Err(JsonRpcError::invalid_request(
            "a JSON-RPC message must be a JSON object",
        ));
    };

    match obj.get("jsonrpc") {
        Some(Value::String(v)) if v == JSONRPC_VERSION => {}
        Some(_) => {
            return Err(JsonRpcError::invalid_request(
                "\"jsonrpc\" must be the string \"2.0\"",
            ));
        }
        None => {
            return Err(JsonRpcError::invalid_request("missing \"jsonrpc\" field"));
        }
    }

    if let Some(method_value) = obj.get("method") {
        return decode_request_or_notification(obj, method_value);
    }

    decode_response(obj)
}

fn decode_request_or_notification(
    obj: &serde_json::Map<String, Value>,
    method_value: &Value,
) -> Result<JsonRpcMessage, JsonRpcError> {
    let Value::String(method) = method_value else {
        return Err(JsonRpcError::invalid_request("\"method\" must be a string"));
    };
    let params = obj.get("params").cloned();

    match obj.get("id") {
        None => Ok(JsonRpcMessage::Notification(JsonRpcNotification {
            jsonrpc: JSONRPC_VERSION.to_string(),
            method: method.clone(),
            params,
        })),
        Some(id_value) => {
            let id = parse_id(id_value).ok_or_else(|| {
                JsonRpcError::invalid_request("\"id\" must be a number or string")
            })?;
            Ok(JsonRpcMessage::Request(JsonRpcRequest {
                jsonrpc: JSONRPC_VERSION.to_string(),
                id,
                method: method.clone(),
                params,
            }))
        }
    }
}

fn decode_response(obj: &serde_json::Map<String, Value>) -> Result<JsonRpcMessage, JsonRpcError> {
    let has_result = obj.contains_key("result");
    let has_error = obj.contains_key("error");

    if has_result && has_error {
        return Err(JsonRpcError::invalid_request(
            "a response must not contain both \"result\" and \"error\"",
        ));
    }

    if has_result {
        let id_value = obj
            .get("id")
            .ok_or_else(|| JsonRpcError::invalid_request("missing \"id\" field"))?;
        let id = parse_id(id_value)
            .ok_or_else(|| JsonRpcError::invalid_request("\"id\" must be a number or string"))?;
        let result = obj.get("result").cloned().unwrap_or(Value::Null);
        return Ok(JsonRpcMessage::SuccessResponse(JsonRpcSuccessResponse {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            result,
        }));
    }

    if has_error {
        let id_value = obj
            .get("id")
            .ok_or_else(|| JsonRpcError::invalid_request("missing \"id\" field"))?;
        let id = match id_value {
            Value::Null => None,
            other => Some(parse_id(other).ok_or_else(|| {
                JsonRpcError::invalid_request("\"id\" must be a number, string, or null")
            })?),
        };
        let error_value = obj.get("error").cloned().unwrap_or(Value::Null);
        let error: JsonRpcError = serde_json::from_value(error_value)
            .map_err(|e| JsonRpcError::invalid_request(format!("invalid \"error\" object: {e}")))?;
        return Ok(JsonRpcMessage::ErrorResponse(JsonRpcErrorResponse {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            error,
        }));
    }

    Err(JsonRpcError::invalid_request(
        "a JSON-RPC message must contain \"method\", \"result\", or \"error\"",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(bytes: &[u8]) -> JsonRpcMessage {
        decode_message(bytes).expect("expected a valid JSON-RPC message")
    }

    #[test]
    fn roundtrips_a_request() {
        let request = JsonRpcRequest::new(
            JsonRpcId::Number(1),
            "tools/call",
            Some(serde_json::json!({"name": "list_tables"})),
        );
        let encoded = serde_json::to_vec(&request).unwrap();
        let decoded = roundtrip(&encoded);
        assert_eq!(decoded, JsonRpcMessage::Request(request));
    }

    #[test]
    fn roundtrips_a_notification() {
        let notification = JsonRpcNotification::new(
            "notifications/progress",
            Some(serde_json::json!({"pct": 50})),
        );
        let encoded = serde_json::to_vec(&notification).unwrap();
        let decoded = roundtrip(&encoded);
        assert_eq!(decoded, JsonRpcMessage::Notification(notification));
    }

    #[test]
    fn roundtrips_a_success_response() {
        let response = JsonRpcSuccessResponse::new(
            JsonRpcId::String("abc".to_string()),
            serde_json::json!({"rows": []}),
        );
        let encoded = serde_json::to_vec(&response).unwrap();
        let decoded = roundtrip(&encoded);
        assert_eq!(decoded, JsonRpcMessage::SuccessResponse(response));
    }

    #[test]
    fn roundtrips_an_error_response() {
        let response = JsonRpcErrorResponse::new(
            Some(JsonRpcId::Number(7)),
            JsonRpcError::invalid_params("missing \"table\" argument"),
        );
        let encoded = serde_json::to_vec(&response).unwrap();
        let decoded = roundtrip(&encoded);
        assert_eq!(decoded, JsonRpcMessage::ErrorResponse(response));
    }

    #[test]
    fn error_response_with_null_id_roundtrips() {
        let response = JsonRpcErrorResponse::new(None, JsonRpcError::parse_error("bad json"));
        let encoded = serde_json::to_vec(&response).unwrap();
        let decoded = roundtrip(&encoded);
        assert_eq!(decoded, JsonRpcMessage::ErrorResponse(response));
    }

    #[test]
    fn notification_serializes_without_an_id_key() {
        let notification = JsonRpcNotification::new("ping", None);
        let encoded = serde_json::to_string(&notification).unwrap();
        assert!(!encoded.contains("\"id\""));
    }

    #[test]
    fn empty_input_is_a_parse_error() {
        let err = decode_message(b"").unwrap_err();
        assert_eq!(err.code, JsonRpcError::PARSE_ERROR);
    }

    #[test]
    fn garbage_bytes_are_a_parse_error() {
        let err = decode_message(b"not json at all {{{").unwrap_err();
        assert_eq!(err.code, JsonRpcError::PARSE_ERROR);
    }

    #[test]
    fn bare_number_is_invalid_request() {
        let err = decode_message(b"42").unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn bare_string_is_invalid_request() {
        let err = decode_message(b"\"hello\"").unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn bare_array_is_invalid_request() {
        let err = decode_message(b"[1,2,3]").unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn wrong_jsonrpc_version_is_invalid_request() {
        let err = decode_message(br#"{"jsonrpc":"1.0","id":1,"method":"ping"}"#).unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn method_with_wrong_type_is_invalid_request() {
        let err = decode_message(br#"{"jsonrpc":"2.0","id":1,"method":123}"#).unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn boolean_id_is_invalid_request() {
        let err = decode_message(br#"{"jsonrpc":"2.0","id":true,"method":"ping"}"#).unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn response_with_both_result_and_error_is_invalid_request() {
        let err = decode_message(
            br#"{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":-32603,"message":"x"}}"#,
        )
        .unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn deeply_nested_json_does_not_panic_or_hang() {
        let depth = 10_000;
        let mut input = String::with_capacity(depth * 2 + 16);
        input.push_str(r#"{"jsonrpc":"2.0","id":1,"method":"x","params":"#);
        for _ in 0..depth {
            input.push('[');
        }
        for _ in 0..depth {
            input.push(']');
        }
        input.push('}');

        let err = decode_message(input.as_bytes()).unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn deeply_nested_object_does_not_panic_or_hang() {
        let depth = 5_000;
        let mut input = String::new();
        for _ in 0..depth {
            input.push_str(r#"{"a":"#);
        }
        input.push_str("null");
        for _ in 0..depth {
            input.push('}');
        }

        let err = decode_message(input.as_bytes()).unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn missing_method_result_and_error_is_invalid_request() {
        let err = decode_message(br#"{"jsonrpc":"2.0","id":1}"#).unwrap_err();
        assert_eq!(err.code, JsonRpcError::INVALID_REQUEST);
    }

    #[test]
    fn notification_has_no_id_key_when_decoded() {
        let msg = roundtrip(br#"{"jsonrpc":"2.0","method":"ping"}"#);
        assert!(matches!(msg, JsonRpcMessage::Notification(_)));
    }
}
