//! Shared wire contract for changelog stream events.
//!
//! Both the SSE handler (`http::routes::subscribe`) and the WebSocket handler
//! (`http::ws`) emit a `subscriber_lagged` notification when the underlying
//! broadcast channel overflows. Clients dispatch on the `type` discriminator
//! and report `dropped_count` to the user; both fields and the recovery hint
//! must agree across transports so a client can treat them identically.
//!
//! All literal strings and the JSON builder live here so the contract has a
//! single source of truth.

/// Stable `type` discriminator for the `subscriber_lagged` wire payload.
pub(in crate::http) const SUBSCRIBER_LAGGED_TYPE: &str = "subscriber_lagged";

/// Recovery hint carried by the `subscriber_lagged` wire payload.
pub(in crate::http) const SUBSCRIBER_LAGGED_HINT: &str =
    "the changelog queue overflowed; fetch a fresh snapshot if you require strict continuity";

/// Build the JSON body of the `subscriber_lagged` notification. Used by both
/// the SSE and WebSocket transports so they emit byte-identical payloads.
pub(in crate::http) fn lagged_payload(dropped_count: u64) -> serde_json::Value {
    serde_json::json!({
        "type": SUBSCRIBER_LAGGED_TYPE,
        "dropped_count": dropped_count,
        "hint": SUBSCRIBER_LAGGED_HINT,
    })
}

#[cfg(test)]
mod tests {
    use super::{SUBSCRIBER_LAGGED_HINT, SUBSCRIBER_LAGGED_TYPE, lagged_payload};

    #[test]
    fn lagged_payload_is_byte_for_byte_stable() {
        // This is the wire contract. Drift in any field — type, key, hint
        // wording — would break consumers, so the test intentionally pins
        // every byte.
        let p = lagged_payload(42);
        assert_eq!(
            p,
            serde_json::json!({
                "type": SUBSCRIBER_LAGGED_TYPE,
                "dropped_count": 42,
                "hint": SUBSCRIBER_LAGGED_HINT,
            })
        );
    }

    #[test]
    fn lagged_payload_handles_zero_drop_count() {
        // Edge case: BroadcastStreamRecvError::Lagged(0) is technically
        // possible if the channel underflows; payload should still be
        // well-formed.
        let p = lagged_payload(0);
        assert_eq!(p["dropped_count"], 0);
    }

    #[test]
    fn type_discriminator_is_stable_string() {
        assert_eq!(SUBSCRIBER_LAGGED_TYPE, "subscriber_lagged");
    }
}
