use serde_json::Value;

/// Recognize explicit account/model rejections. Generic permission, endpoint,
/// parameter, and safety errors must not trigger replay on another account.
pub fn unavailable(value: &Value, model: &str) -> bool {
    if model.is_empty() {
        return false;
    }
    let error = value
        .pointer("/response/error")
        .or_else(|| value.get("error"))
        .unwrap_or(value);
    if matches!(
        error.get("code").and_then(Value::as_str),
        Some("model_not_found" | "model_access_denied")
    ) {
        return true;
    }
    let message = error
        .get("message")
        .or_else(|| error.get("detail"))
        .and_then(Value::as_str)
        .or_else(|| error.as_str());
    message.is_some_and(|message| {
        // The ChatGPT backend can return only a detail string, without a code.
        ['\'', '"', '`'].iter().any(|quote| message.trim() == format!(
            "The {quote}{model}{quote} model is not supported when using Codex with a ChatGPT account."
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recognizes_explicit_rejections_only() {
        assert!(unavailable(
            &json!({"error":{"code":"model_not_found"}}),
            "spark"
        ));
        assert!(unavailable(
            &json!({"type":"response.failed","response":{"error":{"code":"model_access_denied"}}}),
            "spark"
        ));
        assert!(unavailable(
            &json!({"detail":"The 'spark' model is not supported when using Codex with a ChatGPT account."}),
            "spark"
        ));
        for value in [
            json!({"error":{"code":"permission_denied"}}),
            json!({"error":{"code":"unsupported_parameter","param":"temperature"}}),
            json!({"error":{"message":"This model does not support temperature"}}),
            json!({"detail":"The 'other' model is not supported when using Codex with a ChatGPT account."}),
            json!({"error":{"code":"content_policy_violation"}}),
        ] {
            assert!(!unavailable(&value, "spark"));
        }
    }
}
