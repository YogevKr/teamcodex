//! Credit auto top-up.
//!
//! ChatGPT can buy credits with the account's payment method when the credit
//! balance drops below a minimum ("Auto top-up" in Codex Settings > Usage).
//! TeamCodex reads this setting on every probe and spends credits only on an
//! account whose auto top-up is known to be off. It can also turn it off.

use crate::{pool::Pool, reset};
use anyhow::{Context, Result};
use axum::http::Method;
use serde_json::Value;
use std::sync::Arc;

fn endpoint(pool: &Pool, idx: usize) -> Result<String> {
    pool.account(idx)
        .context("unknown account")?
        .auto_top_up()
        .context("this account has no auto top-up endpoint")
}

/// True when the settings payload reports auto top-up as on.
pub fn enabled(value: &Value) -> Option<bool> {
    value.get("is_enabled").and_then(Value::as_bool)
}

/// Read the account's auto top-up settings and record whether it is on.
/// The payment method is not requested.
pub async fn settings(pool: &Arc<Pool>, idx: usize) -> Result<Value> {
    let url = format!(
        "{}/settings?include_payment_method=false",
        endpoint(pool, idx)?
    );
    let value = reset::call(pool, idx, Method::GET, &url, None, true).await?;
    pool.set_auto_top_up(idx, enabled(&value));
    Ok(value)
}

/// Turn auto top-up off, then read the settings again.
pub async fn disable(pool: &Arc<Pool>, idx: usize) -> Result<Value> {
    let url = format!("{}/disable", endpoint(pool, idx)?);
    reset::call(pool, idx, Method::POST, &url, None, true).await?;
    settings(pool, idx).await
}

/// Plain-text summary of a settings payload for `tcx top-up`.
pub fn render(name: &str, value: &Value) -> String {
    let field = |key: &str| {
        value
            .get(key)
            .filter(|v| !v.is_null())
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| v.to_string())
            })
            .unwrap_or_else(|| "-".to_owned())
    };
    match enabled(value) {
        Some(true) => format!(
            "{name}: auto top-up ON (below {} buy up to {}, monthly limit {})\n",
            field("recharge_threshold"),
            field("recharge_target"),
            field("recharge_monthly_limit"),
        ),
        Some(false) => format!("{name}: auto top-up off\n"),
        None => format!("{name}: auto top-up state unknown\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn summary_names_state_and_limits() {
        let on = json!({"is_enabled": true, "recharge_threshold": "100", "recharge_target": "500",
            "recharge_monthly_limit": null});
        assert_eq!(
            render("a", &on),
            "a: auto top-up ON (below 100 buy up to 500, monthly limit -)\n"
        );
        assert_eq!(
            render("a", &json!({"is_enabled": false})),
            "a: auto top-up off\n"
        );
        assert_eq!(render("a", &json!({})), "a: auto top-up state unknown\n");
        assert_eq!(enabled(&json!({"is_enabled": "yes"})), None);
    }
}
