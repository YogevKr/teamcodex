//! Usage-limit reset credits.
//!
//! ChatGPT grants an account reset credits. Redeeming one clears the Codex
//! usage windows at once instead of waiting for their scheduled reset. Codex
//! exposes this under `/usage` as "Redeem usage limit reset". This module
//! lists an account's credits and redeems one, either on request or
//! automatically for an account that opted in.

use crate::{auth::Token, now, pool::Pool, proxy, quota, status, storage};
use anyhow::{Context, Result, bail, ensure};
use axum::http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

/// Wait between automatic redeem attempts on one account.
pub const AUTO_RETRY_SECONDS: u64 = 300;
const TIMEOUT: Duration = Duration::from_secs(15);
const MAX_RESPONSE: usize = 256 * 1024;

/// Upstream result of a redeem request.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    /// A credit was consumed and the eligible windows were reset.
    Reset,
    /// No current window needed a reset. No credit was consumed.
    NothingToReset,
    /// The account has no credit to redeem.
    NoCredit,
    /// The same request id already completed a reset.
    AlreadyRedeemed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Outcome {
    pub code: Code,
    #[serde(default)]
    pub windows_reset: i64,
}

impl Outcome {
    /// True when the windows are clear after this outcome.
    pub fn reset(&self) -> bool {
        matches!(self.code, Code::Reset | Code::AlreadyRedeemed)
    }
}

async fn call(
    pool: &Arc<Pool>,
    idx: usize,
    method: Method,
    url: &str,
    body: Option<&Value>,
) -> Result<Value> {
    let (account, auth) = pool.entry(idx).context("unknown account")?;
    let send = |token: Token| {
        let mut request = pool
            .client
            .request(method.clone(), url)
            .bearer_auth(token.access_token)
            .header("user-agent", "codex-cli")
            .timeout(TIMEOUT);
        if let Some(id) = token.account_id {
            request = request.header("chatgpt-account-id", id);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        request.send()
    };
    let token = auth.get(&account, None).await?;
    let mut response = send(token.clone()).await?;
    if response.status() == StatusCode::UNAUTHORIZED {
        response = send(auth.get(&account, Some(token.generation)).await?).await?;
    }
    let status = response.status();
    let bytes = proxy::read_bounded(response, MAX_RESPONSE, TIMEOUT.as_secs()).await?;
    ensure!(
        status.is_success(),
        "upstream answered {}: {}",
        status.as_u16(),
        summary(&bytes)
    );
    serde_json::from_slice(&bytes).context("upstream answered with invalid JSON")
}

/// The upstream error message, or a short note when the body has none.
fn summary(bytes: &[u8]) -> String {
    serde_json::from_slice::<Value>(bytes)
        .ok()
        .and_then(|value| {
            ["/error/message", "/detail", "/message"]
                .into_iter()
                .find_map(|pointer| {
                    value
                        .pointer(pointer)
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
        })
        .unwrap_or_else(|| "no error message".to_owned())
}

fn endpoint(pool: &Pool, idx: usize) -> Result<String> {
    let account = pool.account(idx).context("unknown account")?;
    account
        .reset_credits()
        .context("this account has no usage-limit reset endpoint")
}

/// List the account's reset credits and record the available count.
pub async fn list(pool: &Arc<Pool>, idx: usize) -> Result<Value> {
    let url = endpoint(pool, idx)?;
    let value = call(pool, idx, Method::GET, &url, None).await?;
    pool.set_reset_credits(idx, quota::reset_credits(&value));
    Ok(value)
}

/// Redeem one reset credit. Without `credit_id` the upstream picks the next
/// available credit. `request_id` names one logical attempt; reuse it to
/// retry that attempt without spending a second credit. The account's usage
/// is probed again afterwards, so the pool sees the cleared windows.
pub async fn redeem(
    pool: &Arc<Pool>,
    idx: usize,
    credit_id: Option<&str>,
    request_id: Option<&str>,
) -> Result<Outcome> {
    let url = format!("{}/consume", endpoint(pool, idx)?);
    let generated;
    let request_id = match request_id {
        Some(id) => id,
        None => {
            generated = storage::random_string()?;
            &generated
        }
    };
    let mut body = json!({"redeem_request_id": request_id});
    if let Some(id) = credit_id {
        ensure!(!id.is_empty(), "credit id must not be empty");
        body["credit_id"] = json!(id);
    }
    let value = call(pool, idx, Method::POST, &url, Some(&body)).await?;
    let Ok(outcome) = serde_json::from_value::<Outcome>(value.clone()) else {
        bail!(
            "unexpected reset outcome: {}",
            value.get("code").map(Value::to_string).unwrap_or_default()
        );
    };
    match outcome.code {
        Code::Reset | Code::AlreadyRedeemed => pool.record_reset(idx),
        Code::NoCredit => pool.set_reset_credits(idx, Some(0)),
        Code::NothingToReset => {}
    }
    let _ = proxy::probe_account(pool, idx).await;
    Ok(outcome)
}

/// Redeem for an account that opted in, once per cooldown, when a request
/// found no eligible account. Concurrent callers wait for one attempt and
/// then see the cooldown, so one exhausted burst spends one credit.
pub async fn auto(pool: &Arc<Pool>, idx: usize) {
    let _guard = pool.reset_lock.lock().await;
    if !pool.auto_reset_ready(idx) {
        return;
    }
    pool.defer_reset(idx, now() + AUTO_RETRY_SECONDS);
    let name = pool.account(idx).map(|a| a.name).unwrap_or_default();
    match redeem(pool, idx, None, None).await {
        Ok(outcome) => eprintln!(
            "{}",
            json!({"at": now(), "event": "reset_redeemed", "account": name,
                "code": outcome.code, "windows_reset": outcome.windows_reset})
        ),
        Err(error) => eprintln!(
            "{}",
            json!({"at": now(), "event": "reset_failed", "account": name, "reason": error.to_string()})
        ),
    }
}

/// Plain-text listing of a credit payload for `tcx reset --list`.
pub fn render_credits(credits: &Value) -> String {
    let available = quota::reset_credits(credits).unwrap_or(0);
    let mut out = format!("reset credits available: {available}\n");
    let rows: Vec<[String; 4]> = credits
        .get("credits")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|credit| {
                    let text = |key: &str, fallback: &str| {
                        credit
                            .get(key)
                            .and_then(Value::as_str)
                            .filter(|v| !v.trim().is_empty())
                            .unwrap_or(fallback)
                            .to_owned()
                    };
                    [
                        text("id", "?"),
                        text("status", "?"),
                        text("title", "Full reset"),
                        text("expires_at", "never"),
                    ]
                })
                .collect()
        })
        .unwrap_or_default();
    if rows.is_empty() {
        return out;
    }
    let header = ["ID", "STATUS", "TITLE", "EXPIRES"];
    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let line = |cells: &[String]| {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i == cells.len() - 1 {
                    c.clone()
                } else {
                    format!("{c:<width$}", width = widths[i])
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
    };
    out.push_str(&line(&header.map(str::to_owned)));
    out.push('\n');
    for row in &rows {
        out.push_str(&line(row));
        out.push('\n');
    }
    out
}

/// Plain-text summary of a `/accounts/{name}/reset` answer.
pub fn render_outcome(outcome: &Value, now: u64) -> String {
    let account = outcome["account"].as_str().unwrap_or("?");
    let code = outcome["code"].as_str().unwrap_or("?");
    let windows = outcome["windows_reset"].as_i64().unwrap_or(0);
    let mut out = match code {
        "reset" => format!("{account}: reset {windows} window(s)"),
        "already_redeemed" => format!("{account}: this request already reset the windows"),
        "nothing_to_reset" => format!("{account}: no window needed a reset; no credit was used"),
        "no_credit" => format!("{account}: no reset credit is available"),
        other => format!("{account}: {other}"),
    };
    if let Some(left) = outcome["reset_credits"].as_i64() {
        out.push_str(&format!("; {left} reset credit(s) left"));
    }
    out.push('\n');
    if let Some(quotas) = outcome["quotas"].as_object() {
        for (id, window) in quotas {
            if !matches!(id.as_str(), "codex-primary" | "codex-secondary") {
                continue;
            }
            let used = window["used_percent"].as_f64().unwrap_or(0.0);
            let reset = window["reset_at"]
                .as_u64()
                .filter(|at| *at > now)
                .map(|at| format!(", resets {}", status::countdown(at, now)))
                .unwrap_or_default();
            out.push_str(&format!("  {id} {used:.0}% used{reset}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credit_listing_names_count_and_rows() {
        let text = render_credits(&json!({"available_count": 1, "credits": [
            {"id": "crd_1", "status": "available", "title": null, "expires_at": "2026-09-20T10:00:00Z"},
            {"id": "crd_0", "status": "redeemed", "title": "Launch bonus", "expires_at": null}
        ]}));
        assert!(text.starts_with("reset credits available: 1\n"), "{text}");
        assert!(
            text.contains("crd_1  available  Full reset    2026-09-20T10:00:00Z"),
            "{text}"
        );
        assert!(
            text.contains("crd_0  redeemed   Launch bonus  never"),
            "{text}"
        );
        assert_eq!(render_credits(&json!({})), "reset credits available: 0\n");
    }

    #[test]
    fn outcome_summary_reports_code_credits_and_codex_windows() {
        let text = render_outcome(
            &json!({"account": "personal", "code": "reset", "windows_reset": 2, "reset_credits": 1,
                "quotas": {"codex-primary": {"used_percent": 0.0, "reset_at": 1000 + 3600},
                           "gpt-spark-primary": {"used_percent": 50.0}}}),
            1000,
        );
        assert_eq!(
            text,
            "personal: reset 2 window(s); 1 reset credit(s) left\n  codex-primary 0% used, resets +1h00m\n"
        );
        let none = render_outcome(&json!({"account": "a", "code": "no_credit"}), 0);
        assert_eq!(none, "a: no reset credit is available\n");
    }
}
