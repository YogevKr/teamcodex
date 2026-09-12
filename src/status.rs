//! Plain-text account table for `tcx status`.
//!
//! One line per account, no colour, so the output stays greppable. The table
//! reads the same JSON the `/status` route serves, so it never needs the
//! server's in-memory types.

use crate::quota::{Quotas, Window};
use serde_json::Value;

/// Describe an account for the table and the terminal display.
pub fn state_label(
    disabled: bool,
    hold_until: u64,
    quotas: &Quotas,
    threshold: f64,
    now: u64,
) -> &'static str {
    if disabled {
        "disabled"
    } else if hold_until > now {
        "waiting"
    } else if quotas.values().any(|w| w.used(now) >= threshold) {
        "limited"
    } else {
        "ready"
    }
}

/// Render a countdown to `at` as `+45s`, `+12m`, `+5h03m`, or `+4d22h`.
pub fn countdown(at: u64, now: u64) -> String {
    let secs = at.saturating_sub(now);
    let mins = secs / 60;
    if secs < 60 {
        format!("+{secs}s")
    } else if mins < 60 {
        format!("+{mins}m")
    } else if mins < 60 * 24 {
        format!("+{}h{:02}m", mins / 60, mins % 60)
    } else {
        format!("+{}d{:02}h", mins / (60 * 24), (mins / 60) % 24)
    }
}

#[derive(PartialEq)]
enum Kind {
    Short,
    Weekly,
    Other,
}

/// Codex accounts expose a short window (5 h) and a weekly window under the
/// same `codex-*` ids; ChatGPT plans only carry the weekly one as
/// `codex-primary`. Classify by length, not by id.
fn kind(id: &str, window: &Window) -> Kind {
    if !matches!(id, "codex-primary" | "codex-secondary") {
        return Kind::Other;
    }
    match window.window_minutes {
        Some(m) if m > 0 && m <= 60 * 24 => Kind::Short,
        Some(m) if m > 60 * 24 => Kind::Weekly,
        _ => Kind::Other,
    }
}

/// `84% (+6d00h)` for a live window, `-` when the account never reported one.
fn remaining(quotas: &Quotas, wanted: Kind, now: u64) -> String {
    let mut best: Option<(f64, Option<u64>)> = None;
    for (id, window) in quotas {
        if kind(id, window) != wanted {
            continue;
        }
        let left = (100.0 - window.used(now)).clamp(0.0, 100.0);
        if best.is_none_or(|(b, _)| left < b) {
            best = Some((left, window.reset_at));
        }
    }
    match best {
        None => "-".to_owned(),
        Some((left, reset)) => match reset {
            Some(at) if at > now => format!("{left:.0}% ({})", countdown(at, now)),
            _ => format!("{left:.0}%"),
        },
    }
}

/// Windows outside the two Codex ones that sit at or over the threshold.
fn other_limits(quotas: &Quotas, threshold: f64, now: u64) -> String {
    let hits: Vec<String> = quotas
        .iter()
        .filter(|(id, w)| kind(id, w) == Kind::Other && w.used(now) >= threshold)
        .map(|(id, w)| format!("{id} {:.0}%", w.used(now)))
        .collect();
    if hits.is_empty() {
        "-".to_owned()
    } else {
        hits.join(", ")
    }
}

fn account_row(account: &Value, default_threshold: f64, now: u64) -> Vec<String> {
    let threshold = account["threshold_percent"]
        .as_f64()
        .unwrap_or(default_threshold);
    let quotas: Quotas = account
        .get("quotas")
        .cloned()
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();
    let disabled = account["disabled"].as_bool().unwrap_or(false);
    let hold_until = account["hold_until"].as_u64().unwrap_or(0);
    let probe = match account.get("last_probe_ok").and_then(Value::as_bool) {
        Some(true) => "ok",
        Some(false) => "fail",
        None => "-",
    };
    let mut note = Vec::new();
    if let Some(error) = account.get("last_error").and_then(Value::as_str) {
        note.push(error.to_owned());
    }
    if hold_until > now {
        note.push(format!("hold {}", countdown(hold_until, now)));
    }
    vec![
        account["name"].as_str().unwrap_or("?").to_owned(),
        state_label(disabled, hold_until, &quotas, threshold, now).to_owned(),
        format!("{threshold:.0}%"),
        remaining(&quotas, Kind::Short, now),
        remaining(&quotas, Kind::Weekly, now),
        other_limits(&quotas, threshold, now),
        match account.get("reset_credits").and_then(Value::as_i64) {
            Some(count) => count.to_string(),
            None => "-".to_owned(),
        },
        account["in_flight"].as_u64().unwrap_or(0).to_string(),
        account["requests"].as_u64().unwrap_or(0).to_string(),
        account["errors"].as_u64().unwrap_or(0).to_string(),
        probe.to_owned(),
        if note.is_empty() {
            "-".to_owned()
        } else {
            note.join(", ")
        },
    ]
}

const HEADER: [&str; 12] = [
    "ACCOUNT",
    "STATE",
    "LIMIT AT",
    "5H LEFT",
    "WEEK LEFT",
    "OTHER LIMITS",
    "RESETS",
    "ACTIVE",
    "CALLS",
    "ERRORS",
    "PROBE",
    "NOTE",
];

/// Render the `/status` payload as a table. `threshold` is the configured
/// `threshold_percent`; `now` is the current Unix time.
pub fn render(status: &Value, threshold: f64, now: u64) -> String {
    let accounts = status["accounts"].as_array().cloned().unwrap_or_default();
    let rows: Vec<Vec<String>> = accounts
        .iter()
        .map(|a| account_row(a, threshold, now))
        .collect();
    let mut widths: Vec<usize> = HEADER.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let line = |cells: &[String]| -> String {
        let last = cells.len() - 1;
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i == last {
                    c.clone()
                } else {
                    format!("{c:<width$}", width = widths[i])
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
    };
    let ready = rows.iter().filter(|r| r[1] == "ready").count();
    let routing = match status["routing_healthy"].as_bool() {
        Some(true) => "ok",
        Some(false) => "UNHEALTHY",
        None => "-",
    };
    let mut out = format!(
        "accounts {}  ready {}  threshold {:.0}%  routing {}\n",
        rows.len(),
        ready,
        threshold,
        routing
    );
    let header: Vec<String> = HEADER.iter().map(|h| (*h).to_owned()).collect();
    out.push_str(&line(&header));
    out.push('\n');
    for row in &rows {
        out.push_str(&line(row));
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn status() -> Value {
        json!({
            "at": 1000,
            "routing_healthy": true,
            "accounts": [
                {
                    "name": "personal", "disabled": false, "hold_until": 0, "in_flight": 1,
                    "requests": 11, "errors": 0, "last_error": null, "last_probe_ok": true, "reset_credits": 2,
                    "quotas": {
                        "codex-primary": {"used_percent": 96.0, "reset_at": 100000, "window_minutes": 10080},
                        "gpt-5.3-codex-spark-primary": {"used_percent": 0.0, "reset_at": 2000, "window_minutes": 300}
                    }
                },
                {
                    "name": "team", "disabled": false, "hold_until": 1023, "in_flight": 0, "threshold_percent": 100.0,
                    "requests": 115, "errors": 746, "last_error": "credential_unavailable", "last_probe_ok": false,
                    "quotas": {
                        "codex-primary": {"used_percent": 0.0, "reset_at": 500, "window_minutes": 300},
                        "codex-secondary": {"used_percent": 16.0, "reset_at": 1000 + 6 * 86400, "window_minutes": 10080}
                    }
                },
                {
                    "name": "parked", "disabled": true, "hold_until": 0, "in_flight": 0,
                    "requests": 0, "errors": 0, "last_error": null, "last_probe_ok": null,
                    "quotas": {
                        "codex-primary": {"used_percent": 10.0, "reset_at": 100000, "window_minutes": 10080},
                        "codex-bengalfox-primary": {"used_percent": 99.0, "reset_at": 5000, "window_minutes": 300}
                    }
                }
            ]
        })
    }

    fn row<'a>(text: &'a str, name: &str) -> &'a str {
        text.lines()
            .find(|l| l.starts_with(name))
            .expect("row present")
    }

    #[test]
    fn weekly_only_account_shows_remaining_and_limited_state() {
        let text = render(&status(), 95.0, 1000);
        let personal = row(&text, "personal");
        assert!(personal.contains("limited"), "{personal}");
        assert!(personal.contains("  95%  "), "default column: {personal}");
        assert!(personal.contains("  -  "), "no short window: {personal}");
        assert!(personal.contains("4% (+1d03h)"), "{personal}");
        assert_eq!(
            personal.split_whitespace().nth(7),
            Some("2"),
            "reset credits: {personal}"
        );
        assert!(personal.contains("  ok  "), "{personal}");
        let team = row(&text, "team");
        assert_eq!(
            team.split_whitespace().nth(7),
            Some("-"),
            "unknown credits: {team}"
        );
    }

    #[test]
    fn short_and_weekly_windows_split_and_hold_shows_in_note() {
        let text = render(&status(), 95.0, 1000);
        let team = row(&text, "team");
        assert!(team.contains("waiting"), "{team}");
        assert!(team.contains("  100%  "), "override column: {team}");
        assert!(
            team.contains("100%"),
            "expired short window reads full: {team}"
        );
        assert!(team.contains("84% (+6d00h)"), "{team}");
        assert!(team.contains("fail"), "{team}");
        assert!(
            team.ends_with("credential_unavailable, hold +23s"),
            "{team}"
        );
    }

    #[test]
    fn disabled_account_lists_other_limits() {
        let text = render(&status(), 95.0, 1000);
        let parked = row(&text, "parked");
        assert!(parked.contains("disabled"), "{parked}");
        assert!(parked.contains("codex-bengalfox-primary 99%"), "{parked}");
        assert!(text.starts_with("accounts 3  ready 0  threshold 95%  routing ok\n"));
    }

    #[test]
    fn countdown_scales_with_distance() {
        assert_eq!(countdown(1045, 1000), "+45s");
        assert_eq!(countdown(1000 + 12 * 60, 1000), "+12m");
        assert_eq!(countdown(1000 + 5 * 3600 + 3 * 60, 1000), "+5h03m");
        assert_eq!(countdown(1000 + 4 * 86400 + 22 * 3600, 1000), "+4d22h");
        assert_eq!(countdown(900, 1000), "+0s");
    }

    #[test]
    fn empty_payload_renders_header_only() {
        let text = render(&json!({}), 95.0, 0);
        assert_eq!(text.lines().count(), 2);
        assert!(text.lines().nth(1).unwrap().starts_with("ACCOUNT"));
    }
}
