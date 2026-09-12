use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Window {
    pub used_percent: f64,
    pub reset_at: Option<u64>,
    pub window_minutes: Option<u64>,
}

impl Window {
    pub fn used(&self, timestamp: u64) -> f64 {
        if self.reset_at.is_some_and(|at| timestamp >= at) {
            0.0
        } else {
            self.used_percent
        }
    }
}

pub type Quotas = BTreeMap<String, Window>;

pub fn normalize(id: &str) -> String {
    id.to_ascii_lowercase().replace('_', "-")
}

pub fn headers(headers: &HeaderMap) -> Quotas {
    let mut quotas = Quotas::new();
    let mut prefixes = BTreeSet::new();
    for name in headers.keys() {
        if let Some(prefix) = name.as_str().strip_suffix("-used-percent")
            && prefix.starts_with("x-")
            && (prefix.ends_with("-primary") || prefix.ends_with("-secondary"))
        {
            prefixes.insert(prefix);
        }
    }
    for prefix in prefixes {
        let get = |suffix: &str| {
            headers
                .get(format!("{prefix}-{suffix}"))
                .and_then(|v| v.to_str().ok())
        };
        let Some(used) = get("used-percent")
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
        else {
            continue;
        };
        quotas.insert(
            prefix.trim_start_matches("x-").to_owned(),
            Window {
                used_percent: used,
                reset_at: get("reset-at").and_then(|v| v.parse().ok()),
                window_minutes: get("window-minutes").and_then(|v| v.parse().ok()),
            },
        );
    }
    quotas
}

/// API limits are observed for the requested model. The API does not identify
/// shared model-family buckets in these headers, so we do not infer them.
pub fn api_headers(headers: &HeaderMap, model: &str, timestamp: u64) -> Quotas {
    let mut quotas = Quotas::new();
    for resource in ["requests", "tokens", "project-tokens"] {
        let get = |part: &str| {
            headers
                .get(format!("x-ratelimit-{part}-{resource}"))
                .and_then(|v| v.to_str().ok())
        };
        let numeric = |part| {
            get(part)
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|n| n.is_finite() && *n >= 0.0)
        };
        if let (Some(limit), Some(remaining)) = (numeric("limit"), numeric("remaining")) {
            if limit <= 0.0 {
                continue;
            }
            let reset_at = get("reset")
                .and_then(duration_seconds)
                .map(|s| timestamp.saturating_add(s));
            quotas.insert(
                format!("api:{model}:{resource}"),
                Window {
                    used_percent: (1.0 - remaining.min(limit) / limit) * 100.0,
                    reset_at,
                    window_minutes: None,
                },
            );
        }
    }
    quotas
}

fn duration_seconds(mut value: &str) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    let mut total = 0.0;
    while !value.is_empty() {
        let end = value.find(|c: char| !c.is_ascii_digit() && c != '.')?;
        let amount: f64 = value[..end].parse().ok()?;
        value = &value[end..];
        let (unit, multiplier) = [
            ("ms", 0.001),
            ("d", 86400.0),
            ("h", 3600.0),
            ("m", 60.0),
            ("s", 1.0),
        ]
        .into_iter()
        .find(|(unit, _)| value.starts_with(unit))?;
        total += amount * multiplier;
        if !total.is_finite() || total < 0.0 || total > (86400 * 365) as f64 {
            return None;
        }
        value = &value[unit.len()..];
    }
    Some(total.ceil() as u64)
}

fn parse_window(value: &Value, timestamp: u64) -> Option<Window> {
    let used = value.get("used_percent")?.as_f64()?;
    if !used.is_finite() || used < 0.0 {
        return None;
    }
    Some(Window {
        used_percent: used,
        reset_at: value.get("reset_at").and_then(Value::as_u64).or_else(|| {
            value
                .get("reset_after_seconds")
                .and_then(Value::as_u64)
                .map(|s| timestamp.saturating_add(s))
        }),
        window_minutes: value
            .get("window_minutes")
            .and_then(Value::as_u64)
            .or_else(|| {
                value
                    .get("limit_window_seconds")
                    .and_then(Value::as_u64)
                    .map(|s| s / 60)
            }),
    })
}

fn bucket(output: &mut Quotas, id: &str, value: &Value, timestamp: u64) {
    for part in ["primary", "secondary"] {
        if let Some(window) = value
            .get(part)
            .or_else(|| value.get(format!("{part}_window")))
            .and_then(|v| parse_window(v, timestamp))
        {
            output.insert(format!("{}-{part}", normalize(id)), window);
        }
    }
}

pub fn usage(value: &Value, timestamp: u64) -> Quotas {
    let mut output = Quotas::new();
    if let Some(rate) = value.get("rate_limit") {
        bucket(&mut output, "codex", rate, timestamp);
    }
    if let Some(additional) = value
        .get("additional_rate_limits")
        .and_then(Value::as_array)
    {
        for item in additional {
            if let Some(id) = item
                .get("metered_limit_name")
                .or_else(|| item.get("limit_name"))
                .and_then(Value::as_str)
            {
                bucket(
                    &mut output,
                    id,
                    item.get("rate_limit").unwrap_or(item),
                    timestamp,
                );
            }
        }
    }
    if value.get("type").and_then(Value::as_str) == Some("codex.rate_limits") {
        let id = value
            .get("metered_limit_name")
            .or_else(|| value.get("limit_name"))
            .and_then(Value::as_str)
            .unwrap_or("codex");
        if let Some(rate) = value.get("rate_limits") {
            bucket(&mut output, id, rate, timestamp);
        }
    }
    output
}

/// The number of usage-limit reset credits the account can redeem, when the
/// usage or credit-list payload reports it.
pub fn reset_credits(value: &Value) -> Option<i64> {
    value
        .get("rate_limit_reset_credits")
        .unwrap_or(value)
        .get("available_count")
        .and_then(Value::as_i64)
        .map(|count| count.max(0))
}

pub fn retry_at(headers: &HeaderMap, timestamp: u64) -> u64 {
    let raw = headers.get("retry-after").and_then(|v| v.to_str().ok());
    let seconds = raw
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            raw.and_then(|v| httpdate::parse_http_date(v).ok())
                .and_then(|d| d.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs().saturating_sub(timestamp))
        })
        .unwrap_or(30);
    timestamp.saturating_add(seconds.clamp(1, 86400 * 7))
}

pub fn stream_hold(event: &Value, timestamp: u64) -> Option<u64> {
    let error = event
        .pointer("/response/error")
        .or_else(|| event.get("error"))
        .unwrap_or(event);
    if !matches!(
        error.get("code").and_then(Value::as_str),
        Some("rate_limit_exceeded" | "usage_limit_reached")
    ) {
        return None;
    }
    if let Some(reset) = error
        .get("resets_at")
        .or_else(|| error.get("reset_at"))
        .and_then(Value::as_u64)
    {
        return Some(reset.max(timestamp.saturating_add(1)));
    }
    let seconds = error
        .get("message")
        .and_then(Value::as_str)
        .and_then(|message| {
            message
                .split_once("Please try again in ")
                .map(|(_, tail)| tail)
        })
        .and_then(|tail| tail.split_whitespace().next())
        .and_then(|duration| duration_seconds(duration.trim_end_matches('.')))
        .unwrap_or(30);
    // The clock uses whole seconds. One extra second preserves the minimum
    // delay when the rejection arrives near the end of the current second.
    Some(
        timestamp
            .saturating_add(seconds.clamp(1, 86400 * 7))
            .saturating_add(1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn reads_extra_buckets_and_resets() {
        let q = usage(
            &json!({"rate_limit":{"primary_window":{"used_percent":99,"reset_at":100,"limit_window_seconds":604800}},
            "additional_rate_limits":[{"metered_limit_name":"codex_spark","rate_limit":{"secondary_window":{"used_percent":87,"reset_after_seconds":20}}}]}),
            90,
        );
        assert_eq!(q["codex-primary"].used(99), 99.0);
        assert_eq!(q["codex-primary"].used(100), 0.0);
        assert_eq!(q["codex-primary"].window_minutes, Some(10080));
        assert_eq!(q["codex-spark-secondary"].reset_at, Some(110));
    }
    #[test]
    fn reads_headers_and_rejects_nan() {
        let mut h = HeaderMap::new();
        h.insert("x-codex-primary-used-percent", "NaN".parse().unwrap());
        h.insert(
            "x-codex-spark-secondary-used-percent",
            "99".parse().unwrap(),
        );
        h.insert("x-codex-spark-secondary-reset-at", "123".parse().unwrap());
        let q = headers(&h);
        assert_eq!(q.len(), 1);
        assert_eq!(q["codex-spark-secondary"].reset_at, Some(123));
    }

    #[test]
    fn api_headers_support_compound_and_fractional_resets() {
        let mut h = HeaderMap::new();
        for (name, value) in [
            ("x-ratelimit-limit-requests", "100"),
            ("x-ratelimit-remaining-requests", "2"),
            ("x-ratelimit-reset-requests", "6m0s"),
            ("x-ratelimit-limit-tokens", "1000"),
            ("x-ratelimit-remaining-tokens", "100"),
            ("x-ratelimit-reset-tokens", "1s200ms"),
        ] {
            h.insert(name, value.parse().unwrap());
        }
        let quotas = api_headers(&h, "test", 100);
        assert_eq!(quotas["api:test:requests"].used_percent, 98.0);
        assert_eq!(quotas["api:test:requests"].reset_at, Some(460));
        assert_eq!(quotas["api:test:tokens"].reset_at, Some(102));
        assert_eq!(duration_seconds("2.5s"), Some(3));
        assert_eq!(duration_seconds("NaNs"), None);
        assert_eq!(duration_seconds("12"), None);
    }

    #[test]
    fn stream_limits_use_error_code_and_reported_delay() {
        let event = json!({"type":"response.failed","response":{"error":{
            "code":"rate_limit_exceeded","message":"Please try again in 11.054s. More details."}}});
        assert_eq!(stream_hold(&event, 100), Some(113));
        assert_eq!(
            stream_hold(
                &json!({"type":"error","code":"usage_limit_reached","resets_at":900}),
                100
            ),
            Some(900)
        );
        assert_eq!(
            stream_hold(
                &json!({"type":"error","code":"unknown_error","message":"Please try again in 1s."}),
                100
            ),
            None
        );
    }
}
