use crate::{
    auth::Token,
    models, now,
    pool::{Lease, Pool},
    quota, reset,
    sse::Parser,
};
use anyhow::Context;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const MAX_BODY: usize = 16 * 1024 * 1024;

/// A connect failure never sends the request, so a retry on the same account
/// is safe. Codex retries a failed request for about three seconds. The
/// in-place retries and the connect hold complete inside that window, so a
/// short outage on the only eligible account recovers without a visible error.
const CONNECT_RETRY_DELAYS: [Duration; 2] =
    [Duration::from_millis(200), Duration::from_millis(600)];
const CONNECT_HOLD_SECONDS: u64 = 1;

#[derive(Clone)]
struct App {
    pool: Arc<Pool>,
    client_token: Arc<String>,
}

pub fn router(pool: Arc<Pool>, client_token: String) -> Router {
    Router::new().fallback(handle).with_state(App {
        pool,
        client_token: Arc::new(client_token),
    })
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error":{"type":code,"code":code,"message":message}})),
    )
        .into_response()
}

/// Codex parses a 429 body with `type: usage_limit_reached` as a final
/// usage-limit error and reports `resets_at` instead of retrying a bare 429.
/// `code` stays `pool_exhausted` for proxy clients.
fn pool_exhausted(resets_at: Option<u64>) -> Response {
    let mut body = json!({"error":{
        "type":"usage_limit_reached",
        "code":"pool_exhausted",
        "message":"No account is eligible; wait for quota reset or enable an account",
    }});
    if let Some(at) = resets_at {
        body["error"]["resets_at"] = json!(at);
    }
    (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response()
}

/// A held account is waiting out a transient failure. The answer names the
/// failure and carries `retry-after`, never a usage-limit reset time that the
/// upstream did not send.
fn held(reason: &str) -> Response {
    match reason {
        "rate_limited" => error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Every eligible account is rate limited; retry after the hold",
        ),
        "credential_unavailable" | "credential_refresh_failed" | "authentication_failed" => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credentials_unavailable",
            "No eligible account has working credentials",
        ),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream_unavailable",
            "An eligible account is waiting out an upstream failure",
        ),
    }
}

fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let Some(actual) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return false;
    };
    let difference = actual
        .as_bytes()
        .iter()
        .zip(expected.as_bytes())
        .fold(0, |diff, (a, b)| diff | (a ^ b));
    actual.len() == expected.len() && difference == 0
}

async fn handle(State(app): State<App>, request: Request) -> Response {
    if !authorized(request.headers(), &app.client_token) {
        return error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid local proxy token is required",
        );
    }
    if request.headers().contains_key("origin") {
        return error(
            StatusCode::FORBIDDEN,
            "browser_request",
            "Browser requests are not supported",
        );
    }
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    if path == "/health" && parts.method == Method::GET {
        return Json(json!({"status":"ok","version":env!("CARGO_PKG_VERSION")})).into_response();
    }
    if path == "/status" && parts.method == Method::GET {
        return Json(app.pool.snapshot()).into_response();
    }
    if path == "/reload" && parts.method == Method::POST {
        return match app.pool.reload_from_disk() {
            Ok(summary) => Json(summary).into_response(),
            Err(failure) => error(
                StatusCode::SERVICE_UNAVAILABLE,
                "reload_failed",
                &failure.to_string(),
            ),
        };
    }
    if let Some(name) = path
        .strip_prefix("/accounts/")
        .and_then(|p| p.strip_suffix("/enabled"))
    {
        if parts.method != Method::POST {
            return error(StatusCode::METHOD_NOT_ALLOWED, "method", "Use POST");
        }
        let Ok(bytes) = to_bytes(body, 1024).await else {
            return error(StatusCode::BAD_REQUEST, "body", "Invalid control body");
        };
        let Some(enabled) = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|v| v.get("enabled").and_then(Value::as_bool))
        else {
            return error(StatusCode::BAD_REQUEST, "body", "Expected enabled boolean");
        };
        return if app.pool.set_enabled(name, enabled) {
            Json(json!({"enabled":enabled})).into_response()
        } else {
            error(StatusCode::NOT_FOUND, "account", "Unknown account")
        };
    }
    if let Some(name) = path
        .strip_prefix("/accounts/")
        .and_then(|p| p.strip_suffix("/reset-credits"))
    {
        if parts.method != Method::GET {
            return error(StatusCode::METHOD_NOT_ALLOWED, "method", "Use GET");
        }
        let Some(idx) = app.pool.index(name) else {
            return error(StatusCode::NOT_FOUND, "account", "Unknown account");
        };
        return match reset::list(&app.pool, idx).await {
            Ok(value) => Json(value).into_response(),
            Err(failure) => error(
                StatusCode::BAD_GATEWAY,
                "reset_credits_failed",
                &failure.to_string(),
            ),
        };
    }
    if let Some(name) = path
        .strip_prefix("/accounts/")
        .and_then(|p| p.strip_suffix("/reset"))
    {
        if parts.method != Method::POST {
            return error(StatusCode::METHOD_NOT_ALLOWED, "method", "Use POST");
        }
        let Some(idx) = app.pool.index(name) else {
            return error(StatusCode::NOT_FOUND, "account", "Unknown account");
        };
        let Ok(bytes) = to_bytes(body, 4096).await else {
            return error(StatusCode::BAD_REQUEST, "body", "Invalid control body");
        };
        let value = if bytes.is_empty() {
            json!({})
        } else {
            match serde_json::from_slice::<Value>(&bytes) {
                Ok(value) if value.is_object() => value,
                _ => return error(StatusCode::BAD_REQUEST, "body", "Expected a JSON object"),
            }
        };
        let field = |key: &str| {
            value
                .get(key)
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
        };
        return match reset::redeem(&app.pool, idx, field("credit_id"), field("request_id")).await {
            Ok(outcome) => {
                let account = app.pool.snapshot().accounts.into_iter().nth(idx);
                Json(json!({
                    "account": name,
                    "code": outcome.code,
                    "windows_reset": outcome.windows_reset,
                    "reset_credits": account.as_ref().and_then(|a| a.reset_credits),
                    "quotas": account.map(|a| a.quotas).unwrap_or_default(),
                }))
                .into_response()
            }
            Err(failure) => error(
                StatusCode::BAD_GATEWAY,
                "reset_failed",
                &failure.to_string(),
            ),
        };
    }
    let endpoint = path
        .strip_prefix("/v1")
        .or_else(|| path.strip_prefix("/backend-api/codex"))
        .unwrap_or(path);
    let valid = (parts.method == Method::POST
        && matches!(endpoint, "/responses" | "/responses/compact"))
        || (parts.method == Method::GET && endpoint == "/models");
    if !valid {
        return error(
            StatusCode::NOT_FOUND,
            "route",
            "Unsupported endpoint or method",
        );
    }
    if parts
        .headers
        .get("content-encoding")
        .is_some_and(|v| v != "identity")
    {
        return error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "encoding",
            "Send an uncompressed JSON request",
        );
    }
    let bytes = match to_bytes(body, MAX_BODY).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body",
                "Request body exceeds 16 MiB",
            );
        }
    };
    let value = if parts.method == Method::POST {
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) if value.is_object() => value,
            _ => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "json",
                    "Request must be a JSON object",
                );
            }
        }
    } else {
        Value::Null
    };
    let model = value.get("model").and_then(Value::as_str).unwrap_or("");
    if parts.method == Method::POST && model.is_empty() {
        return error(StatusCode::BAD_REQUEST, "model", "A model is required");
    }
    if model.len() > 256 {
        return error(
            StatusCode::BAD_REQUEST,
            "model",
            "Model name exceeds 256 bytes",
        );
    }
    let group = parts
        .headers
        .get("x-tcx-group")
        .and_then(|v| v.to_str().ok());
    let session = [
        "session-id",
        "session_id",
        "thread-id",
        "x-codex-thread-id",
        "thread_id",
    ]
    .into_iter()
    .find_map(|name| {
        parts
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.is_empty())
    })
    .or_else(|| {
        value
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
    });
    if !app.pool.routing_healthy() {
        return routing_error();
    }
    let previous = value.get("previous_response_id").and_then(Value::as_str);
    let pinned = match previous {
        Some(id) => match app.pool.response_account(id) {
            Some(idx) => Some(idx),
            None => {
                return error(
                    StatusCode::CONFLICT,
                    "unknown_previous_response",
                    "Previous response is unknown; resend full conversation history",
                );
            }
        },
        None => None,
    };
    // An exhausted pool may hold an account that opted into automatic resets.
    // Redeem before selection, so this request lands on the cleared account.
    if !app.pool.has_eligible(model, group, pinned)
        && let Some(idx) = app.pool.auto_reset_candidate(model, group, pinned)
    {
        reset::auto(&app.pool, idx).await;
    }
    let mut tried = Vec::new();
    let mut auth_failure = false;
    let mut connect_failure = false;
    while let Some(lease) = app.pool.select(model, group, session, pinned, &tried) {
        let idx = lease.idx;
        tried.push(idx);
        let Some((account, auth)) = app.pool.entry(idx) else {
            continue;
        };
        let account = &account;
        let token = match auth.get(account, None).await {
            Ok(token) => token,
            Err(_) => {
                auth_failure = true;
                app.pool.hold(idx, now() + 30, "credential_unavailable");
                continue;
            }
        };
        let started = Instant::now();
        let mut result =
            send_with_connect_retry(&app.pool, account.base(), endpoint, &parts, &bytes, &token)
                .await;
        if result
            .as_ref()
            .is_ok_and(|r| r.status() == StatusCode::UNAUTHORIZED)
        {
            match auth.get(account, Some(token.generation)).await {
                Ok(token) => {
                    result = send_with_connect_retry(
                        &app.pool,
                        account.base(),
                        endpoint,
                        &parts,
                        &bytes,
                        &token,
                    )
                    .await
                }
                Err(_) => {
                    auth_failure = true;
                    app.pool.hold(idx, now() + 30, "credential_refresh_failed");
                    continue;
                }
            }
        }
        let upstream = match result {
            Ok(response) => response,
            Err(SendError::Connect) => {
                connect_failure = true;
                app.pool
                    .hold(idx, now() + CONNECT_HOLD_SECONDS, "connect_failed");
                continue;
            }
            Err(SendError::Unknown { reason, io_kind }) => {
                return send_failure(&app.pool, idx, reason, io_kind, started.elapsed());
            }
        };
        let status = upstream.status();
        app.pool
            .update_quotas(idx, quota::headers(upstream.headers()));
        if account.kind == crate::config::Kind::Api {
            app.pool
                .update_quotas(idx, quota::api_headers(upstream.headers(), model, now()));
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            app.pool.hold(
                idx,
                quota::retry_at(upstream.headers(), now()),
                "rate_limited",
            );
            continue;
        }
        if status == StatusCode::UNAUTHORIZED {
            auth_failure = true;
            app.pool.hold(idx, now() + 30, "authentication_failed");
            continue;
        }
        if !model.is_empty()
            && matches!(
                status,
                StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND
            )
            && !is_event_stream(upstream.headers())
        {
            let headers = response_headers(upstream.headers());
            let body = match read_bounded(upstream, MAX_BODY, app.pool.config.idle_timeout_seconds)
                .await
            {
                Ok(body) => body,
                Err(_) => {
                    app.pool.record(idx, 502, "response_read_failed", None);
                    return error(
                        StatusCode::BAD_GATEWAY,
                        "response_read_failed",
                        "Upstream response exceeded limits or ended early",
                    );
                }
            };
            let value = serde_json::from_slice::<Value>(&body).ok();
            if value
                .as_ref()
                .is_some_and(|value| models::unavailable(value, model))
            {
                app.pool.mark_model_unavailable(idx, model);
                app.pool
                    .record_model(idx, status.as_u16(), "model_unavailable", None, model);
                continue;
            }
            app.pool.record_model(
                idx,
                status.as_u16(),
                "upstream_rejected",
                value.as_ref(),
                model,
            );
            return buffered_response(status, headers, body);
        }
        return forward(
            upstream,
            lease,
            app.pool.config.idle_timeout_seconds,
            model.to_owned(),
        )
        .await;
    }
    if !app.pool.routing_healthy() {
        return routing_error();
    }
    let mut response = if app.pool.model_unavailable(model, group, pinned) {
        error(
            StatusCode::NOT_FOUND,
            "model_unavailable",
            "No account eligible for this request supports the requested model",
        )
    } else if auth_failure {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credentials_unavailable",
            "No eligible account has working credentials",
        )
    } else if connect_failure {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream_unavailable",
            "Cannot connect to an eligible upstream",
        )
    } else if let Some((_, reason)) = app.pool.active_hold() {
        held(&reason)
    } else {
        pool_exhausted(app.pool.quota_reset_at())
    };
    response.headers_mut().insert(
        "retry-after",
        HeaderValue::from_str(&app.pool.retry_seconds().to_string()).unwrap(),
    );
    response
}

fn routing_error() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "routing_storage_unavailable",
        "Routing state could not be saved; repair storage before restarting the proxy",
    )
}

enum SendError {
    Connect,
    Unknown {
        reason: &'static str,
        io_kind: Option<std::io::ErrorKind>,
    },
}

fn classify_send_error(error: reqwest::Error) -> SendError {
    if error.is_connect() {
        return SendError::Connect;
    }
    let reason = if error.is_timeout() {
        "upstream_request_timeout"
    } else if error.is_builder() {
        "upstream_request_invalid"
    } else if error.is_body() {
        "upstream_request_body_error"
    } else {
        "upstream_transport_error"
    };
    let mut source = std::error::Error::source(&error);
    let mut io_kind = None;
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<std::io::Error>() {
            io_kind = Some(error.kind());
            break;
        }
        source = cause.source();
    }
    SendError::Unknown { reason, io_kind }
}

fn send_failure(
    pool: &Pool,
    idx: usize,
    reason: &str,
    io_kind: Option<std::io::ErrorKind>,
    elapsed: Duration,
) -> Response {
    let elapsed_ms = elapsed.as_millis();
    let timeout_seconds = pool.config.idle_timeout_seconds;
    let io_kind = io_kind.map(|kind| format!("{kind:?}"));
    pool.record(idx, 502, reason, None);
    // Error text and its source chain can contain URLs, credentials, or request data.
    // Emit only fixed categories and numeric timing data, never their Display/Debug text.
    eprintln!(
        "{}",
        json!({
            "at": now(), "account": pool.account(idx).map(|a| a.name),
            "status": 502, "outcome": "upstream_outcome_unknown", "reason": reason,
            "io_kind": io_kind, "elapsed_ms": elapsed_ms,
            "timeout_seconds": timeout_seconds, "replayed": false,
        })
    );
    error(
        StatusCode::BAD_GATEWAY,
        "upstream_outcome_unknown",
        &format!(
            "Upstream request failed before response headers: {reason}; I/O: {}. \
             Elapsed: {elapsed_ms} ms; timeout: {timeout_seconds} s. \
             Upstream outcome is unknown; request was not replayed",
            io_kind.as_deref().unwrap_or("unavailable"),
        ),
    )
}

async fn send_with_connect_retry(
    pool: &Pool,
    base: &str,
    endpoint: &str,
    parts: &axum::http::request::Parts,
    body: &Bytes,
    token: &Token,
) -> Result<reqwest::Response, SendError> {
    let mut delays = CONNECT_RETRY_DELAYS.iter();
    loop {
        match send(pool, base, endpoint, parts, body, token).await {
            Err(SendError::Connect) => match delays.next() {
                Some(delay) => tokio::time::sleep(*delay).await,
                None => return Err(SendError::Connect),
            },
            result => return result,
        }
    }
}

async fn send(
    pool: &Pool,
    base: &str,
    endpoint: &str,
    parts: &axum::http::request::Parts,
    body: &Bytes,
    token: &Token,
) -> Result<reqwest::Response, SendError> {
    let mut url =
        reqwest::Url::parse(&format!("{base}{endpoint}")).map_err(|_| SendError::Unknown {
            reason: "upstream_url_invalid",
            io_kind: None,
        })?;
    url.set_query(parts.uri.query());
    let mut headers = HeaderMap::new();
    // Only protocol headers pass. Client credentials and cookies never pass.
    for name in [
        "content-type",
        "accept",
        "user-agent",
        "openai-beta",
        "session-id",
        "thread-id",
        "session_id",
        "thread_id",
        "x-codex-thread-id",
        "x-client-request-id",
        "x-codex-turn-state",
        "x-codex-routing-hint",
        "x-codex-turn-metadata",
        "originator",
    ] {
        if let Some(value) = parts.headers.get(name) {
            headers.insert(name, value.clone());
        }
    }
    headers.insert("accept-encoding", HeaderValue::from_static("identity"));
    if let Some(id) = &token.account_id {
        headers.insert(
            "chatgpt-account-id",
            HeaderValue::from_str(id).map_err(|_| SendError::Unknown {
                reason: "upstream_account_header_invalid",
                io_kind: None,
            })?,
        );
    }
    let request = pool
        .client
        .request(parts.method.clone(), url)
        .headers(headers)
        .bearer_auth(&token.access_token)
        .body(body.clone())
        .send();
    match tokio::time::timeout(
        Duration::from_secs(pool.config.idle_timeout_seconds),
        request,
    )
    .await
    {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(error)) => Err(classify_send_error(error)),
        Err(_) => Err(SendError::Unknown {
            reason: "upstream_response_header_timeout",
            io_kind: None,
        }),
    }
}

fn response_headers(source: &HeaderMap) -> HeaderMap {
    let mut output = HeaderMap::new();
    for (name, value) in source {
        let key = name.as_str();
        if matches!(
            key,
            "content-type"
                | "content-encoding"
                | "cache-control"
                | "retry-after"
                | "x-request-id"
                | "x-codex-turn-state"
        ) || (key.starts_with("x-codex-")
            && (key.contains("used-percent")
                || key.contains("reset-at")
                || key.contains("window-minutes")))
        {
            output.insert(name.clone(), value.clone());
        }
    }
    output
}

fn is_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/event-stream"))
}

fn buffered_response(status: StatusCode, headers: HeaderMap, bytes: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

struct Observation {
    lease: Lease,
    status: u16,
    recorded: bool,
    model: String,
}
impl Observation {
    fn finish(&mut self, outcome: &str, response: Option<&Value>) {
        if !self.recorded {
            self.lease.pool.record_model(
                self.lease.idx,
                self.status,
                outcome,
                response,
                &self.model,
            );
            self.recorded = true;
        }
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        self.finish("client_disconnected", None);
    }
}

async fn forward(upstream: reqwest::Response, lease: Lease, idle: u64, model: String) -> Response {
    let status = upstream.status();
    let headers = response_headers(upstream.headers());
    let stream = is_event_stream(upstream.headers());
    let mut observation = Observation {
        lease,
        status: status.as_u16(),
        recorded: false,
        model,
    };
    if stream {
        let mut source = upstream.bytes_stream();
        let output = async_stream::stream! {
            let mut parser = Parser::default();
            loop {
                match tokio::time::timeout(Duration::from_secs(idle), source.next()).await {
                    Ok(Some(Ok(bytes))) => {
                        for event in parser.feed(&bytes) {
                            observation.lease.pool.update_quotas(observation.lease.idx, quota::usage(&event, now()));
                            match event.get("type").and_then(Value::as_str) {
                                Some("response.completed") => observation.finish("complete", event.get("response")),
                                Some("response.failed" | "response.incomplete" | "error") => {
                                    if models::unavailable(&event, &observation.model) {
                                        observation.lease.pool.mark_model_unavailable(observation.lease.idx, &observation.model);
                                        observation.finish("stream_model_unavailable", event.get("response"));
                                    } else if let Some(until) = quota::stream_hold(&event, now()) {
                                        observation.lease.pool.defer(observation.lease.idx, until);
                                        observation.finish("stream_rate_limited", event.get("response"));
                                    } else {
                                        observation.finish("stream_failed", event.get("response"));
                                    }
                                },
                                _ => {},
                            }
                        }
                        yield Ok::<Bytes, std::io::Error>(bytes);
                    }
                    Ok(None) => {
                        if !observation.recorded {
                            observation.finish(if parser.overflowed { "observation_overflow" } else { "stream_truncated" }, None);
                        }
                        break;
                    }
                    _ => {
                        observation.finish("stream_transport_failed", None);
                        yield Err(std::io::Error::other("upstream stream interrupted"));
                        break;
                    }
                }
            }
        };
        let mut response = Response::new(Body::from_stream(output));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        response
    } else {
        let bytes = match read_bounded(upstream, MAX_BODY, idle).await {
            Ok(bytes) => bytes,
            Err(_) => {
                observation.finish("response_read_failed", None);
                return error(
                    StatusCode::BAD_GATEWAY,
                    "response_read_failed",
                    "Upstream response exceeded limits or ended early",
                );
            }
        };
        let value = serde_json::from_slice::<Value>(&bytes).ok();
        observation.finish(
            if status.is_success() {
                "complete"
            } else {
                "upstream_rejected"
            },
            value.as_ref(),
        );
        buffered_response(status, headers, bytes)
    }
}

pub async fn read_bounded(
    response: reqwest::Response,
    maximum: usize,
    idle: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = tokio::time::timeout(Duration::from_secs(idle), stream.next()).await? {
        let chunk = chunk?;
        anyhow::ensure!(
            bytes.len() + chunk.len() <= maximum,
            "response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Read one account's usage endpoint and record its quota windows and reset
/// credit count. Accounts without a usage endpoint are skipped as an error.
pub async fn probe_account(pool: &Arc<Pool>, idx: usize) -> anyhow::Result<()> {
    let (account, auth) = pool.entry(idx).context("unknown account")?;
    let account = &account;
    let usage = account.usage().context("no usage endpoint")?;
    let result = async {
        let token = auth.get(account, None).await?;
        let get = |token: Token| {
            let mut request = pool
                .client
                .get(usage)
                .bearer_auth(token.access_token)
                .timeout(Duration::from_secs(15));
            if let Some(id) = token.account_id {
                request = request.header("chatgpt-account-id", id);
            }
            request.send()
        };
        let mut response = get(token.clone()).await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            response = get(auth.get(account, Some(token.generation)).await?).await?;
        }
        anyhow::ensure!(response.status().is_success(), "probe rejected");
        let bytes = read_bounded(response, 1024 * 1024, 15).await?;
        let value: Value = serde_json::from_slice(&bytes)?;
        pool.update_quotas(idx, quota::usage(&value, now()));
        pool.set_reset_credits(idx, quota::reset_credits(&value));
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let mut state = pool.state.lock().unwrap();
    state.accounts[idx].last_probe = Some(now());
    state.accounts[idx].last_probe_ok = Some(result.is_ok());
    result
}

pub async fn probe_once(pool: &Arc<Pool>) {
    let jobs = pool
        .entries()
        .into_iter()
        .filter(|(idx, account, _)| {
            account.usage().is_some() && !pool.state.lock().unwrap().accounts[*idx].disabled
        })
        .map(|(idx, _, _)| async move {
            let _ = probe_account(pool, idx).await;
        });
    futures_util::future::join_all(jobs).await;
}

pub async fn probe_loop(pool: Arc<Pool>) {
    if pool.config.probe_interval_seconds == 0 {
        return;
    }
    loop {
        probe_once(&pool).await;
        tokio::time::sleep(Duration::from_secs(pool.config.probe_interval_seconds)).await;
    }
}

fn modified(path: &std::path::Path) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

/// Apply the configuration file to the pool each time its modification time changes.
/// `tcx login` and manual edits replace the file atomically, so a change is complete when seen.
pub async fn reload_loop(pool: Arc<Pool>, interval: Duration) {
    let Some(path) = pool.config_path() else {
        return;
    };
    let mut last = modified(&path);
    loop {
        tokio::time::sleep(interval).await;
        let current = modified(&path);
        if current == last {
            continue;
        }
        last = current;
        if current.is_none() {
            continue;
        }
        match pool.reload_from_disk() {
            Ok(summary) => eprintln!(
                "{}",
                json!({"at": now(), "event": "config_reloaded", "added": summary.added,
                    "updated": summary.updated, "removed": summary.removed,
                    "restart_required": summary.restart_required})
            ),
            // The message names a fixed category; Config::load emits no paths or values.
            Err(error) => eprintln!(
                "{}",
                json!({"at": now(), "event": "config_reload_failed", "reason": error.to_string()})
            ),
        }
    }
}
