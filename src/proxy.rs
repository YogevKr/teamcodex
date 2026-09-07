use crate::{
    auth::Token,
    now,
    pool::{Lease, Pool},
    quota,
    sse::Parser,
};
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
use std::{sync::Arc, time::Duration};

const MAX_BODY: usize = 16 * 1024 * 1024;

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
        "session_id",
        "x-codex-thread-id",
        "thread_id",
        "x-client-request-id",
    ]
    .into_iter()
    .find_map(|name| parts.headers.get(name).and_then(|v| v.to_str().ok()))
    .or_else(|| value.get("prompt_cache_key").and_then(Value::as_str));
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
    let mut tried = Vec::new();
    let mut auth_failure = false;
    let mut connect_failure = false;
    while let Some(lease) = app.pool.select(model, group, session, pinned, &tried) {
        let idx = lease.idx;
        tried.push(idx);
        let account = &app.pool.config.accounts[idx];
        let token = match app.pool.auth[idx].get(account, None).await {
            Ok(token) => token,
            Err(_) => {
                auth_failure = true;
                app.pool.hold(idx, now() + 30, "credential_unavailable");
                continue;
            }
        };
        let mut result = send(&app.pool, account.base(), endpoint, &parts, &bytes, &token).await;
        if result
            .as_ref()
            .is_ok_and(|r| r.status() == StatusCode::UNAUTHORIZED)
        {
            match app.pool.auth[idx]
                .get(account, Some(token.generation))
                .await
            {
                Ok(token) => {
                    result = send(&app.pool, account.base(), endpoint, &parts, &bytes, &token).await
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
                app.pool.hold(idx, now() + 5, "connect_failed");
                continue;
            }
            Err(SendError::Unknown) => {
                app.pool.record(idx, 502, "upstream_outcome_unknown", None);
                return error(
                    StatusCode::BAD_GATEWAY,
                    "upstream_outcome_unknown",
                    "Upstream outcome is unknown; request was not replayed",
                );
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
        return forward(
            upstream,
            lease,
            app.pool.config.idle_timeout_seconds,
            model.to_owned(),
        )
        .await;
    }
    let mut response = if auth_failure {
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
    } else {
        error(
            StatusCode::TOO_MANY_REQUESTS,
            "pool_exhausted",
            "No account is eligible; wait for quota reset or enable an account",
        )
    };
    response.headers_mut().insert(
        "retry-after",
        HeaderValue::from_str(&app.pool.retry_seconds().to_string()).unwrap(),
    );
    response
}

enum SendError {
    Connect,
    Unknown,
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
        reqwest::Url::parse(&format!("{base}{endpoint}")).map_err(|_| SendError::Unknown)?;
    url.set_query(parts.uri.query());
    let mut headers = HeaderMap::new();
    // Only protocol headers pass. Client credentials and cookies never pass.
    for name in [
        "content-type",
        "accept",
        "user-agent",
        "openai-beta",
        "session_id",
        "thread_id",
        "x-codex-thread-id",
        "x-client-request-id",
        "x-codex-turn-state",
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
            HeaderValue::from_str(id).map_err(|_| SendError::Unknown)?,
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
        Ok(Err(error)) if error.is_connect() => Err(SendError::Connect),
        _ => Err(SendError::Unknown),
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
    let stream = upstream
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/event-stream"));
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
                                    if let Some(until) = quota::stream_hold(&event, now()) {
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
        let mut response = Response::new(Body::from(bytes));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        response
    }
}

async fn read_bounded(
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

pub async fn probe_once(pool: &Arc<Pool>) {
    let jobs = pool
        .config
        .accounts
        .iter()
        .enumerate()
        .filter(|(idx, account)| {
            account.usage().is_some() && !pool.state.lock().unwrap().accounts[*idx].disabled
        })
        .map(|(idx, account)| async move {
            let result = async {
                let token = pool.auth[idx].get(account, None).await?;
                let get = |token: Token| {
                    let mut request = pool
                        .client
                        .get(account.usage().unwrap())
                        .bearer_auth(token.access_token)
                        .timeout(Duration::from_secs(15));
                    if let Some(id) = token.account_id {
                        request = request.header("chatgpt-account-id", id);
                    }
                    request.send()
                };
                let mut response = get(token.clone()).await?;
                if response.status() == StatusCode::UNAUTHORIZED {
                    response =
                        get(pool.auth[idx].get(account, Some(token.generation)).await?).await?;
                }
                anyhow::ensure!(response.status().is_success(), "probe rejected");
                let bytes = read_bounded(response, 1024 * 1024, 15).await?;
                let value: Value = serde_json::from_slice(&bytes)?;
                pool.update_quotas(idx, quota::usage(&value, now()));
                Ok::<(), anyhow::Error>(())
            }
            .await;
            let mut state = pool.state.lock().unwrap();
            state.accounts[idx].last_probe = Some(now());
            state.accounts[idx].last_probe_ok = Some(result.is_ok());
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
