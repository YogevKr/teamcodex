use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use teamcodex::{
    config::{Account, Config, Credential, Kind},
    now,
    pool::Pool,
    proxy,
    quota::Window,
};

const CLIENT_TOKEN: &str = "test-client-token-not-a-secret";

#[derive(Clone)]
struct Mock {
    calls: Arc<Mutex<Vec<Call>>>,
    behavior: Arc<Behavior>,
}

type Call = (HeaderMap, Vec<u8>, String);
type Behavior = dyn Fn(&str, &str, &[u8]) -> Response + Send + Sync;

async fn mock_handler(State(mock): State<Mock>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, 20 * 1024 * 1024).await.unwrap();
    let token = parts
        .headers
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap();
    let response = (mock.behavior)(token, parts.uri.path(), &bytes);
    mock.calls
        .lock()
        .unwrap()
        .push((parts.headers, bytes.to_vec(), parts.uri.to_string()));
    response
}

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(app: Router) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server { url, task }
}

fn config(base: &str) -> Config {
    Config {
        listen: "127.0.0.1:0".parse().unwrap(), client_token_env: "TEST_CLIENT_TOKEN".into(), client_token_file: None,
        threshold_percent: 95.0, probe_interval_seconds: 0, idle_timeout_seconds: 2,
        model_limits: HashMap::new(),
        prices: HashMap::new(),
        accounts: ["a", "b"].into_iter().map(|name| Account {
            name: name.into(), kind: Kind::Api, base_url: Some(base.into()), usage_url: None,
            account_id: None,
            user_id: None,
            credential: Credential::Command {
                argv: vec!["python3".into(), "-c".into(), format!("import json; print(json.dumps({{'access_token':'test-upstream-{name}'}}))")],
                cache_seconds: 240,
            },
            priority: 0, disabled: false, groups: vec![], models: vec![],
        }).collect(),
    }
}

fn mock(behavior: impl Fn(&str, &str, &[u8]) -> Response + Send + Sync + 'static) -> Mock {
    Mock {
        calls: Arc::new(Mutex::new(Vec::new())),
        behavior: Arc::new(behavior),
    }
}

async fn upstream(mock: &Mock) -> Server {
    serve(
        Router::new()
            .fallback(any(mock_handler))
            .with_state(mock.clone()),
    )
    .await
}

async fn gateway(config: Config) -> (Arc<Pool>, Server) {
    let pool = Pool::new(config).unwrap();
    let server = serve(proxy::router(pool.clone(), CLIENT_TOKEN.into())).await;
    (pool, server)
}

fn post(server: &Server) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .post(format!("{}/v1/responses", server.url))
        .bearer_auth(CLIENT_TOKEN)
}

async fn assert_send_failure(pool: &Pool, response: reqwest::Response, reason: &str) {
    assert_eq!(response.status(), 502);
    let body = response.text().await.unwrap();
    let value: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["error"]["code"], "upstream_outcome_unknown");
    let message = value["error"]["message"].as_str().unwrap();
    assert!(message.contains(reason), "{message}");
    assert!(message.contains("Elapsed:"));
    assert!(message.contains("timeout:"));
    assert!(message.contains("request was not replayed"));
    for private in [
        CLIENT_TOKEN,
        "test-upstream-a",
        "test-upstream-b",
        "private-query",
        "private-prompt",
    ] {
        assert!(!body.contains(private));
    }
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.recent.len(), 1);
    assert_eq!(snapshot.recent[0].status, 502);
    assert_eq!(snapshot.recent[0].outcome, reason);
    assert_eq!(snapshot.accounts[0].last_error.as_deref(), Some(reason));
    assert_eq!(snapshot.accounts[0].errors, 1);
    assert_eq!(snapshot.accounts[1].requests, 0);
    assert!(
        snapshot
            .accounts
            .iter()
            .all(|account| account.in_flight == 0)
    );
}

#[tokio::test]
async fn response_header_timeout_reports_reason_without_replay() {
    let calls = Arc::new(AtomicUsize::new(0));
    let source = serve(Router::new().fallback(any({
        let calls = calls.clone();
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<Response>()
        }
    })))
    .await;
    let mut cfg = config(&source.url);
    cfg.idle_timeout_seconds = 1;
    let (pool, server) = gateway(cfg).await;
    let response = post(&server)
        .query(&[("redaction", "private-query")])
        .json(&json!({"model":"test", "input":"private-prompt"}))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    assert_send_failure(&pool, response, "upstream_response_header_timeout").await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn transport_failure_reports_reason_without_replay() {
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = Server {
        url: format!("http://{}", listener.local_addr().unwrap()),
        task: tokio::spawn({
            let calls = calls.clone();
            async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut buffer = [0; 4096];
                    assert!(stream.read(&mut buffer).await.unwrap() > 0);
                    calls.fetch_add(1, Ordering::SeqCst);
                    // Close after receiving request bytes, before any response headers.
                }
            }
        }),
    };
    let (pool, server) = gateway(config(&source.url)).await;
    let response = post(&server)
        .query(&[("redaction", "private-query")])
        .json(&json!({"model":"test", "input":"private-prompt"}))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    assert_send_failure(&pool, response, "upstream_transport_error").await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn connection_failure_still_selects_another_account() {
    let mock = mock(|_, _, _| Json(completed("resp_connected")).into_response());
    let source = upstream(&mock).await;
    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config(&source.url);
    cfg.accounts[0].base_url = Some(format!("http://{}", unavailable.local_addr().unwrap()));
    drop(unavailable);
    let (pool, server) = gateway(cfg).await;
    let response = post(&server)
        .json(&json!({"model":"test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<Value>().await.unwrap()["id"],
        "resp_connected"
    );
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0["authorization"], "Bearer test-upstream-b");
    assert_eq!(
        pool.snapshot().accounts[0].last_error.as_deref(),
        Some("connect_failed")
    );
}

fn completed(id: &str) -> Value {
    json!({"id":id,"object":"response","status":"completed","output":[],"usage":{"input_tokens":11,"output_tokens":7,"input_tokens_details":{"cached_tokens":3}}})
}

fn sse_response() -> Response {
    let data = format!(
        "data: {{\"type\":\"response.created\"}}\r\n\r\ndata: {}\r\n\r\n",
        json!({"type":"response.completed","response":completed("resp_test")})
    );
    let chunks = data
        .as_bytes()
        .chunks(3)
        .map(|b| Ok::<_, std::io::Error>(Bytes::copy_from_slice(b)))
        .collect::<Vec<_>>();
    (
        [("content-type", "text/event-stream")],
        Body::from_stream(futures_util::stream::iter(chunks)),
    )
        .into_response()
}

#[tokio::test]
async fn rotates_on_429_replays_exact_body_and_isolates_credentials() {
    let mock = mock(|token, _, _| {
        if token.ends_with("-a") {
            (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "10")],
                "limited",
            )
                .into_response()
        } else {
            sse_response()
        }
    });
    let source = upstream(&mock).await;
    let (pool, server) = gateway(config(&source.url)).await;
    let body = r#"{ "model": "test", "stream": true, "input": "hello" }"#;
    let response = post(&server)
        .header("cookie", "private-cookie")
        .header("x-api-key", "private-api-key")
        .header("chatgpt-account-id", "wrong-account")
        .header("x-openai-actor-authorization", "private-actor")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("response.completed")
    );
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for (headers, bytes, _) in calls.iter() {
        assert_eq!(bytes, body.as_bytes());
        for name in [
            "cookie",
            "x-api-key",
            "chatgpt-account-id",
            "x-openai-actor-authorization",
        ] {
            assert!(!headers.contains_key(name));
        }
        assert_ne!(headers["authorization"], format!("Bearer {CLIENT_TOKEN}"));
    }
    let snapshot = pool.snapshot();
    assert!(snapshot.accounts[0].hold_until > now());
    assert_eq!(snapshot.accounts[1].input_tokens, 11);
    assert_eq!(snapshot.accounts[1].cached_tokens, 3);
    assert_eq!(snapshot.accounts[1].output_tokens, 7);
    assert_eq!(snapshot.accounts[1].in_flight, 0);
    let status = serde_json::to_string(&snapshot).unwrap();
    assert!(!status.contains("test-upstream"));
}

#[tokio::test]
async fn authentication_routes_and_control_do_not_reach_upstream() {
    let mock = mock(|_, _, _| Json(completed("resp_test")).into_response());
    let source = upstream(&mock).await;
    let (_, server) = gateway(config(&source.url)).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{}/status", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        post(&server)
            .header("origin", "https://example.com")
            .json(&json!({"model":"test"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .get(format!("{}/unknown", server.url))
            .bearer_auth(CLIENT_TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    for name in ["a", "b"] {
        assert_eq!(
            client
                .post(format!("{}/accounts/{name}/enabled", server.url))
                .bearer_auth(CLIENT_TOKEN)
                .json(&json!({"enabled":false}))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    let exhausted = post(&server)
        .json(&json!({"model":"test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(exhausted.status(), 429);
    assert!(exhausted.headers().contains_key("retry-after"));
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn session_affinity_and_response_chain_stay_on_account() {
    let mock = mock(|_, _, _| Json(completed("resp_chain")).into_response());
    let source = upstream(&mock).await;
    let (pool, server) = gateway(config(&source.url)).await;
    for _ in 0..2 {
        post(&server)
            .header("session_id", "session-1")
            .json(&json!({"model":"test"}))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
    }
    post(&server)
        .json(&json!({"model":"test","previous_response_id":"resp_chain"}))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    {
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert!(
            calls
                .iter()
                .all(|(headers, _, _)| headers["authorization"] == "Bearer test-upstream-a")
        );
    }
    pool.set_enabled("a", false);
    assert_eq!(
        post(&server)
            .json(&json!({"model":"test","previous_response_id":"resp_chain"}))
            .send()
            .await
            .unwrap()
            .status(),
        429
    );
    assert_eq!(
        post(&server)
            .json(&json!({"model":"test","previous_response_id":"unknown"}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(mock.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn forwards_5xx_once_and_strips_response_secrets() {
    let mock = mock(|_, _, _| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [
                ("set-cookie", "private-cookie"),
                ("authorization", "private"),
                ("retry-after", "8"),
            ],
            "original error",
        )
            .into_response()
    });
    let source = upstream(&mock).await;
    let (_, server) = gateway(config(&source.url)).await;
    let response = post(&server)
        .json(&json!({"model":"test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    assert_eq!(response.headers()["retry-after"], "8");
    assert!(!response.headers().contains_key("set-cookie"));
    assert!(!response.headers().contains_key("authorization"));
    assert_eq!(response.text().await.unwrap(), "original error");
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn sends_first_stream_chunk_before_upstream_finishes() {
    let mock = mock(|_, _, _| {
        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n"));
            tokio::time::sleep(Duration::from_millis(600)).await;
            yield Ok(Bytes::from_static(b"data: {\"type\":\"response.completed\",\"response\":{}}\n\n"));
        };
        (
            [("content-type", "text/event-stream")],
            Body::from_stream(stream),
        )
            .into_response()
    });
    let source = upstream(&mock).await;
    let (_, server) = gateway(config(&source.url)).await;
    let response = post(&server)
        .json(&json!({"model":"test","stream":true}))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let first = tokio::time::timeout(Duration::from_millis(250), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("response.created"));
    assert!(stream.next().await.unwrap().is_ok());
}

#[tokio::test]
async fn truncated_stream_is_recorded_without_replay() {
    let mock = mock(|_, _, _| {
        (
            [("content-type", "text/event-stream")],
            "data: {\"type\":\"response.created\"}\n\n",
        )
            .into_response()
    });
    let source = upstream(&mock).await;
    let (pool, server) = gateway(config(&source.url)).await;
    post(&server)
        .json(&json!({"model":"test"}))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
    assert_eq!(pool.snapshot().recent[0].outcome, "stream_truncated");
    assert_eq!(pool.snapshot().accounts[0].in_flight, 0);
}

#[tokio::test]
async fn probe_updates_multiple_windows_and_account_header() {
    let mock = mock(|_, path, _| {
        assert_eq!(path, "/usage");
        Json(json!({"rate_limit":{"primary_window":{"used_percent":98,"reset_at":now()+100}},
            "additional_rate_limits":[{"metered_limit_name":"codex_spark","rate_limit":{"secondary_window":{"used_percent":72,"reset_at":now()+200}}}]})).into_response()
    });
    let source = upstream(&mock).await;
    let mut cfg = config(&source.url);
    cfg.accounts.truncate(1);
    cfg.accounts[0].kind = Kind::Chatgpt;
    cfg.accounts[0].account_id = Some("test-account-id".into());
    cfg.accounts[0].usage_url = Some(format!("{}/usage", source.url));
    let pool = Pool::new(cfg).unwrap();
    proxy::probe_once(&pool).await;
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.accounts[0].last_probe_ok, Some(true));
    assert_eq!(snapshot.accounts[0].quotas.len(), 2);
    assert_eq!(
        mock.calls.lock().unwrap()[0].0["chatgpt-account-id"],
        "test-account-id"
    );
    assert!(pool.select("test", None, None, None, &[]).is_none());
}

#[test]
fn selection_obeys_priority_groups_models_quota_and_expiry() {
    let mut cfg = config("http://127.0.0.1:1");
    cfg.accounts[0].priority = 0;
    cfg.accounts[1].priority = 10;
    cfg.model_limits
        .insert("spark".into(), vec!["codex-spark".into()]);
    let pool = Pool::new(cfg).unwrap();
    pool.update_quotas(
        0,
        [(
            "codex-spark-primary".into(),
            Window {
                used_percent: 100.0,
                reset_at: Some(now() + 100),
                window_minutes: None,
            },
        )]
        .into(),
    );
    assert_eq!(pool.select("normal", None, None, None, &[]).unwrap().idx, 0);
    assert_eq!(pool.select("spark", None, None, None, &[]).unwrap().idx, 1);
    pool.update_quotas(
        0,
        [(
            "codex-spark-primary".into(),
            Window {
                used_percent: 100.0,
                reset_at: Some(now() - 1),
                window_minutes: None,
            },
        )]
        .into(),
    );
    assert_eq!(pool.select("spark", None, None, None, &[]).unwrap().idx, 0);
    let mut cfg = config("http://127.0.0.1:1");
    cfg.accounts[0].groups = vec!["reserved".into()];
    cfg.accounts[0].models = vec!["spark".into()];
    let pool = Pool::new(cfg).unwrap();
    assert_eq!(pool.select("spark", None, None, None, &[]).unwrap().idx, 1);
    assert_eq!(
        pool.select("spark", Some("reserved"), None, None, &[])
            .unwrap()
            .idx,
        0
    );
    assert!(
        pool.select("other", Some("reserved"), None, None, &[])
            .is_none()
    );
}

#[tokio::test]
async fn concurrent_401_responses_coalesce_credential_refresh() {
    let directory = tempfile::tempdir().unwrap();
    let counter = directory.path().join("counter");
    let script = "import json,os,sys,time\nfrom pathlib import Path\np=Path(sys.argv[1])\nn=int(p.read_text())+1 if p.exists() else 1\np.write_text(str(n))\ntime.sleep(0.05)\nprint(json.dumps({'access_token':'test-fresh' if os.environ['TEAMCODEX_REFRESH']=='1' else 'test-expired'}))";
    let mock = mock(|token, _, _| {
        if token.ends_with("expired") {
            StatusCode::UNAUTHORIZED.into_response()
        } else {
            Json(completed("resp_refreshed")).into_response()
        }
    });
    let source = upstream(&mock).await;
    let mut cfg = config(&source.url);
    cfg.accounts.truncate(1);
    cfg.accounts[0].credential = Credential::Command {
        argv: vec![
            "python3".into(),
            "-c".into(),
            script.into(),
            counter.to_str().unwrap().into(),
        ],
        cache_seconds: 240,
    };
    let (_, server) = gateway(cfg).await;
    let requests = (0..12).map(|_| post(&server).json(&json!({"model":"test"})).send());
    for response in futures_util::future::join_all(requests).await {
        assert_eq!(response.unwrap().status(), 200);
    }
    assert_eq!(std::fs::read_to_string(counter).unwrap(), "2");
}

#[test]
fn invalid_configuration_is_rejected() {
    let mut cfg = config("http://example.com");
    assert!(cfg.validate().is_err());
    cfg = config("https://example.com");
    cfg.listen = "0.0.0.0:4269".parse().unwrap();
    assert!(cfg.validate().is_err());
    cfg = config("https://example.com");
    cfg.accounts[0].usage_url = Some("https://other.example.com/usage".into());
    assert!(cfg.validate().is_err());
    cfg = config("https://example.com");
    cfg.accounts[1].name = "a".into();
    assert!(cfg.validate().is_err());
}

#[test]
fn tui_displays_live_quota_and_token_counts() {
    let pool = Pool::new(config("http://127.0.0.1:1")).unwrap();
    pool.record(0, 200, "complete", Some(&completed("resp_tui")));
    let backend = ratatui::backend::TestBackend::new(130, 24);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| teamcodex::tui::draw(frame, &pool))
        .unwrap();
    let buffer = terminal.backend().buffer();
    let text: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(text.contains("TeamCodex"));
    assert!(text.contains("11/7"));
    assert!(text.contains("unknown"));
    assert!(text.contains("complete"));
}

#[test]
fn price_estimate_counts_cached_input_once() {
    let mut cfg = config("http://127.0.0.1:1");
    cfg.prices.insert(
        "test".into(),
        teamcodex::config::Price {
            input_per_million: 2.0,
            cached_input_per_million: 0.5,
            output_per_million: 10.0,
        },
    );
    let pool = Pool::new(cfg).unwrap();
    pool.record_model(0, 200, "complete", Some(&completed("resp_cost")), "test");
    let account = &pool.snapshot().accounts[0];
    assert!((account.estimated_cost_usd - 0.0000875).abs() < 0.00000001);
    assert_eq!(account.unpriced_requests, 0);
    pool.record_model(
        0,
        200,
        "complete",
        Some(&completed("resp_unpriced")),
        "unknown",
    );
    assert_eq!(pool.snapshot().accounts[0].unpriced_requests, 1);
}

#[test]
fn additional_bucket_with_default_prefix_does_not_block_other_models() {
    let mut cfg = config("http://127.0.0.1:1");
    cfg.accounts.truncate(1);
    cfg.model_limits
        .insert("special".into(), vec!["codex-secondary".into()]);
    let pool = Pool::new(cfg).unwrap();
    pool.update_quotas(
        0,
        [(
            "codex-secondary-primary".into(),
            Window {
                used_percent: 100.0,
                reset_at: Some(now() + 60),
                window_minutes: None,
            },
        )]
        .into(),
    );
    assert!(pool.select("normal", None, None, None, &[]).is_some());
    assert!(pool.select("special", None, None, None, &[]).is_none());
}

#[tokio::test]
async fn api_headers_block_the_observed_model_before_another_request() {
    let mock = mock(|_, _, _| {
        (
            [
                ("x-ratelimit-limit-requests", "100"),
                ("x-ratelimit-remaining-requests", "1"),
                ("x-ratelimit-reset-requests", "1m"),
            ],
            Json(completed("resp_api_limit")),
        )
            .into_response()
    });
    let source = upstream(&mock).await;
    let mut cfg = config(&source.url);
    cfg.accounts.truncate(1);
    let (pool, server) = gateway(cfg).await;
    post(&server)
        .json(&json!({"model":"limited"}))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        pool.snapshot().accounts[0].quotas["api:limited:requests"].used_percent,
        99.0
    );
    assert_eq!(
        post(&server)
            .json(&json!({"model":"limited"}))
            .send()
            .await
            .unwrap()
            .status(),
        429
    );
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
    assert!(pool.select("other", None, None, None, &[]).is_some());
}

#[tokio::test]
async fn slow_failed_credentials_share_one_cooldown() {
    let directory = tempfile::tempdir().unwrap();
    let counter = directory.path().join("counter");
    let mut cfg = config("http://127.0.0.1:1");
    cfg.accounts[0].credential = Credential::Command {
        argv: vec!["python3".into(), "-c".into(),
            "import sys,time; from pathlib import Path; p=Path(sys.argv[1]); p.write_text(p.read_text()+'x' if p.exists() else 'x'); time.sleep(5.2); sys.exit(1)".into(),
            counter.to_str().unwrap().into()], cache_seconds: 240,
    };
    let auth = teamcodex::auth::Auth::default();
    let requests = (0..3).map(|_| auth.get(&cfg.accounts[0], None));
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        futures_util::future::join_all(requests),
    )
    .await
    .unwrap();
    assert!(result.iter().all(Result::is_err));
    assert_eq!(std::fs::read_to_string(counter).unwrap(), "x");
}

#[tokio::test]
async fn stream_rate_limit_holds_account_for_future_requests_without_replay() {
    let failed = format!(
        "data: {}\n\n",
        json!({"type":"response.failed","response":{"error":{
        "code":"rate_limit_exceeded","message":"Please try again in 60s."}}})
    );
    let expected = failed.clone();
    let mock = mock(move |token, _, _| {
        if token.ends_with("-a") {
            ([("content-type", "text/event-stream")], failed.clone()).into_response()
        } else {
            sse_response()
        }
    });
    let source = upstream(&mock).await;
    let (pool, server) = gateway(config(&source.url)).await;
    let response = post(&server)
        .header("session_id", "rate-limited-session")
        .json(&json!({"model":"test","stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), expected);
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        1,
        "The failed stream must not be replayed"
    );
    let snapshot = pool.snapshot();
    assert!(snapshot.accounts[0].hold_until >= now() + 60);
    assert_eq!(snapshot.accounts[0].errors, 1);
    assert_eq!(snapshot.recent[0].outcome, "stream_rate_limited");
    post(&server)
        .header("session_id", "rate-limited-session")
        .json(&json!({"model":"test","stream":true}))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0["authorization"], "Bearer test-upstream-b");
}

#[tokio::test]
async fn model_rejection_rotates_and_caches_only_the_account_model_pair() {
    for (status, rejection) in [
        (
            StatusCode::NOT_FOUND,
            json!({"error":{"code":"model_not_found"}}),
        ),
        (
            StatusCode::FORBIDDEN,
            json!({"error":{"code":"model_access_denied"}}),
        ),
        (
            StatusCode::BAD_REQUEST,
            json!({"detail":"The 'spark' model is not supported when using Codex with a ChatGPT account."}),
        ),
    ] {
        let mock = mock(move |token, _, body| {
            let value: Value = serde_json::from_slice(body).unwrap();
            if token.ends_with("-a") && value["model"] == "spark" {
                (status, Json(rejection.clone())).into_response()
            } else {
                Json(completed("resp_model")).into_response()
            }
        });
        let source = upstream(&mock).await;
        let mut cfg = config(&source.url);
        cfg.accounts[1].priority = 10;
        let (pool, server) = gateway(cfg).await;
        let body = r#"{ "model": "spark", "input": "keep the original request" }"#;
        for _ in 0..2 {
            assert_eq!(
                post(&server)
                    .header("session_id", "model-session")
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
        }
        assert_eq!(
            post(&server)
                .json(&json!({"model":"other"}))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.len(), 4);
        assert_eq!(
            calls[0].1, calls[1].1,
            "Account rotation must retain the model and body"
        );
        assert_eq!(calls[2].0["authorization"], "Bearer test-upstream-b");
        assert_eq!(calls[3].0["authorization"], "Bearer test-upstream-a");
        let snapshot = pool.snapshot();
        assert!(snapshot.accounts[0].unavailable_models["spark"] > now());
        assert_eq!(snapshot.accounts[0].hold_until, 0);
        assert!(snapshot.accounts[1].unavailable_models.is_empty());
    }
}

#[tokio::test]
async fn all_accounts_reject_model_then_recover_after_cache_expiry() {
    let reject = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let reject_upstream = reject.clone();
    let mock = mock(move |_, _, _| {
        if reject_upstream.load(std::sync::atomic::Ordering::SeqCst) {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error":{"code":"model_not_found"}})),
            )
                .into_response()
        } else {
            Json(completed("resp_restored")).into_response()
        }
    });
    let source = upstream(&mock).await;
    let (pool, server) = gateway(config(&source.url)).await;
    for _ in 0..2 {
        let response = post(&server)
            .json(&json!({"model":"spark"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404);
        assert_eq!(
            response.json::<Value>().await.unwrap()["error"]["code"],
            "model_unavailable"
        );
    }
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        2,
        "Cached rejections must not reach the upstream"
    );
    reject.store(false, std::sync::atomic::Ordering::SeqCst);
    for account in &mut pool.state.lock().unwrap().accounts {
        account.unavailable_models.insert("spark".into(), now() - 1);
    }
    assert_eq!(
        post(&server)
            .json(&json!({"model":"spark"}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(
        pool.snapshot()
            .accounts
            .iter()
            .all(|account| account.unavailable_models.is_empty())
    );
}

#[tokio::test]
async fn unrelated_rejections_are_not_replayed_or_cached_as_model_failures() {
    for (status, code) in [
        (StatusCode::BAD_REQUEST, "unsupported_parameter"),
        (StatusCode::FORBIDDEN, "permission_denied"),
        (StatusCode::FORBIDDEN, "content_policy_violation"),
        (StatusCode::NOT_FOUND, "endpoint_not_found"),
        (StatusCode::INTERNAL_SERVER_ERROR, "model_not_found"),
    ] {
        let body = format!("{{\"error\":{{\"code\":\"{code}\"}}}}");
        let expected = body.clone();
        let mock = mock(move |_, _, _| {
            (status, [("content-type", "application/json")], body.clone()).into_response()
        });
        let source = upstream(&mock).await;
        let (pool, server) = gateway(config(&source.url)).await;
        let response = post(&server)
            .json(&json!({"model":"spark"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.text().await.unwrap(), expected);
        assert_eq!(mock.calls.lock().unwrap().len(), 1);
        assert!(
            pool.snapshot()
                .accounts
                .iter()
                .all(|account| account.unavailable_models.is_empty())
        );
    }
}

#[tokio::test]
async fn model_rejection_keeps_previous_response_on_its_account() {
    let mock = mock(|token, _, _| {
        if token.ends_with("-a") {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error":{"code":"model_not_found"}})),
            )
                .into_response()
        } else {
            Json(completed("resp_other")).into_response()
        }
    });
    let source = upstream(&mock).await;
    let (pool, server) = gateway(config(&source.url)).await;
    pool.record(0, 200, "complete", Some(&completed("resp_pinned")));
    for _ in 0..2 {
        assert_eq!(
            post(&server)
                .json(&json!({"model":"spark","previous_response_id":"resp_pinned"}))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
    assert_eq!(
        post(&server)
            .json(&json!({"model":"spark","input":"full history"}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        mock.calls.lock().unwrap()[1].0["authorization"],
        "Bearer test-upstream-b"
    );
}

#[tokio::test]
async fn streamed_model_rejection_affects_future_requests_without_replay() {
    let failed = format!(
        "data: {}\n\ndata: {}\n\n",
        json!({"type":"response.output_text.delta","delta":"partial output"}),
        json!({"type":"response.failed","response":{"error":{"code":"model_not_found"}}})
    );
    let expected = failed.clone();
    let mock = mock(move |token, _, _| {
        if token.ends_with("-a") {
            ([("content-type", "text/event-stream")], failed.clone()).into_response()
        } else {
            sse_response()
        }
    });
    let source = upstream(&mock).await;
    let (pool, server) = gateway(config(&source.url)).await;
    let response = post(&server)
        .json(&json!({"model":"spark","stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), expected);
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.accounts[0].hold_until, 0);
    assert!(snapshot.accounts[0].unavailable_models["spark"] > now());
    assert_eq!(snapshot.recent[0].outcome, "stream_model_unavailable");
    assert_eq!(snapshot.accounts[0].errors, 1);
    post(&server)
        .json(&json!({"model":"spark","stream":true}))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        mock.calls.lock().unwrap()[1].0["authorization"],
        "Bearer test-upstream-b"
    );
}

#[test]
fn model_cache_is_bounded_and_model_checks_respect_groups_and_configuration() {
    let mut cfg = config("http://127.0.0.1:1");
    cfg.accounts[0].models = vec!["base".into()];
    cfg.accounts[1].groups = vec!["reserved".into()];
    let pool = Pool::new(cfg).unwrap();
    assert!(pool.model_unavailable("spark", None, None));
    assert!(!pool.model_unavailable("spark", Some("reserved"), None));
    assert!(!pool.model_unavailable("base", None, None));
    pool.mark_model_unavailable(0, "base");
    assert!(pool.select("base", None, None, None, &[]).is_none());
    assert!(pool.model_unavailable("base", None, None));
    pool.state.lock().unwrap().accounts[0]
        .unavailable_models
        .insert("base".into(), now() - 1);
    assert!(pool.select("base", None, None, None, &[]).is_some());
    for index in 0..500 {
        pool.mark_model_unavailable(0, &format!("model-{index}"));
    }
    assert_eq!(pool.snapshot().accounts[0].unavailable_models.len(), 256);
}

#[tokio::test]
async fn cache_routing_headers_body_and_turn_state_survive_unchanged() {
    for stable in ["session-id", "thread-id", "session_id", "prompt_cache_key"] {
        let mock = mock(|_, _, _| {
            let mut response = Json(completed("resp_cache")).into_response();
            response.headers_mut().insert(
                "x-codex-turn-state",
                "synthetic-turn-state".parse().unwrap(),
            );
            response
        });
        let source = upstream(&mock).await;
        let (_, server) = gateway(config(&source.url)).await;
        let body=br#"{ "model":"test", "prompt_cache_key":"shared-prefix", "prompt_cache_retention":"24h", "instructions":"fixed prefix", "input":[{"role":"user","content":"synthetic"}] }"#;
        for request_id in ["request-1", "request-2", "request-3"] {
            let mut request = post(&server)
                .header("content-type", "application/json")
                .header("x-client-request-id", request_id)
                .header("x-codex-routing-hint", "model=test")
                .header("x-codex-turn-state", "synthetic-turn-state");
            if stable != "prompt_cache_key" {
                request = request.header(stable, "stable-session");
            }
            let response = request.body(body.to_vec()).send().await.unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                response.headers()["x-codex-turn-state"],
                "synthetic-turn-state"
            );
            response.bytes().await.unwrap();
        }
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        for (headers, bytes, _) in calls.iter() {
            assert_eq!(headers["authorization"], "Bearer test-upstream-a");
            assert_eq!(bytes, body);
            assert_eq!(headers["x-codex-routing-hint"], "model=test");
            assert_eq!(headers["x-codex-turn-state"], "synthetic-turn-state");
            if stable != "prompt_cache_key" {
                assert_eq!(headers[stable], "stable-session");
            }
        }
    }
}

#[tokio::test]
async fn request_ids_do_not_create_affinity_and_models_keep_separate_bindings() {
    let mock = mock(|_, _, _| Json(completed("response")).into_response());
    let source = upstream(&mock).await;
    let (pool, server) = gateway(config(&source.url)).await;
    // A repeated diagnostic request ID is not a conversation identity.
    for _ in 0..2 {
        post(&server)
            .header("x-client-request-id", "diagnostic-only")
            .json(&json!({"model":"a"}))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
    }
    {
        let calls = mock.calls.lock().unwrap();
        assert_ne!(calls[0].0["authorization"], calls[1].0["authorization"]);
    }
    let first = pool
        .select("original", None, Some("thread"), None, &[])
        .unwrap()
        .idx;
    pool.mark_model_unavailable(first, "other");
    let other = pool
        .select("other", None, Some("thread"), None, &[])
        .unwrap()
        .idx;
    assert_ne!(first, other);
    assert_eq!(
        pool.select("original", None, Some("thread"), None, &[])
            .unwrap()
            .idx,
        first
    );
}

#[tokio::test]
async fn established_affinity_survives_priority_recovery_and_restart_with_reordered_accounts() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("routing.jsonl");
    let mut cfg = config("http://127.0.0.1:1");
    cfg.accounts[1].priority = 10;
    let pool = Pool::persistent(cfg.clone(), &path).unwrap();
    pool.set_enabled("a", false);
    assert_eq!(
        pool.select("test", None, Some("session"), None, &[])
            .unwrap()
            .idx,
        1
    );
    pool.record(1, 200, "complete", Some(&completed("chain")));
    pool.set_enabled("a", true);
    assert_eq!(
        pool.select("test", None, Some("session"), None, &[])
            .unwrap()
            .idx,
        1
    );
    assert_eq!(
        pool.select("test", None, Some("new-session"), None, &[])
            .unwrap()
            .idx,
        0
    );
    drop(pool);
    cfg.accounts.reverse();
    let restored = Pool::persistent(cfg.clone(), &path).unwrap();
    assert_eq!(
        restored
            .select("test", None, Some("session"), None, &[])
            .unwrap()
            .idx,
        0
    );
    assert_eq!(restored.response_account("chain"), Some(0));
    assert!(restored.snapshot().routing_persistent);
    assert!(restored.snapshot().routing_healthy);
    drop(restored);
    cfg.accounts[0].account_id = Some("different-account".into());
    let changed = Pool::persistent(cfg, &path).unwrap();
    assert_eq!(changed.response_account("chain"), None);
    assert_eq!(
        changed
            .select("test", None, Some("session"), None, &[])
            .unwrap()
            .idx,
        1
    );
}
