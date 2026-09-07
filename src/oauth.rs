//! Browser OAuth and managed refresh. No Codex credential files are imported.
use crate::{auth::Token, now, storage};
use anyhow::{Context, Result, bail, ensure};
use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;

// Public OAuth client identifier used by the official Codex CLI, not a secret.
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const ISSUER: &str = "https://auth.openai.com";
const REFRESH_EARLY: u64 = 300;

#[derive(Clone, Serialize, Deserialize)]
pub struct StoredTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub account_id: String,
    pub user_id: String,
    pub expires_at: u64,
    pub email: Option<String>,
    #[serde(default)]
    pub retry_after: u64,
    #[serde(default)]
    pub login_required: bool,
}

impl StoredTokens {
    pub fn load(path: &Path) -> Result<Self> {
        let tokens: Self = serde_json::from_slice(&storage::read_private(path)?)
            .map_err(|_| anyhow::anyhow!("invalid managed credentials; run tcx login again"))?;
        tokens.validate()?;
        Ok(tokens)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        storage::atomic_write(path, &serde_json::to_vec(self)?)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.user_id.is_empty() && self.user_id.len() <= 256,
            "invalid managed user identity"
        );
        ensure!(
            !self.access_token.is_empty()
                && self.access_token.len() <= 65536
                && self.access_token.bytes().all(|b| b.is_ascii_graphic()),
            "invalid managed access token"
        );
        ensure!(
            !self.refresh_token.is_empty()
                && self.refresh_token.len() <= 65536
                && self.refresh_token.bytes().all(|b| b.is_ascii_graphic()),
            "invalid managed refresh token"
        );
        ensure!(
            !self.account_id.is_empty()
                && self.account_id.len() <= 256
                && axum::http::HeaderValue::from_str(&self.account_id).is_ok(),
            "invalid managed account identity"
        );
        Ok(())
    }

    fn token(&self) -> Token {
        Token {
            access_token: self.access_token.clone(),
            account_id: Some(self.account_id.clone()),
            user_id: Some(self.user_id.clone()),
            expires_at: Some(self.expires_at),
            generation: 0,
        }
    }
}

#[derive(Default, Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    refresh_token: Option<String>,
    id_token: Option<String>,
    expires_in: Option<u64>,
}

// These claims are metadata from the HTTPS token response, not proof supplied
// by a caller. The upstream validates the access token on every API request.
fn claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()
}

fn from_response(response: TokenResponse, previous: Option<&StoredTokens>) -> Result<StoredTokens> {
    let access_token = response
        .access_token
        .context("OAuth response has no access token")?;
    let access_claims = claims(&access_token).unwrap_or(Value::Null);
    let id_claims = response
        .id_token
        .as_deref()
        .and_then(claims)
        .unwrap_or(Value::Null);
    let account_id = access_claims
        .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id")
        .or_else(|| id_claims.pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| previous.map(|tokens| tokens.account_id.clone()))
        .context("OAuth response has no ChatGPT account identity")?;
    let user_id = access_claims
        .pointer("/https:~1~1api.openai.com~1auth/chatgpt_user_id")
        .or_else(|| access_claims.pointer("/https:~1~1api.openai.com~1auth/user_id"))
        .or_else(|| id_claims.pointer("/https:~1~1api.openai.com~1auth/chatgpt_user_id"))
        .or_else(|| id_claims.pointer("/https:~1~1api.openai.com~1auth/user_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| previous.map(|tokens| tokens.user_id.clone()))
        .context("OAuth response has no ChatGPT user identity")?;
    if let Some(previous) = previous {
        ensure!(
            previous.account_id == account_id && previous.user_id == user_id,
            "refreshed account identity changed; run tcx login again"
        );
    }
    let expires_at = access_claims
        .get("exp")
        .and_then(Value::as_u64)
        .or_else(|| {
            response
                .expires_in
                .map(|seconds| now().saturating_add(seconds))
        })
        .context("OAuth response has no access token expiry")?;
    ensure!(
        expires_at > now() + 5,
        "OAuth response contains an expired access token"
    );
    let refresh_token = response
        .refresh_token
        .or_else(|| previous.map(|tokens| tokens.refresh_token.clone()))
        .context("OAuth response has no refresh token")?;
    let email = id_claims
        .get("email")
        .or_else(|| id_claims.pointer("/https:~1~1api.openai.com~1profile/email"))
        .or_else(|| access_claims.pointer("/https:~1~1api.openai.com~1profile/email"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| previous.and_then(|tokens| tokens.email.clone()));
    let tokens = StoredTokens {
        access_token,
        refresh_token,
        account_id,
        user_id,
        expires_at,
        email,
        retry_after: 0,
        login_required: false,
    };
    tokens.validate()?;
    Ok(tokens)
}

struct OAuthClient {
    client: reqwest::Client,
    issuer: String,
}

impl OAuthClient {
    fn new(issuer: &str) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(15))
                .build()?,
            issuer: issuer.to_owned(),
        })
    }

    async fn tokens(
        &self,
        request: reqwest::RequestBuilder,
    ) -> std::result::Result<TokenResponse, bool> {
        // Error bodies, callback codes, and token responses never enter logs.
        let mut response = request.send().await.map_err(|_| false)?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| false)? {
            if bytes.len() + chunk.len() > 1024 * 1024 {
                return Err(false);
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            let code = body
                .get("error")
                .and_then(Value::as_str)
                .or_else(|| body.pointer("/error/code").and_then(Value::as_str))
                .or_else(|| body.get("code").and_then(Value::as_str));
            let permanent = matches!(status.as_u16(), 400 | 401 | 403)
                && matches!(
                    code,
                    Some(
                        "invalid_grant"
                            | "refresh_token_expired"
                            | "refresh_token_reused"
                            | "refresh_token_invalidated"
                    )
                );
            return Err(permanent);
        }
        serde_json::from_slice(&bytes).map_err(|_| false)
    }

    async fn exchange(
        &self,
        code: &str,
        verifier: &str,
        redirect_uri: &str,
    ) -> Result<StoredTokens> {
        let response = self
            .tokens(
                self.client
                    .post(format!("{}/oauth/token", self.issuer))
                    .form(&[
                        ("grant_type", "authorization_code"),
                        ("client_id", CLIENT_ID),
                        ("code", code),
                        ("code_verifier", verifier),
                        ("redirect_uri", redirect_uri),
                    ]),
            )
            .await
            .map_err(|_| anyhow::anyhow!("OAuth token exchange failed; run tcx login again"))?;
        from_response(response, None)
    }

    async fn managed(&self, path: &Path, rejected_access_token: Option<&str>) -> Result<Token> {
        // The lock has a separate inode, so atomic credential replacement does
        // not unlock competing processes that share this refresh token.
        let _lock = storage::lock(&path.with_extension("lock")).await?;
        let mut stored = StoredTokens::load(path)?;
        ensure!(
            !stored.login_required,
            "account login expired; run tcx login again"
        );
        let rejected = rejected_access_token == Some(stored.access_token.as_str());
        if !rejected && stored.expires_at > now() + REFRESH_EARLY {
            return Ok(stored.token());
        }
        if stored.retry_after > now() {
            if !rejected && stored.expires_at > now() + 5 {
                return Ok(stored.token());
            }
            bail!("account refresh is in cooldown");
        }
        let response = self.tokens(self.client.post(format!("{}/oauth/token", self.issuer)).json(&json!({
            "client_id": CLIENT_ID, "grant_type": "refresh_token", "refresh_token": stored.refresh_token,
        }))).await;
        match response {
            Ok(response) => {
                match from_response(response, Some(&stored)) {
                    Ok(updated) => {
                        updated.save(path)?;
                        Ok(updated.token())
                    }
                    Err(_) => {
                        // A successful response can rotate the refresh token even
                        // if its identity or token shape is unusable. Do not reuse it.
                        stored.login_required = true;
                        stored.save(path)?;
                        bail!("invalid refreshed credentials; run tcx login again")
                    }
                }
            }
            Err(permanent) => {
                stored.login_required = permanent;
                stored.retry_after = now() + 30;
                stored.save(path)?;
                if !permanent && !rejected && stored.expires_at > now() + 5 {
                    return Ok(stored.token());
                }
                bail!(if permanent {
                    "account login expired; run tcx login again"
                } else {
                    "account refresh unavailable; retry shortly"
                })
            }
        }
    }
}

pub async fn managed_token(path: &Path, rejected_access_token: Option<&str>) -> Result<Token> {
    OAuthClient::new(ISSUER)?
        .managed(path, rejected_access_token)
        .await
}

struct Callback {
    state: String,
    port: u16,
    sender: Mutex<Option<oneshot::Sender<std::result::Result<String, ()>>>>,
}

fn page(status: StatusCode, message: &'static str) -> Response {
    let mut response = (status, message).into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("referrer-policy", "no-referrer".parse().unwrap());
    response.headers_mut().insert(
        "content-security-policy",
        "default-src 'none'".parse().unwrap(),
    );
    response
}

fn local_host(headers: &HeaderMap, port: u16) -> bool {
    let host = headers.get("host").and_then(|v| v.to_str().ok());
    host == Some(format!("localhost:{port}").as_str())
        || host == Some(format!("127.0.0.1:{port}").as_str())
}

async fn callback(State(state): State<Arc<Callback>>, request: Request) -> Response {
    if !local_host(request.headers(), state.port) || request.headers().contains_key("origin") {
        return page(StatusCode::BAD_REQUEST, "Invalid login callback.");
    }
    let url = match reqwest::Url::parse(&format!("http://localhost{}", request.uri())) {
        Ok(url) => url,
        Err(_) => return page(StatusCode::BAD_REQUEST, "Invalid login callback."),
    };
    let pairs: Vec<_> = url.query_pairs().collect();
    let values = |name: &str| {
        pairs
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, value)| value.as_ref())
            .collect::<Vec<_>>()
    };
    let states = values("state");
    if states.len() != 1 || states[0] != state.state {
        return page(StatusCode::BAD_REQUEST, "Invalid login state.");
    }
    let codes = values("code");
    let errors = values("error");
    let result = if codes.len() == 1
        && errors.is_empty()
        && !codes[0].is_empty()
        && codes[0].len() <= 4096
    {
        Ok(codes[0].to_owned())
    } else if codes.is_empty() && errors.len() == 1 {
        Err(())
    } else {
        return page(StatusCode::BAD_REQUEST, "Invalid login callback.");
    };
    let Some(sender) = state.sender.lock().unwrap().take() else {
        return page(StatusCode::CONFLICT, "Login callback already received.");
    };
    let _ = sender.send(result);
    page(
        StatusCode::OK,
        "Authorization received. Return to the terminal to finish login.",
    )
}

struct LoginSession {
    url: String,
    redirect_uri: String,
    verifier: String,
    receiver: oneshot::Receiver<std::result::Result<String, ()>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for LoginSession {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl LoginSession {
    async fn start(client: &OAuthClient, port: u16) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
            .await
            .context("cannot open login callback port; close any other Codex login and retry")?;
        let port = listener.local_addr()?.port();
        let redirect_uri = format!("http://localhost:{port}/auth/callback");
        let verifier = storage::random_string()?;
        let challenge = URL_SAFE_NO_PAD.encode(digest(&SHA256, verifier.as_bytes()).as_ref());
        let state = storage::random_string()?;
        let mut url = reqwest::Url::parse(&format!("{}/oauth/authorize", client.issuer))?;
        url.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", CLIENT_ID),
            ("redirect_uri", redirect_uri.as_str()),
            ("scope", "openid profile email offline_access"),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("state", state.as_str()),
            ("id_token_add_organizations", "true"),
            ("codex_cli_simplified_flow", "true"),
            ("originator", "codex_cli_rs"),
        ]);
        let (sender, receiver) = oneshot::channel();
        let app = Router::new()
            .route("/auth/callback", get(callback))
            .with_state(Arc::new(Callback {
                state,
                port,
                sender: Mutex::new(Some(sender)),
            }));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            url: url.to_string(),
            redirect_uri,
            verifier,
            receiver,
            task,
        })
    }

    async fn finish(&mut self, client: &OAuthClient) -> Result<StoredTokens> {
        let code = tokio::time::timeout(Duration::from_secs(300), &mut self.receiver)
            .await
            .context("browser login timed out; run tcx login again")?
            .context("login callback stopped")?
            .map_err(|_| anyhow::anyhow!("browser login was denied"))?;
        client
            .exchange(&code, &self.verifier, &self.redirect_uri)
            .await
    }
}

pub async fn browser_login(no_browser: bool) -> Result<StoredTokens> {
    let client = OAuthClient::new(ISSUER)?;
    let mut session = LoginSession::start(&client, 1455).await?;
    eprintln!(
        "Open this URL and sign in to the account you want to add:\n{}",
        session.url
    );
    if !no_browser {
        let opener = if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        };
        let mut command = tokio::process::Command::new(opener);
        command
            .arg(&session.url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), command.status()).await;
    }
    tokio::select! {
        result = session.finish(&client) => result,
        _ = tokio::signal::ctrl_c() => bail!("login cancelled"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, extract::Form, routing::post};
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    fn jwt(account: &str, expires_at: u64, generation: &str) -> String {
        format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&json!({
                    "exp": expires_at, "generation": generation, "email": "tester@example.test",
                    "https://api.openai.com/auth": {"chatgpt_account_id": account, "chatgpt_user_id": "user-a"}
                }))
                .unwrap()
            )
        )
    }

    fn stored(expires_at: u64) -> StoredTokens {
        StoredTokens {
            access_token: jwt("workspace-a", expires_at, "old"),
            refresh_token: "synthetic-refresh-old".into(),
            account_id: "workspace-a".into(),
            user_id: "user-a".into(),
            expires_at,
            email: Some("tester@example.test".into()),
            retry_after: 0,
            login_required: false,
        }
    }

    struct TestServer {
        issuer: String,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn serve(router: Router) -> TestServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        TestServer { issuer, task }
    }

    #[tokio::test]
    async fn browser_pkce_callback_exchange_and_account_registration() {
        let challenge = Arc::new(Mutex::new(String::new()));
        let expected = challenge.clone();
        let token_calls = Arc::new(AtomicUsize::new(0));
        let calls = token_calls.clone();
        let server = serve(Router::new().route("/oauth/token", post(move |Form(form): Form<HashMap<String, String>>| {
            let expected = expected.clone();
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(form["grant_type"], "authorization_code");
                assert_eq!(form["client_id"], CLIENT_ID);
                assert_eq!(form["code"], "synthetic-code");
                assert_eq!(URL_SAFE_NO_PAD.encode(digest(&SHA256, form["code_verifier"].as_bytes()).as_ref()), *expected.lock().unwrap());
                Json(json!({"access_token":jwt("workspace-a", now()+3600, "login"),
                    "refresh_token":"synthetic-refresh-login", "id_token":jwt("workspace-a",now()+3600,"id")}))
            }
        }))).await;
        let client = OAuthClient::new(&server.issuer).unwrap();
        let mut session = LoginSession::start(&client, 0).await.unwrap();
        let url = reqwest::Url::parse(&session.url).unwrap();
        let params: HashMap<_, _> = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        assert_eq!(params["code_challenge_method"], "S256");
        assert!(params["scope"].contains("offline_access"));
        *challenge.lock().unwrap() = params["code_challenge"].clone();
        let browser = reqwest::Client::new();
        let callback_url = &params["redirect_uri"];
        for query in [
            "state=wrong&code=synthetic-code".to_owned(),
            format!("state={}&code=a&code=b", params["state"]),
            format!("state={}&state=other&code=a", params["state"]),
        ] {
            assert_eq!(
                browser
                    .get(format!("{callback_url}?{query}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
        }
        let good = format!(
            "{callback_url}?state={}&code=synthetic-code",
            params["state"]
        );
        assert_eq!(
            browser
                .get(&good)
                .header("origin", "https://example.invalid")
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        assert_eq!(
            browser
                .get(&good)
                .header("host", "example.invalid")
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        let response = browser.get(&good).send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(browser.get(&good).send().await.unwrap().status(), 409);
        let tokens = session.finish(&client).await.unwrap();
        assert_eq!(tokens.account_id, "workspace-a");
        assert_eq!(token_calls.load(Ordering::SeqCst), 1);
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        let mut initial = crate::config::Config::local(&path);
        initial.client_token_env = "TEAMCODEX_TEST_TOKEN_NOT_CONFIGURED".into();
        initial.save(&path).unwrap();
        let name = crate::login::register(&path, Some("personal"), tokens)
            .await
            .unwrap();
        assert_eq!(name, "personal");
        let config = crate::config::Config::load(&path).unwrap();
        assert_eq!(config.accounts.len(), 1);
        assert_eq!(config.client_token().unwrap().len(), 43);
        let crate::config::Credential::Managed { path: token_path } =
            &config.accounts[0].credential
        else {
            panic!("expected managed account")
        };
        let loaded = crate::auth::Auth::default()
            .get(&config.accounts[0], None)
            .await
            .unwrap();
        assert_eq!(loaded.account_id.as_deref(), Some("workspace-a"));
        assert!(
            StoredTokens::load(token_path)
                .unwrap()
                .refresh_token
                .starts_with("synthetic-refresh-")
        );
        let config_text = std::fs::read_to_string(&path).unwrap();
        assert!(!config_text.contains("synthetic-refresh"));
        assert!(!config_text.contains("header."));
        drop(session);
    }

    #[tokio::test]
    async fn refresh_is_coalesced_across_independent_clients_and_rotated_on_disk() {
        for force in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counted = calls.clone();
            let server = serve(Router::new().route("/oauth/token", post(move |Json(body): Json<Value>| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(body["grant_type"], "refresh_token");
                    assert_eq!(body["refresh_token"], "synthetic-refresh-old");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Json(json!({"access_token":jwt("workspace-a",now()+3600,"new"),"refresh_token":"synthetic-refresh-new"}))
                }
            }))).await;
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("account.json");
            let previous = stored(now() + if force { 3600 } else { 20 });
            previous.save(&path).unwrap();
            let clients: Vec<_> = (0..8)
                .map(|_| OAuthClient::new(&server.issuer).unwrap())
                .collect();
            let jobs = clients.iter().map(|client| {
                client.managed(&path, force.then_some(previous.access_token.as_str()))
            });
            let results = futures_util::future::join_all(jobs).await;
            assert!(results.iter().all(Result::is_ok));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let saved = StoredTokens::load(&path).unwrap();
            assert_eq!(saved.refresh_token, "synthetic-refresh-new");
            assert!(saved.expires_at > now() + 300);
        }
    }

    #[tokio::test]
    async fn transient_refresh_failure_keeps_valid_access_and_shares_cooldown() {
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::FORBIDDEN,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counted = calls.clone();
            let server = serve(Router::new().route(
                "/oauth/token",
                post(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    async move { (status, "private-response-must-not-appear") }
                }),
            ))
            .await;
            let client = OAuthClient::new(&server.issuer).unwrap();
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("account.json");
            let previous = stored(now() + 60);
            previous.save(&path).unwrap();
            for _ in 0..2 {
                assert_eq!(
                    client.managed(&path, None).await.unwrap().access_token,
                    previous.access_token
                );
            }
            let error = client
                .managed(&path, Some(&previous.access_token))
                .await
                .err()
                .unwrap()
                .to_string();
            assert!(!error.contains("private-response"));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(!StoredTokens::load(&path).unwrap().login_required);
            let mut retry = StoredTokens::load(&path).unwrap();
            retry.retry_after = 0;
            retry.save(&path).unwrap();
            assert!(client.managed(&path, None).await.is_ok());
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    async fn expired_refresh_or_changed_identity_requires_login_without_repeated_refresh() {
        for changed_identity in [0, 1, 2] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counted = calls.clone();
            let server = serve(Router::new().route("/oauth/token", post(move || {
                counted.fetch_add(1, Ordering::SeqCst);
                async move {
                    if changed_identity == 2 {
                        let token = jwt("workspace-a",now()+3600,"new");
                        let mut payload = claims(&token).unwrap();
                        payload["https://api.openai.com/auth"]["chatgpt_user_id"] = "user-b".into();
                        let token = format!("header.{}.signature", URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap()));
                        Json(json!({"access_token":token,"refresh_token":"new-private-token"})).into_response()
                    } else if changed_identity == 1 {
                        Json(json!({"access_token":jwt("other-account",now()+3600,"new"),"refresh_token":"new-private-token"})).into_response()
                    } else { (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid_grant", "error_description":"private-response"}))).into_response() }
                }
            }))).await;
            let client = OAuthClient::new(&server.issuer).unwrap();
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("account.json");
            stored(now() + 30).save(&path).unwrap();
            for _ in 0..2 {
                let error = client.managed(&path, None).await.err().unwrap().to_string();
                assert!(error.contains("tcx login"));
                assert!(!error.contains("private-response"));
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(StoredTokens::load(&path).unwrap().login_required);
        }
    }

    #[tokio::test]
    async fn permanent_oauth_error_formats_require_login() {
        for code in [
            "invalid_grant",
            "refresh_token_expired",
            "refresh_token_reused",
            "refresh_token_invalidated",
        ] {
            for body in [
                json!({"error":code}),
                json!({"error":{"code":code}}),
                json!({"code":code}),
            ] {
                let calls = Arc::new(AtomicUsize::new(0));
                let counted = calls.clone();
                let server = serve(Router::new().route(
                    "/oauth/token",
                    post(move || {
                        counted.fetch_add(1, Ordering::SeqCst);
                        let body = body.clone();
                        async move { (StatusCode::BAD_REQUEST, Json(body)) }
                    }),
                ))
                .await;
                let client = OAuthClient::new(&server.issuer).unwrap();
                let temp = tempfile::tempdir().unwrap();
                let path = temp.path().join("account.json");
                stored(now() + 30).save(&path).unwrap();
                for _ in 0..2 {
                    assert!(client.managed(&path, None).await.is_err());
                }
                assert!(StoredTokens::load(&path).unwrap().login_required);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn refresh_preserves_refresh_token_when_server_omits_replacement() {
        let server = serve(Router::new().route(
            "/oauth/token",
            post(|| async { Json(json!({"access_token":jwt("workspace-a",now()+3600,"new")})) }),
        ))
        .await;
        let client = OAuthClient::new(&server.issuer).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("account.json");
        stored(now() + 10).save(&path).unwrap();
        client.managed(&path, None).await.unwrap();
        assert_eq!(
            StoredTokens::load(&path).unwrap().refresh_token,
            "synthetic-refresh-old"
        );
    }

    #[tokio::test]
    async fn denied_browser_login_stops_without_token_exchange() {
        let client = OAuthClient::new("http://127.0.0.1:1").unwrap();
        let mut session = LoginSession::start(&client, 0).await.unwrap();
        let url = reqwest::Url::parse(&session.url).unwrap();
        let state = url
            .query_pairs()
            .find(|(key, _)| key == "state")
            .unwrap()
            .1
            .into_owned();
        reqwest::Client::new()
            .get(format!(
                "{}?state={state}&error=access_denied",
                session.redirect_uri
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(
            session.finish(&client).await.err().unwrap().to_string(),
            "browser login was denied"
        );
    }
}
