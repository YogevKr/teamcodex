use crate::{
    config::{Account, Credential, Kind},
    now,
};
use anyhow::{Result, anyhow, ensure};
use serde::Deserialize;
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command, sync::Mutex};

#[derive(Clone, Deserialize)]
pub struct Token {
    pub access_token: String,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub expires_at: Option<u64>,
    #[serde(skip)]
    pub generation: u64,
}

#[derive(Default)]
struct Cache {
    token: Option<Token>,
    loaded_at: u64,
    generation: u64,
    failed_at: Option<u64>,
}

#[derive(Default)]
pub struct Auth {
    cache: Mutex<Cache>,
}

impl Auth {
    // The mutex covers the command, so concurrent 401 responses cause one refresh.
    pub async fn get(&self, account: &Account, rejected_generation: Option<u64>) -> Result<Token> {
        let mut cache = self.cache.lock().await;
        let timestamp = now();
        if cache.failed_at.is_some_and(|at| timestamp < at + 5) {
            return Err(anyhow!("credential source is in cooldown"));
        }
        let ttl = match &account.credential {
            Credential::Env { .. } => 240,
            Credential::Command { cache_seconds, .. } => *cache_seconds,
        };
        if let Some(token) = &cache.token {
            let fresh = token
                .expires_at
                .is_none_or(|expiry| expiry > timestamp + 30);
            let rejected = rejected_generation == Some(token.generation);
            if fresh && !rejected && timestamp < cache.loaded_at.saturating_add(ttl.max(1)) {
                return Ok(token.clone());
            }
        }
        let result = load(account, rejected_generation.is_some()).await;
        match result {
            Ok(mut token) => {
                cache.generation += 1;
                token.generation = cache.generation;
                cache.loaded_at = timestamp;
                cache.failed_at = None;
                cache.token = Some(token.clone());
                Ok(token)
            }
            Err(error) => {
                cache.failed_at = Some(now());
                Err(error)
            }
        }
    }
}

async fn load(account: &Account, refresh: bool) -> Result<Token> {
    let mut token = match &account.credential {
        Credential::Env { name } => Token {
            access_token: std::env::var(name)
                .map_err(|_| anyhow!("credential variable is not set"))?,
            account_id: account.account_id.clone(),
            expires_at: None,
            generation: 0,
        },
        Credential::Command { argv, .. } => {
            let mut child = Command::new(&argv[0])
                .args(&argv[1..])
                .env("TEAMCODEX_REFRESH", if refresh { "1" } else { "0" })
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .map_err(|_| anyhow!("cannot start credential command"))?;
            let mut stdout = child
                .stdout
                .take()
                .ok_or_else(|| anyhow!("credential output unavailable"))?
                .take(65537);

            tokio::time::timeout(Duration::from_secs(15), async {
                let mut bytes = Vec::new();
                stdout.read_to_end(&mut bytes).await?;
                ensure!(bytes.len() <= 65536, "credential output exceeds limit");
                ensure!(child.wait().await?.success(), "credential command failed");
                serde_json::from_slice::<Token>(&bytes)
                    .map_err(|_| anyhow!("credential command must return token JSON"))
            })
            .await
            .map_err(|_| anyhow!("credential command timed out"))??
        }
    };
    ensure!(
        !token.access_token.is_empty() && token.access_token.bytes().all(|b| b.is_ascii_graphic()),
        "invalid access token"
    );
    token.account_id = account.account_id.clone().or(token.account_id);
    if account.kind == Kind::Chatgpt {
        ensure!(
            token.account_id.as_ref().is_some_and(|id| !id.is_empty()),
            "ChatGPT credential requires account_id"
        );
    }
    if let Some(id) = &token.account_id {
        ensure!(
            axum::http::HeaderValue::from_str(id).is_ok(),
            "invalid account_id"
        );
    }
    ensure!(
        token.expires_at.is_none_or(|expiry| expiry > now() + 5),
        "credential source returned an expired token"
    );
    Ok(token)
}
