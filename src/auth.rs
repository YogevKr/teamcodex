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
    pub user_id: Option<String>,
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
            Credential::Managed { .. } => 30,
            Credential::Env { .. } => 240,
            Credential::Command { cache_seconds, .. } => *cache_seconds,
        };
        if let Some(token) = &cache.token {
            let early = if matches!(account.credential, Credential::Managed { .. }) {
                300
            } else {
                30
            };
            let fresh = token
                .expires_at
                .is_none_or(|expiry| expiry > timestamp + early);
            let rejected = rejected_generation == Some(token.generation);
            if fresh && !rejected && timestamp < cache.loaded_at.saturating_add(ttl.max(1)) {
                return Ok(token.clone());
            }
        }
        let rejected_token = cache
            .token
            .as_ref()
            .filter(|token| rejected_generation == Some(token.generation))
            .map(|token| token.access_token.as_str());
        let forced = rejected_token.is_some();
        let result = load(account, rejected_generation.is_some(), rejected_token).await;
        match result {
            Ok(mut token) => {
                // Reloading identical credentials must not hide a concurrent 401.
                // A completed forced refresh still advances the generation so
                // callers that rejected the old generation share that attempt.
                if forced
                    || cache.token.as_ref().is_none_or(|previous| {
                        previous.access_token != token.access_token
                            || previous.account_id != token.account_id
                    })
                {
                    cache.generation += 1;
                }
                token.generation = cache.generation;
                cache.loaded_at = timestamp;
                cache.failed_at = None;
                cache.token = Some(token.clone());
                Ok(token)
            }
            Err(error) => {
                cache.failed_at = Some(now());
                cache.token = None;
                Err(error)
            }
        }
    }
}

async fn load(account: &Account, refresh: bool, rejected_token: Option<&str>) -> Result<Token> {
    let mut token = match &account.credential {
        Credential::Managed { path } => crate::oauth::managed_token(path, rejected_token).await?,
        Credential::Env { name } => Token {
            access_token: std::env::var(name)
                .map_err(|_| anyhow!("credential variable is not set"))?,
            account_id: account.account_id.clone(),
            user_id: account.user_id.clone(),
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
    if matches!(account.credential, Credential::Managed { .. }) {
        ensure!(
            account
                .account_id
                .as_ref()
                .is_none_or(|id| Some(id) == token.account_id.as_ref()),
            "managed credentials belong to another account"
        );
    }
    ensure!(
        account
            .user_id
            .as_ref()
            .is_none_or(|id| Some(id) == token.user_id.as_ref()),
        "credentials belong to another user"
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

/// Renew managed accounts even when quota probing is disabled or traffic is idle.
pub async fn refresh_loop(pool: std::sync::Arc<crate::pool::Pool>) {
    loop {
        let jobs = pool
            .entries()
            .into_iter()
            .filter(|(idx, account, _)| {
                matches!(account.credential, Credential::Managed { .. })
                    && !pool.state.lock().unwrap().accounts[*idx].disabled
            })
            .map(|(idx, account, auth)| {
                let pool = &pool;
                async move {
                    let failed = auth.get(&account, None).await.is_err();
                    let mut state = pool.state.lock().unwrap();
                    if failed {
                        state.accounts[idx].last_error = Some("login_or_refresh_required".into());
                    } else if state.accounts[idx].last_error.as_deref()
                        == Some("login_or_refresh_required")
                    {
                        state.accounts[idx].last_error = None;
                    }
                }
            });
        futures_util::future::join_all(jobs).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn identical_background_reload_does_not_hide_an_in_flight_rejection() {
        let account: Account = serde_json::from_value(serde_json::json!({
            "name":"test", "kind":"api", "credential":{"type":"command","cache_seconds":30,
            "argv":["python3","-c","import os,json; print(json.dumps({'access_token': 'synthetic-refreshed' if os.environ['TEAMCODEX_REFRESH']=='1' else 'synthetic-original'}))"]}
        })).unwrap();
        let auth = Auth::default();
        let original = auth.get(&account, None).await.unwrap();
        auth.cache.lock().await.loaded_at = now() - 60;
        let reloaded = auth.get(&account, None).await.unwrap();
        assert_eq!(original.generation, reloaded.generation);
        let refreshed = auth.get(&account, Some(original.generation)).await.unwrap();
        assert_eq!(refreshed.access_token, "synthetic-refreshed");
        assert!(refreshed.generation > original.generation);
        let concurrent = auth.get(&account, Some(original.generation)).await.unwrap();
        assert_eq!(concurrent.generation, refreshed.generation);
    }
}
