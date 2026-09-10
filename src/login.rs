use crate::{
    config::{self, Account, Config, Credential, Kind},
    oauth::{self, StoredTokens},
    storage,
};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest::{SHA256, digest};
use std::path::Path;

pub async fn login(path: &Path, name: Option<&str>, no_browser: bool) -> Result<()> {
    if let Some(name) = name {
        config::validate_name(name)?;
    }
    if path.try_exists()? {
        Config::load(path)?;
    }
    let tokens = oauth::browser_login(no_browser).await?;
    let name = register(path, name, tokens).await?;
    match notify_server(&Config::load(path)?).await {
        Ok(Some(summary)) => {
            println!("Account {name} saved and loaded into the running pool.");
            if summary["restart_required"] == true {
                println!("Settings other than accounts changed; restart the server to apply them.");
            }
        }
        Ok(None) => println!("Account {name} saved. Run tcx server to start the pool."),
        Err(error) => {
            println!("Account {name} saved. The running server did not load it: {error}");
            println!("Run tcx reload, or restart the server.");
        }
    }
    Ok(())
}

/// Ask a running server to apply the configuration file. `None` means no server listens.
pub async fn notify_server(config: &Config) -> Result<Option<serde_json::Value>> {
    let client = reqwest::Client::builder().no_proxy().build()?;
    let response = match client
        .post(format!("http://{}/reload", config.listen))
        .bearer_auth(config.client_token()?)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) if error.is_connect() => return Ok(None),
        Err(error) => return Err(error).context("cannot reach the proxy"),
    };
    let status = response.status();
    let value: serde_json::Value = response.json().await.context("invalid proxy response")?;
    ensure!(
        status.is_success(),
        "{}",
        value["error"]["message"]
            .as_str()
            .unwrap_or("reload rejected")
    );
    Ok(Some(value))
}

pub async fn register(
    path: &Path,
    requested_name: Option<&str>,
    tokens: StoredTokens,
) -> Result<String> {
    let _config_lock = storage::lock(&path.with_extension("lock")).await?;
    let mut config = if path.try_exists()? {
        Config::load(path)?
    } else {
        Config::local(path)
    };
    let existing_identity = config.accounts.iter().find(|account| {
        account.account_id.as_deref() == Some(&tokens.account_id)
            && account.user_id.as_deref() == Some(&tokens.user_id)
    });
    let name = match (requested_name, existing_identity) {
        (Some(name), Some(account)) => {
            ensure!(
                name == account.name,
                "this account is already stored under another name"
            );
            name.to_owned()
        }
        (None, Some(account)) => account.name.clone(),
        (Some(name), None) => name.to_owned(),
        (None, None) => {
            let prefix: String = tokens
                .email
                .as_deref()
                .unwrap_or("account")
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || "-_.".contains(c) {
                        c
                    } else {
                        '_'
                    }
                })
                .take(48)
                .collect();
            let suffix = URL_SAFE_NO_PAD.encode(
                digest(
                    &SHA256,
                    format!("{}:{}", tokens.user_id, tokens.account_id).as_bytes(),
                )
                .as_ref(),
            );
            format!("{prefix}-{}", &suffix[..8])
        }
    };
    config::validate_name(&name)?;
    let token_path = match config.accounts.iter().find(|account| account.name == name) {
        Some(account) => {
            ensure!(
                account.account_id.as_deref() == Some(&tokens.account_id)
                    && account.user_id.as_deref() == Some(&tokens.user_id),
                "account name belongs to another identity; use another --name"
            );
            let Credential::Managed { path } = &account.credential else {
                anyhow::bail!("account uses external credentials; use another --name");
            };
            path.clone()
        }
        None => path
            .with_extension("state")
            .join("accounts")
            .join(format!("{name}.json")),
    };
    let _token_lock = storage::lock(&token_path.with_extension("lock")).await?;
    if token_path.try_exists()? {
        // Keep file safety errors fatal. A new verified login may repair invalid
        // JSON only when config already binds this path to the same identity.
        let bytes = storage::read_private(&token_path)?;
        match serde_json::from_slice::<StoredTokens>(&bytes) {
            Ok(previous) => ensure!(
                previous.account_id == tokens.account_id && previous.user_id == tokens.user_id,
                "stored account identity differs; use another --name"
            ),
            Err(_) => ensure!(
                config.accounts.iter().any(|account| account.name == name),
                "unregistered credential file exists; use another --name"
            ),
        }
    }
    tokens.save(&token_path)?;
    if !config.accounts.iter().any(|account| account.name == name) {
        config.accounts.push(Account {
            name: name.clone(),
            kind: Kind::Chatgpt,
            base_url: None,
            usage_url: None,
            account_id: Some(tokens.account_id),
            user_id: Some(tokens.user_id),
            credential: Credential::Managed { path: token_path },
            priority: 0,
            disabled: false,
            groups: Vec::new(),
            models: Vec::new(),
            threshold_percent: None,
        });
    }
    if config.client_token_file.is_none() {
        config.client_token_file = Some(path.with_extension("state").join("proxy.token"));
    }
    config.prepare_client_token().await?;
    config.save(path)?;
    Ok(name)
}

pub fn accounts(config: &Config) -> Result<()> {
    let rows: Vec<_> = config.accounts.iter().map(|account| {
        let mut row = serde_json::json!({"name": account.name, "kind": account.kind, "disabled": account.disabled,
            "account_id": account.account_id});
        if let Credential::Managed { path } = &account.credential {
            match StoredTokens::load(path) {
                Ok(tokens) => {
                    row["expires_at"] = tokens.expires_at.into();
                    row["login_required"] = tokens.login_required.into();
                }
                Err(_) => { row["login_required"] = true.into(); }
            }
        }
        row
    }).collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&rows).context("cannot format account list")?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(id: &str) -> StoredTokens {
        StoredTokens {
            access_token: format!("synthetic-access-{id}"),
            refresh_token: format!("synthetic-refresh-{id}"),
            account_id: id.into(),
            user_id: "user-a".into(),
            expires_at: crate::now() + 3600,
            email: Some("tester@example.test".into()),
            retry_after: 0,
            login_required: false,
        }
    }

    #[tokio::test]
    async fn workspace_members_keep_separate_accounts() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        let first = tokens("shared-workspace");
        let mut second = first.clone();
        second.user_id = "user-b".into();
        second.access_token = "synthetic-second-access".into();
        let a = register(&path, None, first.clone()).await.unwrap();
        let b = register(&path, None, second.clone()).await.unwrap();
        assert_ne!(a, b);
        assert!(register(&path, Some(&a), second).await.is_err());
        assert_eq!(register(&path, None, first).await.unwrap(), a);
        let config = Config::load(&path).unwrap();
        assert_eq!(config.accounts.len(), 2);
        assert_eq!(config.accounts[0].user_id.as_deref(), Some("user-a"));
        assert_eq!(config.accounts[1].user_id.as_deref(), Some("user-b"));
    }

    #[tokio::test]
    async fn relogin_repairs_invalid_json_only_for_configured_identity() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        register(&path, Some("a"), tokens("workspace-a"))
            .await
            .unwrap();
        let config = Config::load(&path).unwrap();
        let Credential::Managed { path: token_path } = &config.accounts[0].credential else {
            panic!()
        };
        storage::atomic_write(token_path, b"invalid JSON").unwrap();
        let mut wrong_user = tokens("workspace-a");
        wrong_user.user_id = "user-b".into();
        assert!(register(&path, Some("a"), wrong_user).await.is_err());
        register(&path, Some("a"), tokens("workspace-a"))
            .await
            .unwrap();
        assert_eq!(StoredTokens::load(token_path).unwrap().user_id, "user-a");
        #[cfg(unix)]
        {
            use std::os::unix::fs::{PermissionsExt, symlink};
            std::fs::set_permissions(token_path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(
                register(&path, Some("a"), tokens("workspace-a"))
                    .await
                    .is_err()
            );
            std::fs::remove_file(token_path).unwrap();
            let destination = temp.path().join("other.json");
            storage::atomic_write(&destination, b"unchanged").unwrap();
            symlink(&destination, token_path).unwrap();
            assert!(
                register(&path, Some("a"), tokens("workspace-a"))
                    .await
                    .is_err()
            );
            assert_eq!(std::fs::read(destination).unwrap(), b"unchanged");
        }
    }

    #[tokio::test]
    async fn concurrent_logins_preserve_accounts_and_relogin_preserves_settings() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        let (a, b) = tokio::join!(
            register(&path, Some("a"), tokens("account-a")),
            register(&path, Some("b"), tokens("account-b"))
        );
        a.unwrap();
        b.unwrap();
        let mut config = Config::load(&path).unwrap();
        assert_eq!(config.accounts.len(), 2);
        let account = config
            .accounts
            .iter_mut()
            .find(|account| account.name == "a")
            .unwrap();
        account.models = vec!["model-a".into()];
        account.groups = vec!["reserved".into()];
        account.priority = 7;
        account.disabled = true;
        config.save(&path).unwrap();
        register(&path, None, tokens("account-a")).await.unwrap();
        let config = Config::load(&path).unwrap();
        let account = config
            .accounts
            .iter()
            .find(|account| account.name == "a")
            .unwrap();
        assert_eq!(account.priority, 7);
        assert!(account.disabled);
        assert_eq!(account.models, ["model-a"]);
        assert_eq!(account.groups, ["reserved"]);
        assert_eq!(config.accounts.len(), 2);
        assert!(
            register(&path, Some("a"), tokens("different-account"))
                .await
                .is_err()
        );
        assert!(
            register(&path, Some("other-name"), tokens("account-a"))
                .await
                .is_err()
        );
        let unchanged = Config::load(&path).unwrap();
        assert_eq!(unchanged.accounts.len(), 2);
        assert!(
            !std::fs::read_to_string(path)
                .unwrap()
                .contains("synthetic-access")
        );
    }
}
