use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "listen")]
    pub listen: SocketAddr,
    #[serde(default = "client_token_env")]
    pub client_token_env: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_token_file: Option<PathBuf>,
    #[serde(default = "threshold")]
    pub threshold_percent: f64,
    #[serde(default = "probe_interval")]
    pub probe_interval_seconds: u64,
    #[serde(default = "idle_timeout")]
    pub idle_timeout_seconds: u64,
    #[serde(default)]
    pub model_limits: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub prices: HashMap<String, Price>,
    #[serde(default)]
    pub accounts: Vec<Account>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Price {
    pub input_per_million: f64,
    pub cached_input_per_million: f64,
    pub output_per_million: f64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Account {
    pub name: String,
    pub kind: Kind,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub usage_url: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    pub credential: Credential,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub models: Vec<String>,
    /// Per-account override of the top-level `threshold_percent`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold_percent: Option<f64>,
    /// Redeem a usage-limit reset credit automatically when this account is
    /// the only limited candidate for a request. Default: `false`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub auto_reset: bool,
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Chatgpt,
    Api,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Credential {
    Managed {
        path: PathBuf,
    },
    Env {
        name: String,
    },
    Command {
        argv: Vec<String>,
        #[serde(default = "credential_ttl")]
        cache_seconds: u64,
    },
}

fn listen() -> SocketAddr {
    "127.0.0.1:4269".parse().unwrap()
}
fn client_token_env() -> String {
    "TEAMCODEX_PROXY_TOKEN".to_owned()
}

pub fn default_path() -> Result<PathBuf> {
    Ok(
        PathBuf::from(std::env::var_os("HOME").context("HOME is not set; use --config")?)
            .join(".config/teamcodex/config.json"),
    )
}

pub fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
        "account names must contain 1-64 letters, digits, dots, underscores, or hyphens"
    );
    Ok(())
}
fn threshold() -> f64 {
    95.0
}
fn probe_interval() -> u64 {
    60
}
fn idle_timeout() -> u64 {
    300
}
fn credential_ttl() -> u64 {
    240
}

impl Account {
    /// The selection threshold for this account: its override, else the default.
    pub fn threshold(&self, default: f64) -> f64 {
        self.threshold_percent.unwrap_or(default)
    }

    pub fn base(&self) -> &str {
        self.base_url
            .as_deref()
            .unwrap_or(match self.kind {
                Kind::Chatgpt => "https://chatgpt.com/backend-api/codex",
                Kind::Api => "https://api.openai.com/v1",
            })
            .trim_end_matches('/')
    }

    pub fn usage(&self) -> Option<&str> {
        self.usage_url.as_deref().or_else(|| {
            (self.kind == Kind::Chatgpt && self.base_url.is_none())
                .then_some("https://chatgpt.com/backend-api/wham/usage")
        })
    }

    /// The usage-limit reset credit endpoint. It lives next to the usage
    /// endpoint, so only ChatGPT accounts with a usage endpoint have one.
    pub fn reset_credits(&self) -> Option<String> {
        if self.kind != Kind::Chatgpt {
            return None;
        }
        let usage = self.usage()?;
        let base = usage.strip_suffix("/usage")?;
        Some(format!("{base}/rate-limit-reset-credits"))
    }
}

impl Config {
    pub fn local(path: &Path) -> Self {
        Self {
            listen: listen(),
            client_token_env: client_token_env(),
            client_token_file: Some(path.with_extension("state").join("proxy.token")),
            threshold_percent: threshold(),
            probe_interval_seconds: probe_interval(),
            idle_timeout_seconds: idle_timeout(),
            model_limits: HashMap::new(),
            prices: HashMap::new(),
            accounts: Vec::new(),
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        crate::storage::atomic_write(path, &serde_json::to_vec_pretty(self)?)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).context("cannot read configuration")?;
        let config: Self = serde_json::from_slice(&bytes).map_err(|_| {
            anyhow::anyhow!("invalid configuration; check the documented JSON schema")
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.listen.ip().is_loopback(),
            "listen address must be loopback"
        );
        ensure!(
            !self.client_token_env.is_empty(),
            "client_token_env is required"
        );
        if let Some(path) = &self.client_token_file {
            ensure!(
                path.is_absolute(),
                "client_token_file must be an absolute path"
            );
        }
        ensure!(
            self.threshold_percent.is_finite()
                && self.threshold_percent > 0.0
                && self.threshold_percent <= 100.0,
            "threshold_percent must be in (0, 100]"
        );
        ensure!(
            self.idle_timeout_seconds > 0,
            "idle timeout must be positive"
        );
        let mut names = HashSet::new();
        for price in self.prices.values() {
            ensure!(
                [
                    price.input_per_million,
                    price.cached_input_per_million,
                    price.output_per_million
                ]
                .iter()
                .all(|v| v.is_finite() && *v >= 0.0),
                "prices must be finite and nonnegative"
            );
        }
        for a in &self.accounts {
            validate_name(&a.name)?;
            ensure!(names.insert(&a.name), "duplicate account name");
            if let Some(t) = a.threshold_percent {
                ensure!(
                    t.is_finite() && t > 0.0 && t <= 100.0,
                    "account threshold_percent must be in (0, 100]"
                );
            }
            validate_url(a.base())?;
            if let Some(url) = a.usage() {
                validate_url(url)?;
                ensure!(
                    reqwest::Url::parse(url)?.origin() == reqwest::Url::parse(a.base())?.origin(),
                    "usage URL must have the same origin as the account base URL"
                );
            }
            if let Some(id) = &a.account_id {
                ensure!(
                    !id.is_empty() && axum::http::HeaderValue::from_str(id).is_ok(),
                    "invalid account_id"
                );
            }
            match &a.credential {
                Credential::Managed { path } => {
                    ensure!(
                        a.account_id
                            .as_ref()
                            .is_some_and(|id| !id.is_empty() && id.len() <= 256)
                            && a.user_id
                                .as_ref()
                                .is_some_and(|id| !id.is_empty() && id.len() <= 256),
                        "managed credentials require account_id and user_id"
                    );
                    ensure!(
                        path.is_absolute(),
                        "managed credential path must be absolute"
                    );
                    ensure!(
                        a.kind == Kind::Chatgpt,
                        "managed OAuth credentials require a ChatGPT account"
                    );
                }
                Credential::Env { name } => {
                    ensure!(!name.is_empty(), "credential variable is required")
                }
                Credential::Command { argv, .. } => ensure!(
                    !argv.is_empty() && !argv[0].is_empty(),
                    "credential command is required"
                ),
            }
            if a.kind == Kind::Chatgpt && matches!(a.credential, Credential::Env { .. }) {
                ensure!(
                    a.account_id.is_some(),
                    "ChatGPT environment credentials require account_id"
                );
            }
        }
        Ok(())
    }

    pub fn client_token(&self) -> Result<String> {
        let token = match std::env::var(&self.client_token_env) {
            Ok(token) => token,
            Err(std::env::VarError::NotPresent) => {
                let path = self
                    .client_token_file
                    .as_ref()
                    .context("client token variable is not set")?;
                String::from_utf8(crate::storage::read_private(path)?)
                    .context("invalid local proxy token")?
            }
            Err(_) => anyhow::bail!("invalid local proxy token variable"),
        };
        ensure!(
            token.len() >= 16 && token.bytes().all(|b| b.is_ascii_graphic()),
            "client token must contain at least 16 visible ASCII characters"
        );
        Ok(token)
    }

    pub async fn prepare_client_token(&self) -> Result<String> {
        if std::env::var_os(&self.client_token_env).is_none()
            && let Some(path) = &self.client_token_file
        {
            crate::storage::local_token(path).await?;
        }
        self.client_token()
    }
}

fn validate_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value).context("invalid upstream URL")?;
    let local = url.host_str().is_some_and(|h| {
        h.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && local),
        "upstreams require HTTPS; HTTP is allowed only for loopback test servers"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "upstream URLs must not contain credentials, queries, or fragments"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_accounts_require_complete_identity() {
        let mut config = Config::local(Path::new("/tmp/teamcodex/config.json"));
        let valid = serde_json::json!({"name":"personal", "kind":"chatgpt",
            "account_id":"workspace-a", "user_id":"user-a",
            "credential":{"type":"managed", "path":"/tmp/teamcodex/account.json"}});
        config
            .accounts
            .push(serde_json::from_value(valid.clone()).unwrap());
        assert!(config.validate().is_ok());
        for field in ["account_id", "user_id"] {
            for value in [serde_json::Value::Null, serde_json::json!("")] {
                let mut invalid = valid.clone();
                invalid[field] = value;
                config.accounts[0] = serde_json::from_value(invalid).unwrap();
                assert!(config.validate().is_err());
            }
        }
        config.accounts[0] = serde_json::from_value(serde_json::json!({"name":"external",
            "kind":"api", "credential":{"type":"env", "name":"SYNTHETIC_TEST_KEY"}}))
        .unwrap();
        assert!(config.validate().is_ok());
    }
}
