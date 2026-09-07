use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path,
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "listen")]
    pub listen: SocketAddr,
    pub client_token_env: String,
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
    pub credential: Credential,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub models: Vec<String>,
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
}

impl Config {
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
        ensure!(
            !self.accounts.is_empty(),
            "at least one account is required"
        );
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
            ensure!(
                !a.name.is_empty()
                    && a.name.len() <= 64
                    && a.name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
                "account names must contain 1-64 letters, digits, dots, underscores, or hyphens"
            );
            ensure!(names.insert(&a.name), "duplicate account name");
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
        let token =
            std::env::var(&self.client_token_env).context("client token variable is not set")?;
        ensure!(
            token.len() >= 16 && token.bytes().all(|b| b.is_ascii_graphic()),
            "client token must contain at least 16 visible ASCII characters"
        );
        Ok(token)
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
