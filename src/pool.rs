use crate::{
    affinity::{self, Kind as RouteKind, Routing},
    auth::Auth,
    config::{Account, Config},
    now,
    quota::{self, Quotas},
};
use anyhow::Context;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, RwLock},
};

const REMOVED_FROM_CONFIG: &str = "removed_from_config";

const MODEL_RETRY_SECONDS: u64 = 300;
const MODEL_CACHE_LIMIT: usize = 256;

#[derive(Default, Clone, Serialize)]
pub struct AccountState {
    pub name: String,
    pub disabled: bool,
    pub hold_until: u64,
    pub in_flight: u64,
    pub requests: u64,
    pub errors: u64,
    pub input_tokens: u64,
    pub cached_tokens: u64,
    pub output_tokens: u64,
    pub unmetered_requests: u64,
    pub estimated_cost_usd: f64,
    pub unpriced_requests: u64,
    pub quotas: Quotas,
    pub unavailable_models: HashMap<String, u64>,
    pub last_error: Option<String>,
    pub last_probe: Option<u64>,
    pub last_probe_ok: Option<bool>,
    /// Effective selection threshold: the account override or the default.
    pub threshold_percent: f64,
    /// Usage-limit reset credits the account can redeem, from the last probe
    /// or credit listing. `None` until the upstream reports a count.
    pub reset_credits: Option<i64>,
    /// Reset credits redeemed through this server.
    pub resets: u64,
    /// Unix time of the last redeemed reset.
    pub last_reset: Option<u64>,
    /// No automatic redeem before this Unix time.
    pub reset_retry_at: u64,
    #[serde(skip)]
    pub selected: u64,
}

#[derive(Clone, Serialize)]
pub struct RequestLog {
    pub at: u64,
    pub account: String,
    pub status: u16,
    pub outcome: String,
}

#[derive(Serialize)]
pub struct Snapshot {
    pub accounts: Vec<AccountState>,
    pub recent: Vec<RequestLog>,
    pub at: u64,
    pub routing_persistent: bool,
    pub routing_healthy: bool,
}

pub struct State {
    pub accounts: Vec<AccountState>,
    routing: Routing,
    sequence: u64,
    recent: VecDeque<RequestLog>,
}

/// One configured account with its credential cache and durable routing binding.
pub struct Entry {
    pub account: Account,
    pub auth: Arc<Auth>,
    pub binding: String,
}

impl Entry {
    fn new(account: Account) -> anyhow::Result<Self> {
        let binding = binding(&account)?;
        Ok(Self {
            account,
            auth: Arc::new(Auth::default()),
            binding,
        })
    }
}

fn binding(a: &Account) -> anyhow::Result<String> {
    let bytes = serde_json::to_vec(&(
        &a.name,
        a.kind,
        a.base(),
        &a.account_id,
        &a.user_id,
        &a.credential,
    ))?;
    Ok(affinity::hash(&bytes))
}

/// Outcome of applying a configuration file to the running pool.
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct Reload {
    pub added: Vec<String>,
    pub updated: Vec<String>,
    pub removed: Vec<String>,
    /// Settings other than `accounts` changed. They apply on the next start.
    pub restart_required: bool,
}

pub struct Pool {
    /// Startup settings. `accounts` is empty here; the live list is behind `accounts()`.
    pub config: Config,
    accounts: RwLock<Vec<Entry>>,
    pub state: Mutex<State>,
    pub client: reqwest::Client,
    config_path: OnceLock<PathBuf>,
    /// One reset redeem at a time, so concurrent exhausted requests spend one credit.
    pub reset_lock: tokio::sync::Mutex<()>,
}

// Lock order: `accounts` (read or write) before `state`. Never hold either across an await.
impl Pool {
    pub fn new(config: Config) -> anyhow::Result<Arc<Self>> {
        Self::with_routing(config, Routing::default())
    }

    pub fn persistent(config: Config, path: &std::path::Path) -> anyhow::Result<Arc<Self>> {
        Self::with_routing(config, Routing::open(path)?)
    }

    fn with_routing(mut config: Config, routing: Routing) -> anyhow::Result<Arc<Self>> {
        config.validate()?;
        let entries = std::mem::take(&mut config.accounts)
            .into_iter()
            .map(Entry::new)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let accounts = entries
            .iter()
            .map(|e| AccountState {
                name: e.account.name.clone(),
                disabled: e.account.disabled,
                threshold_percent: e.account.threshold(config.threshold_percent),
                ..Default::default()
            })
            .collect();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(10))
            .no_proxy()
            .build()?;
        Ok(Arc::new(Self {
            config,
            accounts: RwLock::new(entries),
            client,
            state: Mutex::new(State {
                accounts,
                routing,
                sequence: 0,
                recent: VecDeque::new(),
            }),
            config_path: OnceLock::new(),
            reset_lock: tokio::sync::Mutex::new(()),
        }))
    }

    /// Track the configuration file so `reload_from_disk` and the watcher can read it.
    pub fn set_config_path(&self, path: PathBuf) -> bool {
        self.config_path.set(path).is_ok()
    }

    pub fn config_path(&self) -> Option<PathBuf> {
        self.config_path.get().cloned()
    }

    pub fn len(&self) -> usize {
        self.accounts.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn account(&self, idx: usize) -> Option<Account> {
        self.accounts
            .read()
            .unwrap()
            .get(idx)
            .map(|e| e.account.clone())
    }

    /// Owned account and credential cache, safe to use across awaits.
    pub fn entry(&self, idx: usize) -> Option<(Account, Arc<Auth>)> {
        self.accounts
            .read()
            .unwrap()
            .get(idx)
            .map(|e| (e.account.clone(), e.auth.clone()))
    }

    pub fn entries(&self) -> Vec<(usize, Account, Arc<Auth>)> {
        self.accounts
            .read()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(idx, e)| (idx, e.account.clone(), e.auth.clone()))
            .collect()
    }

    pub fn reload_from_disk(&self) -> anyhow::Result<Reload> {
        let path = self
            .config_path
            .get()
            .context("configuration path is not tracked; restart the server")?;
        self.reload(Config::load(path)?)
    }

    /// Apply a configuration to the running pool without a restart.
    /// New accounts are appended. Existing accounts are updated in place; a changed
    /// identity or credential resets its credential cache and routing binding.
    /// Accounts missing from the file are disabled and keep their index.
    pub fn reload(&self, mut config: Config) -> anyhow::Result<Reload> {
        config.validate()?;
        let incoming = std::mem::take(&mut config.accounts);
        let mut summary = Reload {
            restart_required: serde_json::to_value(&self.config)? != serde_json::to_value(&config)?,
            ..Default::default()
        };
        let mut entries = self.accounts.write().unwrap();
        let mut state = self.state.lock().unwrap();
        let mut present = vec![false; entries.len()];
        for account in incoming {
            let Some(idx) = entries.iter().position(|e| e.account.name == account.name) else {
                summary.added.push(account.name.clone());
                state.accounts.push(AccountState {
                    name: account.name.clone(),
                    disabled: account.disabled,
                    threshold_percent: account.threshold(self.config.threshold_percent),
                    ..Default::default()
                });
                entries.push(Entry::new(account)?);
                continue;
            };
            present[idx] = true;
            let entry = &mut entries[idx];
            let live = &mut state.accounts[idx];
            let restored = live.last_error.as_deref() == Some(REMOVED_FROM_CONFIG);
            let binding = binding(&account)?;
            if serde_json::to_value(&entry.account)? == serde_json::to_value(&account)? && !restored
            {
                continue;
            }
            if entry.binding != binding {
                entry.auth = Arc::new(Auth::default());
                entry.binding = binding;
                live.quotas.clear();
                live.unavailable_models.clear();
                live.hold_until = 0;
            }
            if restored || entry.account.disabled != account.disabled {
                live.disabled = account.disabled;
            }
            if restored {
                live.last_error = None;
            }
            live.threshold_percent = account.threshold(self.config.threshold_percent);
            entry.account = account;
            summary.updated.push(entry.account.name.clone());
        }
        for (idx, seen) in present.into_iter().enumerate() {
            let live = &mut state.accounts[idx];
            if !seen && live.last_error.as_deref() != Some(REMOVED_FROM_CONFIG) {
                live.disabled = true;
                live.last_error = Some(REMOVED_FROM_CONFIG.to_owned());
                summary.removed.push(live.name.clone());
            }
        }
        Ok(summary)
    }

    pub fn snapshot(&self) -> Snapshot {
        let state = self.state.lock().unwrap();
        let mut accounts = state.accounts.clone();
        let at = now();
        for account in &mut accounts {
            account.unavailable_models.retain(|_, until| *until > at);
            for window in account.quotas.values_mut() {
                window.used_percent = window.used(at);
            }
        }
        Snapshot {
            accounts,
            recent: state.recent.iter().cloned().collect(),
            at,
            routing_persistent: state.routing.persistent(),
            routing_healthy: state.routing.healthy(),
        }
    }

    pub fn set_enabled(&self, name: &str, enabled: bool) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(account) = state.accounts.iter_mut().find(|a| a.name == name) else {
            return false;
        };
        account.disabled = !enabled;
        true
    }

    pub fn response_account(&self, id: &str) -> Option<usize> {
        let entries = self.accounts.read().unwrap();
        let state = self.state.lock().unwrap();
        let binding = state
            .routing
            .get(RouteKind::Response, &affinity::hash(id.as_bytes()))?;
        entries.iter().position(|e| e.binding == binding)
    }

    pub fn routing_healthy(&self) -> bool {
        self.state.lock().unwrap().routing.healthy()
    }

    pub fn select(
        self: &Arc<Self>,
        model: &str,
        group: Option<&str>,
        session: Option<&str>,
        pinned: Option<usize>,
        tried: &[usize],
    ) -> Option<Lease> {
        let entries = self.accounts.read().unwrap();
        let mut state = self.state.lock().unwrap();
        if !state.routing.healthy() {
            return None;
        }
        let session =
            session.map(|key| affinity::hash(&serde_json::to_vec(&(model, group, key)).unwrap()));
        let timestamp = now();
        let candidates: Vec<usize> = state
            .accounts
            .iter()
            .enumerate()
            .filter_map(|(idx, account)| {
                let eligible = !tried.contains(&idx)
                    && self.eligible(
                        &entries[idx].account,
                        idx,
                        account,
                        model,
                        group,
                        pinned,
                        timestamp,
                    );
                eligible.then_some(idx)
            })
            .collect();
        // An eligible established session stays on its account, including after
        // a higher-priority account recovers. Priority selects new sessions.
        let affinity = session
            .as_deref()
            .and_then(|key| state.routing.get(RouteKind::Session, key))
            .and_then(|binding| entries.iter().position(|e| e.binding == binding))
            .filter(|idx| candidates.contains(idx));
        let priority = candidates
            .iter()
            .map(|&idx| entries[idx].account.priority)
            .min()?;
        let candidates: Vec<_> = candidates
            .into_iter()
            .filter(|&idx| entries[idx].account.priority == priority)
            .collect();
        let idx = affinity.or_else(|| {
            candidates.into_iter().min_by_key(|&idx| {
                let account = &state.accounts[idx];
                let reset = account
                    .quotas
                    .values()
                    .filter_map(|w| w.reset_at)
                    .filter(|at| *at > timestamp)
                    .min()
                    .unwrap_or(u64::MAX);
                (account.in_flight, reset, account.selected)
            })
        })?;
        if let Some(key) = session
            && state
                .routing
                .remember(RouteKind::Session, key, entries[idx].binding.clone())
                .is_err()
        {
            return None;
        }
        state.sequence += 1;
        let sequence = state.sequence;
        state.accounts[idx].in_flight += 1;
        state.accounts[idx].selected = sequence;
        Some(Lease {
            pool: self.clone(),
            idx,
        })
    }

    pub fn update_quotas(&self, idx: usize, quotas: Quotas) {
        self.state.lock().unwrap().accounts[idx]
            .quotas
            .extend(quotas);
    }

    pub fn set_reset_credits(&self, idx: usize, count: Option<i64>) {
        if let Some(count) = count {
            self.state.lock().unwrap().accounts[idx].reset_credits = Some(count);
        }
    }

    /// Record a redeemed reset. The Codex windows read as empty until the next
    /// probe or response header reports the real value.
    pub fn record_reset(&self, idx: usize) {
        let mut state = self.state.lock().unwrap();
        let account = &mut state.accounts[idx];
        account.resets += 1;
        account.last_reset = Some(now());
        if let Some(count) = account.reset_credits.as_mut() {
            *count = (*count - 1).max(0);
        }
        for (id, window) in account.quotas.iter_mut() {
            if matches!(id.as_str(), "codex-primary" | "codex-secondary") {
                window.used_percent = 0.0;
            }
        }
    }

    /// Block automatic redeems on the account until `until`.
    pub fn defer_reset(&self, idx: usize, until: u64) {
        let account = &mut self.state.lock().unwrap().accounts[idx];
        account.reset_retry_at = account.reset_retry_at.max(until);
    }

    /// True while an automatic redeem on `idx` still helps: the account opted
    /// in, is enabled, holds a credit, sits outside its cooldown, and a Codex
    /// window still blocks it.
    pub fn auto_reset_ready(&self, idx: usize) -> bool {
        let entries = self.accounts.read().unwrap();
        let state = self.state.lock().unwrap();
        let timestamp = now();
        let (Some(entry), Some(account)) = (entries.get(idx), state.accounts.get(idx)) else {
            return false;
        };
        entry.account.auto_reset
            && !account.disabled
            && account.reset_retry_at <= timestamp
            && account.reset_credits.is_some_and(|count| count > 0)
            && self.codex_limited(account, timestamp)
    }

    /// Index of the account named `name`.
    pub fn index(&self, name: &str) -> Option<usize> {
        self.accounts
            .read()
            .unwrap()
            .iter()
            .position(|e| e.account.name == name)
    }

    /// True when some enabled account can take a request for `model` now.
    pub fn has_eligible(&self, model: &str, group: Option<&str>, pinned: Option<usize>) -> bool {
        let entries = self.accounts.read().unwrap();
        let state = self.state.lock().unwrap();
        let timestamp = now();
        entries.iter().enumerate().any(|(idx, entry)| {
            self.eligible(
                &entry.account,
                idx,
                &state.accounts[idx],
                model,
                group,
                pinned,
                timestamp,
            )
        })
    }

    /// Selection rule shared by `select` and `has_eligible`: the account
    /// matches the request and no relevant quota window, hold, or model
    /// restriction blocks it.
    #[allow(clippy::too_many_arguments)]
    fn eligible(
        &self,
        config: &Account,
        idx: usize,
        account: &AccountState,
        model: &str,
        group: Option<&str>,
        pinned: Option<usize>,
        timestamp: u64,
    ) -> bool {
        let limits = self.config.model_limits.get(model);
        let relevant = |key: &str| {
            key == "codex-primary"
                || key == "codex-secondary"
                || key == format!("api:{model}:requests")
                || key == format!("api:{model}:tokens")
                || key == format!("api:{model}:project-tokens")
                || limits.is_some_and(|ids| {
                    ids.iter().any(|id| {
                        key == format!("{}-primary", quota::normalize(id))
                            || key == format!("{}-secondary", quota::normalize(id))
                    })
                })
        };
        let limited = account
            .quotas
            .iter()
            .any(|(key, w)| relevant(key) && w.used(timestamp) >= account.threshold_percent);
        !account.disabled
            && account.hold_until <= timestamp
            && !limited
            && account
                .unavailable_models
                .get(model)
                .is_none_or(|until| *until <= timestamp)
            && self.matches(config, idx, model, group, pinned)
    }

    /// The limited account that an automatic reset should recover for this
    /// request: it opted in, matches the request, and holds a credit. The
    /// cooldown is checked under the reset lock, so a request that arrives
    /// during a redeem waits for it instead of failing. Ties prefer the
    /// higher-priority account.
    pub fn auto_reset_candidate(
        &self,
        model: &str,
        group: Option<&str>,
        pinned: Option<usize>,
    ) -> Option<usize> {
        let entries = self.accounts.read().unwrap();
        let state = self.state.lock().unwrap();
        let timestamp = now();
        entries
            .iter()
            .enumerate()
            .filter(|(idx, entry)| {
                let account = &state.accounts[*idx];
                entry.account.auto_reset
                    && self.matches(&entry.account, *idx, model, group, pinned)
                    && !account.disabled
                    && account.hold_until <= timestamp
                    && account.reset_credits.is_some_and(|count| count > 0)
                    && self.codex_limited(account, timestamp)
            })
            .min_by_key(|(idx, entry)| (entry.account.priority, *idx))
            .map(|(idx, _)| idx)
    }

    fn matches(
        &self,
        config: &Account,
        idx: usize,
        model: &str,
        group: Option<&str>,
        pinned: Option<usize>,
    ) -> bool {
        pinned.is_none_or(|pin| pin == idx)
            && (model.is_empty()
                || config.models.is_empty()
                || config.models.iter().any(|m| m == model))
            && match group {
                Some(g) => config.groups.iter().any(|s| s == g),
                None => config.groups.is_empty(),
            }
    }

    /// A reset credit only clears the Codex windows, so only those decide
    /// whether a redeem can help.
    fn codex_limited(&self, account: &AccountState, timestamp: u64) -> bool {
        account.quotas.iter().any(|(id, w)| {
            matches!(id.as_str(), "codex-primary" | "codex-secondary")
                && w.used(timestamp) >= account.threshold_percent
        })
    }

    pub fn mark_model_unavailable(&self, idx: usize, model: &str) {
        if model.is_empty() || model.len() > 256 {
            return;
        }
        let mut state = self.state.lock().unwrap();
        let models = &mut state.accounts[idx].unavailable_models;
        let at = now();
        models.retain(|_, until| *until > at);
        if models.len() >= MODEL_CACHE_LIMIT
            && !models.contains_key(model)
            && let Some(oldest) = models
                .iter()
                .min_by_key(|(_, until)| *until)
                .map(|(name, _)| name.clone())
        {
            models.remove(&oldest);
        }
        models.insert(model.to_owned(), at + MODEL_RETRY_SECONDS);
    }

    /// Ignore quota and account holds: distinguish model access from capacity.
    pub fn model_unavailable(
        &self,
        model: &str,
        group: Option<&str>,
        pinned: Option<usize>,
    ) -> bool {
        if model.is_empty() {
            return false;
        }
        let entries = self.accounts.read().unwrap();
        let state = self.state.lock().unwrap();
        let at = now();
        let mut matched = false;
        for (idx, account) in entries.iter().map(|e| &e.account).enumerate() {
            if pinned.is_some_and(|pin| pin != idx)
                || !match group {
                    Some(g) => account.groups.iter().any(|name| name == g),
                    None => account.groups.is_empty(),
                }
            {
                continue;
            }
            matched = true;
            if (account.models.is_empty() || account.models.iter().any(|name| name == model))
                && state.accounts[idx]
                    .unavailable_models
                    .get(model)
                    .is_none_or(|until| *until <= at)
            {
                return false;
            }
        }
        matched
    }

    pub fn hold(&self, idx: usize, until: u64, reason: &str) {
        let mut state = self.state.lock().unwrap();
        let account = &mut state.accounts[idx];
        account.hold_until = account.hold_until.max(until);
        account.last_error = Some(reason.to_owned());
        account.errors += 1;
    }

    pub fn defer(&self, idx: usize, until: u64) {
        let mut state = self.state.lock().unwrap();
        let account = &mut state.accounts[idx];
        account.hold_until = account.hold_until.max(until);
        account.last_error = Some("rate_limited".to_owned());
    }

    pub fn record(&self, idx: usize, status: u16, outcome: &str, response: Option<&Value>) {
        self.record_model(idx, status, outcome, response, "");
    }

    pub fn record_model(
        &self,
        idx: usize,
        status: u16,
        outcome: &str,
        response: Option<&Value>,
        model: &str,
    ) {
        let entries = self.accounts.read().unwrap();
        let mut state = self.state.lock().unwrap();
        let at = now();
        if let Some(id) = response.and_then(|r| r.get("id")).and_then(Value::as_str) {
            // A completed upstream response cannot be undone. A storage failure
            // marks routing unhealthy and blocks later requests until recovery.
            let _ = state.routing.remember(
                RouteKind::Response,
                affinity::hash(id.as_bytes()),
                entries[idx].binding.clone(),
            );
        }
        drop(entries);
        let account = &mut state.accounts[idx];
        account.requests += 1;
        if status >= 400 || outcome != "complete" {
            account.errors += 1;
        }
        if let Some(usage) = response
            .and_then(|r| r.get("usage"))
            .filter(|v| v.is_object())
        {
            if let Some(price) = self.config.prices.get(model) {
                let input = usage
                    .get("input_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let cached = usage
                    .pointer("/input_tokens_details/cached_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    .min(input);
                let output = usage
                    .get("output_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                account.estimated_cost_usd += ((input - cached) as f64 * price.input_per_million
                    + cached as f64 * price.cached_input_per_million
                    + output as f64 * price.output_per_million)
                    / 1_000_000.0;
            } else {
                account.unpriced_requests += 1;
            }
            account.input_tokens += usage
                .get("input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            account.output_tokens += usage
                .get("output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            account.cached_tokens += usage
                .pointer("/input_tokens_details/cached_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
        } else {
            account.unmetered_requests += 1;
            account.unpriced_requests += 1;
        }
        if outcome == "complete" {
            account.last_error = None;
        } else {
            account.last_error = Some(outcome.to_owned());
        }
        let name = account.name.clone();
        if state.recent.len() == 100 {
            state.recent.pop_front();
        }
        state.recent.push_back(RequestLog {
            at,
            account: name,
            status,
            outcome: outcome.to_owned(),
        });
    }

    /// Earliest future time, in Unix seconds, when a hold, model restriction,
    /// or exhausted quota window clears on any account.
    pub fn reset_at(&self) -> Option<u64> {
        let state = self.state.lock().unwrap();
        let timestamp = now();
        state
            .accounts
            .iter()
            .flat_map(|a| {
                std::iter::once(a.hold_until)
                    .chain(a.unavailable_models.values().copied())
                    .chain(
                        a.quotas
                            .values()
                            .filter(|w| w.used(timestamp) >= a.threshold_percent)
                            .filter_map(|w| w.reset_at),
                    )
            })
            .filter(|at| *at > timestamp)
            .min()
    }

    fn limited(&self, account: &AccountState, timestamp: u64) -> bool {
        account
            .quotas
            .values()
            .any(|w| w.used(timestamp) >= account.threshold_percent)
    }

    /// The earliest active hold on an enabled account that has quota left, with
    /// the reason that set it. A hold is the proxy waiting out a transient
    /// failure; it carries no upstream reset time.
    pub fn active_hold(&self) -> Option<(u64, String)> {
        let state = self.state.lock().unwrap();
        let timestamp = now();
        state
            .accounts
            .iter()
            .filter(|a| !a.disabled && a.hold_until > timestamp && !self.limited(a, timestamp))
            .map(|a| {
                (
                    a.hold_until,
                    a.last_error.clone().unwrap_or_else(|| "hold".to_owned()),
                )
            })
            .min_by_key(|(until, _)| *until)
    }

    /// The earliest reset of a quota window that blocks an enabled account.
    /// This is the only time the pool reports as a usage-limit reset.
    pub fn quota_reset_at(&self) -> Option<u64> {
        let state = self.state.lock().unwrap();
        let timestamp = now();
        state
            .accounts
            .iter()
            .filter(|a| !a.disabled)
            .flat_map(|a| {
                a.quotas
                    .values()
                    .filter(|w| w.used(timestamp) >= a.threshold_percent)
                    .filter_map(|w| w.reset_at)
            })
            .filter(|at| *at > timestamp)
            .min()
    }

    pub fn retry_seconds(&self) -> u64 {
        self.reset_at()
            .map(|at| at.saturating_sub(now()))
            .unwrap_or(30)
            .max(1)
    }
}

pub struct Lease {
    pub pool: Arc<Pool>,
    pub idx: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.pool.state.lock().unwrap();
        state.accounts[self.idx].in_flight -= 1;
    }
}
