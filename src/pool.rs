use crate::{
    affinity::{self, Kind as RouteKind, Routing},
    auth::Auth,
    config::Config,
    now,
    quota::{self, Quotas},
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

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

pub struct Pool {
    pub config: Config,
    pub auth: Vec<Auth>,
    pub state: Mutex<State>,
    pub client: reqwest::Client,
    bindings: Vec<String>,
}

impl Pool {
    pub fn new(config: Config) -> anyhow::Result<Arc<Self>> {
        Self::with_routing(config, Routing::default())
    }

    pub fn persistent(config: Config, path: &std::path::Path) -> anyhow::Result<Arc<Self>> {
        Self::with_routing(config, Routing::open(path)?)
    }

    fn with_routing(config: Config, routing: Routing) -> anyhow::Result<Arc<Self>> {
        config.validate()?;
        let bindings = config
            .accounts
            .iter()
            .map(|a| {
                serde_json::to_vec(&(
                    &a.name,
                    a.kind,
                    a.base(),
                    &a.account_id,
                    &a.user_id,
                    &a.credential,
                ))
                .map(|bytes| affinity::hash(&bytes))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let accounts = config
            .accounts
            .iter()
            .map(|a| AccountState {
                name: a.name.clone(),
                disabled: a.disabled,
                ..Default::default()
            })
            .collect();
        let auth = config.accounts.iter().map(|_| Auth::default()).collect();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(10))
            .no_proxy()
            .build()?;
        Ok(Arc::new(Self {
            config,
            bindings,
            auth,
            client,
            state: Mutex::new(State {
                accounts,
                routing,
                sequence: 0,
                recent: VecDeque::new(),
            }),
        }))
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
        let state = self.state.lock().unwrap();
        let binding = state
            .routing
            .get(RouteKind::Response, &affinity::hash(id.as_bytes()))?;
        self.bindings
            .iter()
            .position(|candidate| candidate == binding)
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
        let mut state = self.state.lock().unwrap();
        if !state.routing.healthy() {
            return None;
        }
        let session =
            session.map(|key| affinity::hash(&serde_json::to_vec(&(model, group, key)).unwrap()));
        let timestamp = now();
        let limits = self.config.model_limits.get(model);
        let candidates: Vec<usize> = state
            .accounts
            .iter()
            .enumerate()
            .filter_map(|(idx, account)| {
                let config = &self.config.accounts[idx];
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
                let limited = account.quotas.iter().any(|(key, w)| {
                    relevant(key) && w.used(timestamp) >= self.config.threshold_percent
                });
                let eligible = !account.disabled
                    && account.hold_until <= timestamp
                    && !limited
                    && account
                        .unavailable_models
                        .get(model)
                        .is_none_or(|until| *until <= timestamp)
                    && !tried.contains(&idx)
                    && pinned.is_none_or(|pin| pin == idx)
                    && (model.is_empty()
                        || config.models.is_empty()
                        || config.models.iter().any(|m| m == model))
                    && match group {
                        Some(g) => config.groups.iter().any(|s| s == g),
                        None => config.groups.is_empty(),
                    };
                eligible.then_some(idx)
            })
            .collect();
        // An eligible established session stays on its account, including after
        // a higher-priority account recovers. Priority selects new sessions.
        let affinity = session
            .as_deref()
            .and_then(|key| state.routing.get(RouteKind::Session, key))
            .and_then(|binding| {
                self.bindings
                    .iter()
                    .position(|candidate| candidate == binding)
            })
            .filter(|idx| candidates.contains(idx));
        let priority = candidates
            .iter()
            .map(|&idx| self.config.accounts[idx].priority)
            .min()?;
        let candidates: Vec<_> = candidates
            .into_iter()
            .filter(|&idx| self.config.accounts[idx].priority == priority)
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
                .remember(RouteKind::Session, key, self.bindings[idx].clone())
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
        let state = self.state.lock().unwrap();
        let at = now();
        let mut matched = false;
        for (idx, account) in self.config.accounts.iter().enumerate() {
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
        state.accounts[idx].hold_until = state.accounts[idx].hold_until.max(until);
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
        let mut state = self.state.lock().unwrap();
        let at = now();
        if let Some(id) = response.and_then(|r| r.get("id")).and_then(Value::as_str) {
            // A completed upstream response cannot be undone. A storage failure
            // marks routing unhealthy and blocks later requests until recovery.
            let _ = state.routing.remember(
                RouteKind::Response,
                affinity::hash(id.as_bytes()),
                self.bindings[idx].clone(),
            );
        }
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

    pub fn retry_seconds(&self) -> u64 {
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
                            .filter(|w| w.used(timestamp) >= self.config.threshold_percent)
                            .filter_map(|w| w.reset_at),
                    )
            })
            .filter(|at| *at > timestamp)
            .min()
            .map(|at| at - timestamp)
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
