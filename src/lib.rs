pub mod affinity;
pub mod auth;
pub mod config;
pub mod login;
pub mod models;
pub mod oauth;
pub mod pool;
pub mod proxy;
pub mod quota;
pub mod sse;
pub mod status;
pub mod storage;
pub mod tui;

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
