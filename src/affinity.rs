//! Durable account routing only. Keys and account bindings are hashed; no prompts or tokens are stored.
use crate::{now, storage};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

const LIMIT: usize = 10_000;
const TTL: u64 = 24 * 3600;
const COMPACT_BYTES: u64 = 4 * 1024 * 1024;

pub fn hash(value: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(digest(&SHA256, value).as_ref())
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Session,
    Response,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Record {
    kind: Kind,
    key: String,
    account: String,
    at: u64,
}

struct Journal {
    path: PathBuf,
    file: File,
    _lock: File,
}

#[derive(Default)]
pub struct Routing {
    sessions: HashMap<String, Record>,
    responses: HashMap<String, Record>,
    journal: Option<Journal>,
    failed: bool,
}

impl Routing {
    pub fn open(path: &Path) -> Result<Self> {
        let lock = storage::open_private_append(&path.with_extension("lock"))?;
        lock.try_lock()
            .map_err(|_| anyhow::anyhow!("another server owns routing state"))?;
        let file = storage::open_private_append(path)?;
        ensure!(
            file.metadata()?.len() <= COMPACT_BYTES * 2,
            "routing journal exceeds size limit"
        );
        let mut bytes = Vec::new();
        (&file)
            .take(COMPACT_BYTES * 2 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= COMPACT_BYTES * 2,
            "routing journal exceeds size limit"
        );
        // A process can exit during the last append. Completed records must parse.
        let end = bytes
            .iter()
            .rposition(|b| *b == b'\n')
            .map_or(0, |idx| idx + 1);
        let mut routing = Self::default();
        for line in bytes[..end]
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
        {
            let record: Record = serde_json::from_slice(line).context("invalid routing journal")?;
            ensure!(
                [&record.key, &record.account].iter().all(|s| s.len() == 43
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))),
                "invalid routing binding"
            );
            if record.at <= now() && now() < record.at.saturating_add(TTL) {
                routing.insert(record);
            }
        }
        if end < bytes.len() {
            file.set_len(end as u64)?;
            file.sync_data()?;
        }
        routing.journal = Some(Journal {
            path: path.to_owned(),
            file,
            _lock: lock,
        });
        Ok(routing)
    }

    pub fn persistent(&self) -> bool {
        self.journal.is_some()
    }
    pub fn healthy(&self) -> bool {
        !self.failed
    }

    pub fn get(&self, kind: Kind, key: &str) -> Option<&str> {
        if self.failed {
            return None;
        }
        let map = match kind {
            Kind::Session => &self.sessions,
            Kind::Response => &self.responses,
        };
        map.get(key)
            .filter(|r| now() < r.at.saturating_add(TTL))
            .map(|r| r.account.as_str())
    }

    fn insert(&mut self, record: Record) {
        let map = match record.kind {
            Kind::Session => &mut self.sessions,
            Kind::Response => &mut self.responses,
        };
        if map.len() >= LIMIT && !map.contains_key(&record.key) {
            map.retain(|_, r| now() < r.at.saturating_add(TTL));
            if map.len() >= LIMIT
                && let Some(oldest) = map
                    .values()
                    .min_by(|a, b| (a.at, &a.key).cmp(&(b.at, &b.key)))
                    .map(|r| r.key.clone())
            {
                map.remove(&oldest);
            }
        }
        map.insert(record.key.clone(), record);
    }

    pub fn remember(&mut self, kind: Kind, key: String, account: String) -> Result<()> {
        ensure!(!self.failed, "routing storage unavailable");
        let at = now();
        let map = match kind {
            Kind::Session => &self.sessions,
            Kind::Response => &self.responses,
        };
        // Refresh an unchanged binding at most once per minute.
        if map
            .get(&key)
            .is_some_and(|r| r.account == account && at < r.at.saturating_add(60))
        {
            return Ok(());
        }
        let record = Record {
            kind,
            key,
            account,
            at,
        };
        let result = self.append(&record);
        if result.is_err() {
            self.failed = true;
        }
        result?;
        self.insert(record);
        Ok(())
    }

    fn append(&mut self, record: &Record) -> Result<()> {
        let Some(journal) = &mut self.journal else {
            return Ok(());
        };
        if journal.file.metadata()?.len() >= COMPACT_BYTES {
            let mut compact = Vec::new();
            for row in self
                .sessions
                .values()
                .chain(self.responses.values())
                .filter(|r| now() < r.at.saturating_add(TTL))
            {
                serde_json::to_writer(&mut compact, row)?;
                compact.push(b'\n');
            }
            storage::atomic_write(&journal.path, &compact)?;
            journal.file = storage::open_private_append(&journal.path)?;
        }
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        journal.file.write_all(&bytes)?;
        journal.file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_restores_bindings_and_recovers_only_an_incomplete_tail() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("routing.jsonl");
        let key = hash(b"synthetic-session");
        let account = hash(b"synthetic-account");
        let mut routing = Routing::open(&path).unwrap();
        routing
            .remember(Kind::Session, key.clone(), account.clone())
            .unwrap();
        routing
            .remember(Kind::Response, hash(b"response"), account.clone())
            .unwrap();
        assert!(Routing::open(&path).is_err(), "one writer per journal");
        drop(routing);
        storage::open_private_append(&path)
            .unwrap()
            .write_all(b"{\"incomplete\":")
            .unwrap();
        let routing = Routing::open(&path).unwrap();
        assert_eq!(routing.get(Kind::Session, &key), Some(account.as_str()));
        assert_eq!(
            routing.get(Kind::Response, &hash(b"response")),
            Some(account.as_str())
        );
        drop(routing);
        let saved = std::fs::read(&path).unwrap();
        assert!(
            !String::from_utf8(saved)
                .unwrap()
                .contains("synthetic-session")
        );
        storage::open_private_append(&path)
            .unwrap()
            .write_all(b"invalid complete record\n")
            .unwrap();
        assert!(Routing::open(&path).is_err());
    }

    #[test]
    fn expired_entries_and_compaction_obey_bounds() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("routing.jsonl");
        let mut routing = Routing::open(&path).unwrap();
        let account = hash(b"a");
        for idx in 0..LIMIT + 1 {
            routing.insert(Record {
                kind: Kind::Session,
                key: hash(idx.to_string().as_bytes()),
                account: account.clone(),
                at: now(),
            });
        }
        assert_eq!(routing.sessions.len(), LIMIT);
        let expired = hash(b"expired");
        routing.insert(Record {
            kind: Kind::Response,
            key: expired.clone(),
            account: account.clone(),
            at: now() - TTL,
        });
        assert!(routing.get(Kind::Response, &expired).is_none());
        // Force compaction without creating thousands of synchronous writes.
        routing
            .journal
            .as_ref()
            .unwrap()
            .file
            .set_len(COMPACT_BYTES)
            .unwrap();
        routing
            .remember(Kind::Response, hash(b"fresh"), account)
            .unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() < COMPACT_BYTES);
        drop(routing);
        let restored = Routing::open(&path).unwrap();
        assert_eq!(restored.sessions.len(), LIMIT);
        assert_eq!(restored.responses.len(), 1);
    }

    #[test]
    fn replay_preserves_eviction_with_tied_timestamps() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("routing.jsonl");
        let mut expected = Routing::default();
        let mut bytes = Vec::new();
        let at = now();
        for idx in 0..LIMIT + 17 {
            let record = Record {
                kind: Kind::Session,
                key: hash(idx.to_string().as_bytes()),
                account: hash(b"account"),
                at,
            };
            serde_json::to_writer(&mut bytes, &record).unwrap();
            bytes.push(b'\n');
            expected.insert(record);
        }
        storage::atomic_write(&path, &bytes).unwrap();
        let restored = Routing::open(&path).unwrap();
        assert_eq!(expected.sessions.len(), LIMIT);
        assert_eq!(
            expected
                .sessions
                .keys()
                .collect::<std::collections::BTreeSet<_>>(),
            restored
                .sessions
                .keys()
                .collect::<std::collections::BTreeSet<_>>()
        );
    }

    #[test]
    fn failed_writes_block_routing_and_unsafe_files_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("routing.jsonl");
        let mut routing = Routing::open(&path).unwrap();
        routing.journal.as_mut().unwrap().file = File::open(&path).unwrap();
        assert!(
            routing
                .remember(Kind::Session, hash(b"s"), hash(b"a"))
                .is_err()
        );
        assert!(!routing.healthy());
        assert!(routing.get(Kind::Session, &hash(b"s")).is_none());
        drop(routing);
        #[cfg(unix)]
        {
            use std::os::unix::fs::{PermissionsExt, symlink};
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(Routing::open(&path).is_err());
            let link = temp.path().join("link.jsonl");
            symlink(&path, &link).unwrap();
            assert!(Routing::open(&link).is_err());
        }
    }
}
