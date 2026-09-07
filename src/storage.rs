//! Private, atomic files and process-wide file locks for managed credentials.
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    time::{Duration, Instant},
};

pub fn random_string() -> Result<String> {
    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow::anyhow!("secure random generation failed"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options
}

pub fn create_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("file needs a parent directory")?;
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(parent)
        .context("cannot create TeamCodex directory")?;
    Ok(())
}

fn check_file(file: &File) -> Result<()> {
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "expected a regular private file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "managed credential file must have owner-only permissions (0600)"
        );
    }
    Ok(())
}

pub fn read_private(path: &Path) -> Result<Vec<u8>> {
    let file = options()
        .read(true)
        .open(path)
        .context("cannot open managed credential file")?;
    check_file(&file)?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 1024 * 1024,
        "managed credential file exceeds size limit"
    );
    Ok(bytes)
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    create_parent(path)?;
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "refusing to replace a non-regular file"
        );
    }
    let temp = path.with_extension(format!("tmp-{}", random_string()?));
    let result = (|| -> Result<()> {
        let mut file = options().write(true).create_new(true).open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(path.parent().unwrap())?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.context("cannot save TeamCodex file")
}

pub async fn lock(path: &Path) -> Result<File> {
    create_parent(path)?;
    let file = options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    check_file(&file)?;
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                bail!("another TeamCodex process is updating this account; retry shortly")
            }
            Err(error) => return Err(anyhow::anyhow!(error)),
        }
    }
}

pub async fn local_token(path: &Path) -> Result<String> {
    let _lock = lock(&path.with_extension("lock")).await?;
    if !path.try_exists()? {
        atomic_write(path, random_string()?.as_bytes())?;
    }
    let token = String::from_utf8(read_private(path)?).context("invalid local proxy token")?;
    ensure!(
        token.len() >= 16 && token.bytes().all(|b| b.is_ascii_graphic()),
        "invalid local proxy token"
    );
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_local_token_creation_is_stable_and_private() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("private/proxy.token");
        let results = futures_util::future::join_all((0..8).map(|_| local_token(&path))).await;
        let first = results[0].as_ref().unwrap();
        assert!(results.iter().all(|value| value.as_ref().unwrap() == first));
        assert_eq!(first.len(), 43);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(path.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn private_reads_and_writes_reject_symlinks_and_public_permissions() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target.json");
        let link = temp.path().join("link.json");
        atomic_write(&target, b"synthetic-private-value").unwrap();
        symlink(&target, &link).unwrap();
        assert!(read_private(&link).is_err());
        assert!(atomic_write(&link, b"replacement").is_err());
        assert_eq!(read_private(&target).unwrap(), b"synthetic-private-value");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_private(&target).is_err());
        atomic_write(&target, b"replacement").unwrap();
        assert_eq!(read_private(&target).unwrap(), b"replacement");
        assert_eq!(
            fs::metadata(target).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
