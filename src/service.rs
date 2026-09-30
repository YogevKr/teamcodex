//! The headless server as a macOS LaunchAgent: its plist, its own log file,
//! and a warning when macOS throttles it as a background task.
use crate::{now, storage};
use anyhow::{Context, Result};
use serde_json::json;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};

pub const LABEL: &str = "com.yogevkr.teamcodex";
const SEARCH_PATH: &str = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin";
/// XNU `MAXPRI_THROTTLE`: the base priority of a darwin-background task.
#[cfg(target_os = "macos")]
const THROTTLED_PRIORITY: i32 = 4;

pub fn default_log_file() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join("Library/Logs/teamcodex/server.log"))
}

/// The upgrade-stable path of a Homebrew install:
/// `<prefix>/Cellar/<formula>/<version>/bin/tcx` becomes `<prefix>/opt/<formula>/bin/tcx`.
pub fn stable_program(program: &Path) -> PathBuf {
    let parts: Vec<_> = program.components().collect();
    let Some(cellar) = parts.iter().position(|part| part.as_os_str() == "Cellar") else {
        return program.to_path_buf();
    };
    if parts.len() < cellar + 4 {
        return program.to_path_buf();
    }
    let mut stable: PathBuf = parts[..cellar].iter().collect();
    stable.push("opt");
    stable.push(parts[cellar + 1]);
    stable.extend(&parts[cellar + 3..]);
    stable
}

/// A LaunchAgent that runs the headless server at login.
///
/// It sets no `ProcessType`. `Background` clamps every server thread to
/// priority 4, so under heavy CPU load the proxy cannot answer Codex or
/// `tcx run` for seconds. The server writes its own log file, so launchd
/// needs no log paths and a deleted log directory comes back.
pub fn launch_agent(program: &Path, config: &Path, log_file: &Path, home: &Path) -> String {
    let arguments: String = [
        program.to_string_lossy(),
        "--config".into(),
        config.to_string_lossy(),
        "server".into(),
        "--headless".into(),
        "--log-file".into(),
        log_file.to_string_lossy(),
    ]
    .iter()
    .map(|argument| format!("\t\t<string>{}</string>\n", escape(argument)))
    .collect();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<!-- No ProcessType: Background clamps the proxy to priority 4 and starves Codex under load. -->
<dict>
	<key>Label</key>
	<string>{LABEL}</string>
	<key>ProgramArguments</key>
	<array>
{arguments}	</array>
	<key>EnvironmentVariables</key>
	<dict>
		<key>PATH</key>
		<string>{SEARCH_PATH}</string>
	</dict>
	<key>WorkingDirectory</key>
	<string>{home}</string>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>ThrottleInterval</key>
	<integer>10</integer>
	<key>ExitTimeOut</key>
	<integer>30</integer>
</dict>
</plist>
"#,
        home = escape(&home.to_string_lossy()),
    )
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn open_log(path: &Path) -> Result<File> {
    storage::create_parent(path)?;
    let mut options = OpenOptions::new();
    options.append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).context("cannot open TeamCodex log file")
}

/// Send stdout and stderr to `path` in append mode.
#[cfg(unix)]
pub fn redirect_output(path: &Path) -> Result<()> {
    attach(path, &[libc::STDOUT_FILENO, libc::STDERR_FILENO])
}

#[cfg(not(unix))]
pub fn redirect_output(_path: &Path) -> Result<()> {
    anyhow::bail!("--log-file requires a Unix system")
}

/// Reopen the log file when it is deleted or replaced, for example when a
/// cleanup tool removes its directory or a rotation renames it.
pub async fn log_file_loop(path: PathBuf, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        // A failed reopen leaves output on the old file; the next tick tries again.
        #[cfg(unix)]
        if let Ok(true) = reattach(&path, &[libc::STDOUT_FILENO, libc::STDERR_FILENO]) {
            eprintln!("{}", json!({"at": now(), "event": "log_reopened"}));
        }
    }
}

#[cfg(unix)]
fn attach(path: &Path, fds: &[std::os::fd::RawFd]) -> Result<()> {
    use std::os::fd::AsRawFd;
    let file = open_log(path)?;
    for &fd in fds {
        // SAFETY: dup2 only replaces the descriptor number `fd` with a copy of an open file.
        if unsafe { libc::dup2(file.as_raw_fd(), fd) } < 0 {
            return Err(std::io::Error::last_os_error()).context("cannot redirect output");
        }
    }
    Ok(())
}

/// Attach `fds` to `path` again unless they already write to it. True when reopened.
#[cfg(unix)]
fn reattach(path: &Path, fds: &[std::os::fd::RawFd]) -> Result<bool> {
    if fds.iter().all(|&fd| writes_to(path, fd)) {
        return Ok(false);
    }
    attach(path, fds)?;
    Ok(true)
}

#[cfg(unix)]
fn writes_to(path: &Path, fd: std::os::fd::RawFd) -> bool {
    use std::os::{fd::BorrowedFd, unix::fs::MetadataExt};
    let Ok(expected) = std::fs::metadata(path) else {
        return false;
    };
    // SAFETY: `fd` stays open for this call; the clone owns its own descriptor.
    let Ok(open) = unsafe { BorrowedFd::borrow_raw(fd) }.try_clone_to_owned() else {
        return false;
    };
    File::from(open)
        .metadata()
        .is_ok_and(|actual| actual.dev() == expected.dev() && actual.ino() == expected.ino())
}

/// This process's base priority when macOS runs it as a background task: a
/// LaunchAgent with `ProcessType` `Background`, or `taskpolicy -b`.
#[cfg(target_os = "macos")]
pub fn background_priority() -> Option<i32> {
    let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::zeroed();
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    // SAFETY: the buffer holds one proc_taskinfo, the PROC_PIDTASKINFO result type.
    let read = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if read != size {
        return None;
    }
    // SAFETY: proc_pidinfo filled all `size` bytes.
    let priority = unsafe { info.assume_init() }.pti_priority;
    (priority <= THROTTLED_PRIORITY).then_some(priority)
}

#[cfg(not(target_os = "macos"))]
pub fn background_priority() -> Option<i32> {
    None
}

pub fn warn_if_background() {
    if let Some(priority) = background_priority() {
        eprintln!(
            "{}",
            json!({
                "at": now(), "event": "background_priority", "priority": priority,
                "hint": "macOS throttles this server; remove ProcessType Background from its LaunchAgent (see tcx launch-agent)",
            })
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn homebrew_cellar_programs_use_the_opt_link() {
        assert_eq!(
            stable_program(Path::new("/opt/homebrew/Cellar/teamcodex/0.3.16/bin/tcx")),
            Path::new("/opt/homebrew/opt/teamcodex/bin/tcx")
        );
        for other in ["/opt/homebrew/bin/tcx", "/Users/me/src/target/release/tcx"] {
            assert_eq!(stable_program(Path::new(other)), Path::new(other));
        }
        assert_eq!(
            stable_program(Path::new("/x/Cellar/teamcodex/0.3.16")),
            Path::new("/x/Cellar/teamcodex/0.3.16")
        );
    }

    #[test]
    fn launch_agent_runs_the_headless_server_without_background_priority() {
        let plist = launch_agent(
            Path::new("/opt/homebrew/opt/teamcodex/bin/tcx"),
            Path::new("/Users/a&b/config.json"),
            Path::new("/Users/a&b/Library/Logs/teamcodex/server.log"),
            Path::new("/Users/a&b"),
        );
        assert!(plist.contains("<string>com.yogevkr.teamcodex</string>"));
        assert!(plist.contains(
            "\t\t<string>/opt/homebrew/opt/teamcodex/bin/tcx</string>\n\
             \t\t<string>--config</string>\n\
             \t\t<string>/Users/a&amp;b/config.json</string>\n\
             \t\t<string>server</string>\n\
             \t\t<string>--headless</string>\n\
             \t\t<string>--log-file</string>\n\
             \t\t<string>/Users/a&amp;b/Library/Logs/teamcodex/server.log</string>\n\t</array>"
        ));
        assert!(!plist.contains("<key>ProcessType</key>"));
        assert!(!plist.contains("StandardErrorPath"));
        assert!(plist.contains("<string>/Users/a&amp;b</string>"));
    }

    #[cfg(unix)]
    #[test]
    fn log_file_comes_back_after_its_directory_is_deleted() {
        use std::{io::Write, os::fd::AsRawFd};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs/server.log");
        let mut target = File::create(dir.path().join("fd")).unwrap();
        let fd = target.as_raw_fd();
        assert!(!writes_to(&path, fd));

        assert!(reattach(&path, &[fd]).unwrap());
        assert!(writes_to(&path, fd));
        assert!(!reattach(&path, &[fd]).unwrap());
        target.write_all(b"first\n").unwrap();

        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
        assert!(!writes_to(&path, fd));
        assert!(reattach(&path, &[fd]).unwrap());
        target.write_all(b"second\n").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "second\n");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn detects_darwin_background_priority() {
        let set =
            |value: libc::c_int| unsafe { libc::setpriority(libc::PRIO_DARWIN_PROCESS, 0, value) };
        assert_eq!(background_priority(), None);
        assert_eq!(set(libc::PRIO_DARWIN_BG), 0);
        let throttled = background_priority();
        assert_eq!(set(0), 0);
        assert_eq!(throttled, Some(THROTTLED_PRIORITY));
        assert_eq!(background_priority(), None);
    }
}
