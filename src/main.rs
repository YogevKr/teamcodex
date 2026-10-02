use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use std::{io::IsTerminal, path::PathBuf};
use teamcodex::{
    auth,
    config::{self, Config},
    login,
    pool::Pool,
    proxy, service, status, tui,
};

#[derive(Parser)]
#[command(
    name = "tcx",
    version,
    about = "A local account pool and streaming proxy for Codex"
)]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Add or renew a ChatGPT account through browser login.
    Login {
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        no_browser: bool,
    },
    /// List configured accounts without displaying credentials.
    Accounts,
    /// Start the proxy and terminal display.
    Server {
        #[arg(long)]
        headless: bool,
        /// Append stdout and stderr to this file, and reopen it when it is deleted.
        #[arg(long, requires = "headless")]
        log_file: Option<PathBuf>,
    },
    /// Check configuration without resolving credentials.
    Check,
    /// Show live account status: a table on a terminal, JSON otherwise.
    Status {
        /// Print the raw status JSON.
        #[arg(long, conflicts_with = "table")]
        json: bool,
        /// Print the account table even when stdout is not a terminal.
        #[arg(long)]
        table: bool,
    },
    /// Enable or disable an account until the server restarts.
    Account {
        name: String,
        #[arg(value_parser = ["enable", "disable"])]
        action: String,
    },
    /// Apply the configuration file's account list to the running server.
    Reload,
    /// Redeem one usage-limit reset credit on an account through the running server.
    Reset {
        name: String,
        /// Redeem this credit id. Default: the next available credit.
        #[arg(long)]
        credit: Option<String>,
        /// List the account's reset credits and stop.
        #[arg(long, conflicts_with_all = ["credit", "yes"])]
        list: bool,
        /// Redeem without a confirmation prompt. Required when stdin is not a terminal.
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Show an account's credit auto top-up setting through the running server.
    TopUp {
        name: String,
        /// Turn auto top-up off.
        #[arg(long)]
        disable: bool,
    },
    /// Print a Codex provider configuration without changing existing files.
    CodexConfig,
    /// Print a macOS LaunchAgent plist that runs the headless server at login.
    LaunchAgent {
        /// Server log file. Default: ~/Library/Logs/teamcodex/server.log.
        #[arg(long)]
        log_file: Option<PathBuf>,
    },
    /// Run Codex through the proxy, or directly when the proxy is stopped.
    Run {
        #[arg(long)]
        group: Option<String>,
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Print a configuration example. Credentials stay outside the file.
    Example,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut command = cli.command.unwrap_or(Commands::Server {
        headless: false,
        log_file: None,
    });
    // Redirect first, so configuration errors reach the log file too.
    if let Commands::Server {
        log_file: Some(file),
        ..
    } = &mut command
    {
        *file = std::path::absolute(&*file)?;
        service::redirect_output(file)?;
    }
    if matches!(command, Commands::Example) {
        print!("{}", include_str!("../examples/config.json"));
        return Ok(());
    }
    let path = std::path::absolute(cli.config.map(Ok).unwrap_or_else(config::default_path)?)?;
    if let Commands::Login { name, no_browser } = &command {
        return login::login(&path, name.as_deref(), *no_browser).await;
    }
    if let Commands::LaunchAgent { log_file } = &command {
        let log_file = match log_file {
            Some(file) => std::path::absolute(file)?,
            None => service::default_log_file()?,
        };
        let home = std::env::var_os("HOME").context("HOME is not set")?;
        print!(
            "{}",
            service::launch_agent(
                &service::stable_program(&std::env::current_exe()?),
                &path,
                &log_file,
                std::path::Path::new(&home),
            )
        );
        return Ok(());
    }
    let config = if path.try_exists()? {
        Config::load(&path)?
    } else {
        Config::local(&path)
    };
    match command {
        Commands::Accounts => login::accounts(&config)?,
        Commands::Check => println!("Configuration valid: {} accounts", config.accounts.len()),
        Commands::CodexConfig => print!("{}", provider_config(&config)),
        Commands::Server { headless, log_file } => {
            ensure!(
                !config.accounts.is_empty(),
                "no accounts configured; run tcx login first"
            );
            let token = config.prepare_client_token().await?;
            let listener = tokio::net::TcpListener::bind(config.listen)
                .await
                .context("cannot bind proxy address")?;
            service::warn_if_background();
            let pool =
                Pool::persistent(config, &path.with_extension("state").join("routing.jsonl"))?;
            pool.set_config_path(path.clone());
            let app = proxy::router(pool.clone(), token);
            let (stop, mut receiver) = tokio::sync::watch::channel(false);
            let signal_stop = stop.clone();
            let signal = tokio::spawn(async move {
                #[cfg(unix)]
                {
                    if let Ok(mut terminate) =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    {
                        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
                    } else {
                        let _ = tokio::signal::ctrl_c().await;
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = tokio::signal::ctrl_c().await;
                }
                let _ = signal_stop.send(true);
            });
            let probe = tokio::spawn(proxy::probe_loop(pool.clone()));
            let refresh = tokio::spawn(auth::refresh_loop(pool.clone()));
            let reload = tokio::spawn(proxy::reload_loop(
                pool.clone(),
                std::time::Duration::from_secs(2),
            ));
            let logs = log_file.map(|file| {
                tokio::spawn(service::log_file_loop(
                    file,
                    std::time::Duration::from_secs(2),
                ))
            });
            let ui = if !headless && std::io::stdout().is_terminal() {
                let ui_stop = stop.clone();
                Some(tokio::task::spawn_blocking(move || {
                    let result = tui::run(pool, ui_stop.clone());
                    let _ = ui_stop.send(true);
                    result
                }))
            } else {
                eprintln!("TeamCodex listening on {}", listener.local_addr()?);
                None
            };
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    while !*receiver.borrow() {
                        if receiver.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await;
            let _ = stop.send(true);
            probe.abort();
            refresh.abort();
            reload.abort();
            if let Some(logs) = logs {
                logs.abort();
            }
            signal.abort();
            if let Some(ui) = ui {
                ui.await??;
            }
            result?;
        }
        action @ (Commands::Status { .. } | Commands::Account { .. } | Commands::Reload) => {
            let client = reqwest::Client::builder().no_proxy().build()?;
            let url = format!("http://{}", config.listen);
            let request = match &action {
                Commands::Account { name, action } => {
                    ensure!(
                        config.accounts.iter().any(|a| a.name == *name),
                        "unknown account"
                    );
                    client
                        .post(format!("{url}/accounts/{name}/enabled"))
                        .json(&serde_json::json!({"enabled":action == "enable"}))
                }
                Commands::Reload => client.post(format!("{url}/reload")),
                _ => client.get(format!("{url}/status")),
            };
            let value: serde_json::Value = request
                .bearer_auth(config.client_token()?)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let as_table = match &action {
                Commands::Status { json, table } => {
                    !*json && (*table || std::io::stdout().is_terminal())
                }
                _ => false,
            };
            if as_table {
                print!(
                    "{}",
                    status::render(&value, config.threshold_percent, teamcodex::now())
                );
            } else {
                println!("{}", serde_json::to_string_pretty(&value)?);
            }
        }
        Commands::Reset {
            name,
            credit,
            list,
            yes,
        } => {
            return reset_account(&config, &name, credit.as_deref(), list, yes).await;
        }
        Commands::TopUp { name, disable } => {
            return top_up(&config, &name, disable).await;
        }
        Commands::Run { group, args } => {
            return run_codex(&config, group.as_deref(), args).await;
        }
        Commands::Example | Commands::Login { .. } | Commands::LaunchAgent { .. } => {
            unreachable!()
        }
    }
    Ok(())
}

/// Show an account's credit auto top-up setting, and turn it off on request.
async fn top_up(config: &Config, name: &str, disable: bool) -> Result<()> {
    ensure!(
        config.accounts.iter().any(|a| a.name == name),
        "unknown account"
    );
    let client = reqwest::Client::builder().no_proxy().build()?;
    let url = format!("http://{}/accounts/{name}/auto-top-up", config.listen);
    let request = if disable {
        client.post(format!("{url}/disable"))
    } else {
        client.get(url)
    };
    let response = request
        .bearer_auth(config.client_token()?)
        .timeout(std::time::Duration::from_secs(40))
        .send()
        .await
        .context("cannot reach the proxy; is tcx server running?")?;
    let status = response.status();
    let value: serde_json::Value = response.json().await?;
    ensure!(
        status.is_success(),
        "auto top-up request failed: {}",
        value["error"]["message"]
            .as_str()
            .unwrap_or("unknown error")
    );
    print!("{}", teamcodex::topup::render(name, &value));
    Ok(())
}

/// List an account's usage-limit reset credits and redeem one on request.
async fn reset_account(
    config: &Config,
    name: &str,
    credit: Option<&str>,
    list: bool,
    yes: bool,
) -> Result<()> {
    ensure!(
        config.accounts.iter().any(|a| a.name == name),
        "unknown account"
    );
    let client = reqwest::Client::builder().no_proxy().build()?;
    let url = format!("http://{}/accounts/{name}", config.listen);
    let token = config.client_token()?;
    let credits: serde_json::Value = client
        .get(format!("{url}/reset-credits"))
        .bearer_auth(&token)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .context("cannot reach the proxy; is tcx server running?")?
        .error_for_status()
        .context("the proxy could not list reset credits")?
        .json()
        .await?;
    let available = teamcodex::quota::reset_credits(&credits).unwrap_or(0);
    print!("{}", teamcodex::reset::render_credits(&credits));
    if list {
        return Ok(());
    }
    ensure!(available > 0, "no usage-limit reset credit is available");
    if !yes {
        ensure!(
            std::io::stdin().is_terminal(),
            "confirmation needs a terminal; pass --yes to redeem without a prompt"
        );
        eprint!("Redeem one usage-limit reset on {name}? [y/N] ");
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        if !matches!(answer.trim(), "y" | "Y" | "yes") {
            println!("Cancelled; no credit was redeemed.");
            return Ok(());
        }
    }
    let mut body = serde_json::json!({});
    if let Some(id) = credit {
        body["credit_id"] = serde_json::json!(id);
    }
    let response = client
        .post(format!("{url}/reset"))
        .bearer_auth(&token)
        .json(&body)
        .timeout(std::time::Duration::from_secs(40))
        .send()
        .await
        .context("cannot reach the proxy; is tcx server running?")?;
    let status = response.status();
    let value: serde_json::Value = response.json().await?;
    ensure!(
        status.is_success(),
        "reset failed: {}",
        value["error"]["message"]
            .as_str()
            .unwrap_or("unknown error")
    );
    print!(
        "{}",
        teamcodex::reset::render_outcome(&value, teamcodex::now())
    );
    Ok(())
}

async fn run_codex(config: &Config, group: Option<&str>, args: Vec<String>) -> Result<()> {
    if let Some(group) = group {
        ensure!(
            config
                .accounts
                .iter()
                .any(|account| account.groups.iter().any(|g| g == group)),
            "unknown account group"
        );
    }
    let available = match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio::net::TcpStream::connect(config.listen),
    )
    .await
    {
        Ok(Ok(_)) => true,
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => false,
        _ => anyhow::bail!("cannot determine proxy status; check tcx server"),
    };
    let mut command = std::process::Command::new("codex");
    if !available {
        ensure!(
            group.is_none(),
            "the requested account group requires a running proxy"
        );
        eprintln!("TeamCodex proxy is stopped; launching Codex directly.");
        command.args(args);
    } else {
        let token = config.client_token()?;
        let health = reqwest::Client::builder()
            .no_proxy()
            .build()?
            .get(format!("http://{}/health", config.listen))
            .bearer_auth(&token)
            .timeout(HEALTH_TIMEOUT)
            .send()
            .await;
        match health {
            // The kernel accepts connections for a server that gets no CPU time,
            // so a timeout here is almost always a starved TeamCodex. Codex retries
            // its own requests; launching through the proxy beats refusing.
            Err(error) if error.is_timeout() => eprintln!(
                "TeamCodex proxy did not answer its health check within {} s{}; launching Codex through it anyway.",
                HEALTH_TIMEOUT.as_secs(),
                load_note()
            ),
            health => {
                let health = health.context("proxy health check failed")?;
                ensure!(
                    health.status().is_success(),
                    "proxy health check failed; refusing direct fallback while the port is in use"
                );
                let health: serde_json::Value = health
                    .json()
                    .await
                    .context("invalid proxy health response")?;
                ensure!(
                    health["status"] == "ok" && health.get("version").is_some(),
                    "unexpected service on the proxy port"
                );
            }
        }
        command
            .args(proxied_args(config, group, args)?)
            .env(&config.client_token_env, token);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        Err(command.exec()).context("cannot launch Codex; install Codex CLI first")
    }
    #[cfg(not(unix))]
    {
        std::process::exit(command.status()?.code().unwrap_or(1));
    }
}

const HEALTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// " (load average N)" when the system reports one.
fn load_note() -> String {
    #[cfg(unix)]
    {
        let mut load = [0f64; 1];
        // SAFETY: getloadavg writes at most one sample into a one-element buffer.
        if unsafe { libc::getloadavg(load.as_mut_ptr(), 1) } == 1 {
            return format!(" (load average {:.0})", load[0]);
        }
    }
    String::new()
}

fn proxied_args(
    config: &Config,
    group: Option<&str>,
    mut args: Vec<String>,
) -> Result<Vec<String>> {
    // Exec has a separate override parser. The YOLO alias puts its flag before
    // exec; move that flag into exec too, so both sandbox and provider survive.
    let prefix = args
        .iter()
        .take_while(|arg| {
            matches!(
                arg.as_str(),
                "--yolo" | "--dangerously-bypass-approvals-and-sandbox"
            )
        })
        .count();
    let exec = args
        .get(prefix)
        .is_some_and(|arg| arg == "exec" || arg == "e");
    let mut output = Vec::new();
    if exec {
        args.remove(prefix);
        output.push("exec".to_owned());
    }
    let mut settings = provider_settings(config);
    if let Some(group) = group {
        settings.push(format!(
            "model_providers.teamcodex.http_headers.x-tcx-group={}",
            serde_json::to_string(group)?
        ));
    }
    for setting in settings {
        output.extend(["-c".to_owned(), setting]);
    }
    output.extend(args);
    Ok(output)
}

fn provider_settings(config: &Config) -> Vec<String> {
    vec![
        "model_provider=\"teamcodex\"".into(),
        "model_providers.teamcodex.name=\"TeamCodex\"".into(),
        format!(
            "model_providers.teamcodex.base_url=\"http://{}/v1\"",
            config.listen
        ),
        format!(
            "model_providers.teamcodex.env_key={}",
            serde_json::to_string(&config.client_token_env).unwrap()
        ),
        "model_providers.teamcodex.wire_api=\"responses\"".into(),
        "model_providers.teamcodex.requires_openai_auth=false".into(),
        "model_providers.teamcodex.supports_websockets=false".into(),
    ]
}

fn provider_config(config: &Config) -> String {
    provider_settings(config).join("\n") + "\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yolo_exec_keeps_provider_and_yolo_in_exec_parser() {
        let config = Config::local(std::path::Path::new("/tmp/tcx-test/config.json"));
        let args = proxied_args(
            &config,
            None,
            vec!["--yolo".into(), "exec".into(), "hello".into()],
        )
        .unwrap();
        assert_eq!(args[0], "exec");
        assert!(
            args.iter()
                .any(|value| value == "model_provider=\"teamcodex\"")
        );
        assert_eq!(&args[args.len() - 2..], ["--yolo", "hello"]);
        let interactive = proxied_args(&config, None, vec!["--yolo".into()]).unwrap();
        assert_eq!(interactive.last().unwrap(), "--yolo");
        assert!(!interactive.iter().any(|value| value == "exec"));
    }
}
