use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use std::{io::IsTerminal, path::PathBuf};
use teamcodex::{config::Config, pool::Pool, proxy, tui};

#[derive(Parser)]
#[command(
    name = "tcx",
    version,
    about = "A local account pool and streaming proxy for Codex"
)]
struct Cli {
    #[arg(long, global = true, default_value = "config.json")]
    config: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the proxy and terminal display.
    Server {
        #[arg(long)]
        headless: bool,
    },
    /// Check configuration without resolving credentials.
    Check,
    /// Read live account status as JSON.
    Status,
    /// Enable or disable an account until the server restarts.
    Account {
        name: String,
        #[arg(value_parser = ["enable", "disable"])]
        action: String,
    },
    /// Print a Codex provider configuration without changing existing files.
    CodexConfig,
    /// Run Codex through an already running proxy.
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
    if matches!(cli.command, Commands::Example) {
        print!("{}", include_str!("../examples/config.json"));
        return Ok(());
    }
    let config = Config::load(&cli.config)?;
    match cli.command {
        Commands::Check => println!("Configuration valid: {} accounts", config.accounts.len()),
        Commands::CodexConfig => print!("{}", provider_config(&config)),
        Commands::Server { headless } => {
            let token = config.client_token()?;
            let listener = tokio::net::TcpListener::bind(config.listen)
                .await
                .context("cannot bind proxy address")?;
            let pool = Pool::new(config)?;
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
            signal.abort();
            if let Some(ui) = ui {
                ui.await??;
            }
            result?;
        }
        Commands::Status | Commands::Account { .. } => {
            let client = reqwest::Client::builder().no_proxy().build()?;
            let url = format!("http://{}", config.listen);
            let request = match &cli.command {
                Commands::Account { name, action } => {
                    ensure!(
                        config.accounts.iter().any(|a| a.name == *name),
                        "unknown account"
                    );
                    client
                        .post(format!("{url}/accounts/{name}/enabled"))
                        .json(&serde_json::json!({"enabled":action == "enable"}))
                }
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
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Commands::Run { group, args } => {
            let token = config.client_token()?;
            let health = reqwest::Client::builder()
                .no_proxy()
                .build()?
                .get(format!("http://{}/health", config.listen))
                .bearer_auth(&token)
                .timeout(std::time::Duration::from_secs(5))
                .send()
                .await
                .context("start tcx server first")?;
            ensure!(health.status().is_success(), "proxy health check failed");
            let mut command = std::process::Command::new("codex");
            // Exec has its own override parser. Keep provider settings in that
            // parser so exec-local flags cannot discard the root overrides.
            let exec = args.first().is_some_and(|arg| arg == "exec" || arg == "e");
            if exec {
                command.arg("exec");
            }
            for setting in provider_settings(&config) {
                command.arg("-c").arg(setting);
            }
            if let Some(group) = group {
                ensure!(
                    config.accounts.iter().any(|a| a.groups.contains(&group)),
                    "unknown account group"
                );
                command.arg("-c").arg(format!(
                    "model_providers.teamcodex.http_headers.x-tcx-group={}",
                    serde_json::to_string(&group)?
                ));
            }
            command
                .args(if exec { &args[1..] } else { &args[..] })
                .env(&config.client_token_env, token);
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                return Err(command.exec().into());
            }
            #[cfg(not(unix))]
            {
                std::process::exit(command.status()?.code().unwrap_or(1));
            }
        }
        Commands::Example => unreachable!(),
    }
    Ok(())
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
        "model_providers.teamcodex.request_max_retries=0".into(),
        "model_providers.teamcodex.stream_max_retries=0".into(),
    ]
}

fn provider_config(config: &Config) -> String {
    provider_settings(config).join("\n") + "\n"
}
