# TeamCodex

TeamCodex is a local account pool and streaming proxy for the Codex CLI.
It selects an eligible account for each request and keeps sessions on that account.

TeamCodex tracks quota windows, usage, account health, model access, and session affinity.
It forwards Responses API streams and turns temporary capacity errors into retryable responses.

The project is written in Rust and uses the MIT license.

## Install

### Homebrew

```sh
brew install yogevkr/tap/teamcodex
tcx --version
```

### Build from source

Requirements:

- Rust 1.89 or newer
- Python 3
- Codex CLI

```sh
cargo build --release --locked
./target/release/tcx --version
```

## Quick start

Log in to each ChatGPT account that TeamCodex should use:

```sh
tcx login --name personal
tcx login --name work
tcx accounts
```

`tcx login` opens the browser, creates the configuration, and stores OAuth tokens in private state files.
It does not change the active Codex login.

Start the proxy:

```sh
tcx server
```

The terminal display shows account status.
Use `tcx server --headless` when you do not want the display.

In another terminal, run Codex through the pool:

```sh
tcx run -- --yolo
```

`tcx run` checks the proxy before launch.
If the proxy is stopped, it launches Codex with its normal configuration.
If the proxy accepts the connection but does not answer within 5 seconds, `tcx run` prints a warning and launches Codex through the proxy.
This happens when heavy CPU load starves the server.
A command with `--group` requires a running proxy.

The default configuration path is `~/.config/teamcodex/config.json`.
Use `--config /absolute/path/config.json` to select another file.

Useful aliases:

```sh
alias tcxs='tcx server'
alias tcxy='tcx run -- --yolo'
```

## Run at login on macOS

Install the server as a LaunchAgent:

```sh
tcx launch-agent > ~/Library/LaunchAgents/com.yogevkr.teamcodex.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.yogevkr.teamcodex.plist
```

The plist runs `tcx server --headless --log-file ~/Library/Logs/teamcodex/server.log`.
The server creates the log directory and reopens the file when a cleanup tool deletes it.

Do not set `ProcessType` to `Background`.
macOS then runs every server thread at priority 4.
Under heavy CPU load, the proxy cannot answer Codex for seconds.
The server logs a `background_priority` event when macOS throttles it.

After you change the plist, load it again:

```sh
launchctl bootout gui/$(id -u)/com.yogevkr.teamcodex
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.yogevkr.teamcodex.plist
```

## Features

- Browser OAuth login with automatic token renewal.
- Managed tokens stored in private local files.
- Environment variables or credential commands for external brokers.
- Priority tiers, reserved groups, model restrictions, and session affinity.
- Quota polling for ChatGPT accounts.
- Response-header quota tracking for API accounts.
- Account fallback after model access, credential, rate-limit, or connection errors.
- Retryable responses for transient overloads.
- Usage-limit reset credit listing, redemption, and optional automatic redemption.
- Terminal, table, and JSON status views.
- A Codex launcher that leaves existing configuration files unchanged.
- Responses API streaming, tool calls, compaction, and `previous_response_id` routing.

## Configuration

Print a complete example:

```sh
tcx example > config.json
tcx --config config.json check
```

The example uses the optional opgate credential adapter.
Configuration files contain credential references, not credential values.

### Top-level fields

| Field | Description |
| --- | --- |
| `listen` | Loopback address. Default: `127.0.0.1:4269`. |
| `client_token_env` | Environment variable for the local proxy token. Default: `TEAMCODEX_PROXY_TOKEN`. |
| `client_token_file` | Private file for the local proxy token. Browser login creates this file. |
| `threshold_percent` | Stop selecting an account at this quota percentage. Default: `95`. |
| `probe_interval_seconds` | Usage polling interval. Set `0` to disable polling. Default: `60`. |
| `idle_timeout_seconds` | Upstream response and stream inactivity limit. Default: `300`. |
| `model_limits` | Map model names to extra quota bucket IDs. |
| `prices` | Optional per-million-token price estimates. |
| `accounts` | Configured account list. Names must be unique. |

### Account fields

Each account has a `name`, `kind`, and `credential`.
The `kind` value is `chatgpt` or `api`.

| Field | Description |
| --- | --- |
| `base_url` | Upstream URL. Defaults to the ChatGPT Codex backend or OpenAI `/v1`. |
| `usage_url` | Optional usage endpoint on the same origin. |
| `account_id` | ChatGPT account identity. |
| `priority` | Lower values are selected first for new sessions. Default: `0`. |
| `disabled` | Initial account state. Default: `false`. |
| `groups` | Serve requests with a matching `x-tcx-group` header. Accounts with no groups serve shared requests. |
| `shared_percent` | Quota percentage available to ungrouped traffic. Default: `0`, which keeps it group-only. |
| `models` | Exact model names allowed for this account. An empty list allows all models. |
| `threshold_percent` | Per-account quota threshold override. |
| `auto_reset` | Redeem a reset credit when this account is blocked. Default: `false`. |

Example account entries:

```json
{
  "name": "personal",
  "kind": "chatgpt",
  "credential": {
    "type": "command",
    "argv": ["your-credential-broker", "token", "personal"],
    "cache_seconds": 240
  },
  "priority": 0
}
```

```json
{
  "name": "api-fallback",
  "kind": "api",
  "credential": {
    "type": "env",
    "name": "OPENAI_API_KEY"
  },
  "priority": 10,
  "disabled": true
}
```

To share part of a grouped account, set `shared_percent`:

```json
{
  "name": "daybreak",
  "kind": "chatgpt",
  "groups": ["daybreak-blue"],
  "shared_percent": 20,
  "credential": {
    "type": "command",
    "argv": ["your-credential-broker", "token", "daybreak"]
  }
}
```

This account can use up to 20% of its quota for ungrouped traffic.
Its remaining quota serves `daybreak-blue` traffic.

ChatGPT accounts with browser login use managed credentials.
ChatGPT accounts with environment credentials also require `account_id`.
API accounts use API keys or another external credential source.

## Credentials

### Managed credentials

Browser login stores the access token, refresh token, account ID, and expiry in:

```
~/.config/teamcodex/config.state/accounts/
```

Token files and the local proxy token use owner-only permissions.
TeamCodex replaces tokens atomically and locks files across processes.
Keep the state directory out of source control and shared backups.

The server checks managed accounts every 30 seconds.
It refreshes tokens within five minutes of expiry.
A temporary refresh error keeps an unexpired token and delays the next attempt.
An expired or revoked refresh token requires `tcx login`.

### External credentials

A credential command runs without a shell.
Use an absolute executable path when needed.
The command must print this JSON to stdout:

```json
{
  "access_token": "token-from-the-broker",
  "account_id": "chatgpt-account-id",
  "expires_at": 1900000000
}
```

`account_id` is required for ChatGPT accounts.
`expires_at` is an optional Unix timestamp.
The command has a 15-second timeout and a 64-KiB output limit.
TeamCodex discards command stderr and never prints token values.

TeamCodex sets `TEAMCODEX_REFRESH=1` when a 401 response requires renewal.
It sets `TEAMCODEX_REFRESH=0` for other requests.
Concurrent requests share one refresh.

The example adapter is at [`examples/opgate-credential.py`](examples/opgate-credential.py).
It reads values supplied by `opagent` and returns a broker response.
It does not renew OAuth tokens.

## Account selection and recovery

TeamCodex uses these rules:

- Lower priority values win when a new session starts.
- A session stays on its selected account while that account remains eligible.
- Groups route requests to matching accounts.
- Grouped accounts can serve ungrouped traffic up to `shared_percent` quota use.
- The remaining quota stays available to matching group requests.
- Model restrictions apply per account.
- Quota thresholds exclude accounts before their limit.
- The proxy tries another eligible account after a model access rejection.
- Rate limits, credential failures, and connection failures create temporary holds.
- Temporary holds return HTTP 503 with `Retry-After` so Codex can retry.
- Quota exhaustion returns HTTP 429 with `usage_limit_reached`.
- The response includes a reset time when the proxy knows one.
- A timeout after connection does not replay the request because its outcome is unknown.
- Stream errors are never replayed after streaming starts.

ChatGPT accounts poll `/backend-api/wham/usage` by default.
API accounts use quota headers from the upstream response.
The proxy does not infer shared API limits from those headers.

The proxy pins requests with `previous_response_id` to the account that produced the response.
An unavailable pinned account returns HTTP 429 when its quota is exhausted.
An unknown response ID returns HTTP 409 and requires the full conversation history.
Bindings persist in `<config>.state/routing.jsonl`.
The journal stores routing hashes and account bindings, not prompts or response text.

### Usage-limit reset credits

ChatGPT can provide credits that reset usage windows.
List and redeem credits through the running proxy:

```sh
tcx --config config.json reset personal --list
tcx --config config.json reset personal
tcx --config config.json reset personal --credit crd_123 --yes
```

Set `auto_reset: true` to redeem a credit when a blocked request has no other eligible account.
One redemption serves concurrent requests.
TeamCodex waits 300 seconds before another automatic redemption for the same account.

## Commands

```sh
tcx login --name NAME              # Add or renew a ChatGPT account
tcx login --no-browser              # Print the login URL
tcx accounts                        # List configured accounts
tcx server [--headless]             # Start the proxy
tcx server --headless --log-file F  # Start the proxy and append output to F
tcx launch-agent                    # Print a macOS LaunchAgent plist
tcx check                           # Validate configuration
tcx status [--json|--table]        # Show live account status
tcx account NAME enable             # Enable an account until restart
tcx account NAME disable            # Disable an account until restart
tcx reload                          # Apply account changes to a running server
tcx reset NAME --list               # List reset credits
tcx reset NAME [--credit ID]        # Redeem a reset credit
tcx codex-config                    # Print Codex provider settings
tcx run [--group NAME] -- ARGS      # Run Codex through the proxy
tcx example                         # Print an example configuration
```

Use the global `--config` option before a command:

```sh
tcx --config /absolute/path/config.json status
```

Runtime account enable and disable changes reset on restart.
Edit `disabled` in the configuration to keep the state.
The server watches the configuration file and reloads account changes within two seconds.
Use `tcx reload` to apply changes at once.

## HTTP API

All routes require `Authorization: Bearer <local proxy token>`.

| Method | Route | Description |
| --- | --- | --- |
| GET | `/health` | Server status and version. |
| GET | `/status` | Account quota, usage, errors, and outcomes. |
| POST | `/accounts/{name}/enabled` | Enable or disable an account. |
| POST | `/reload` | Reload the configuration file. |
| GET | `/accounts/{name}/reset-credits` | List upstream reset credits. |
| POST | `/accounts/{name}/reset` | Redeem a reset credit. |
| POST | `/v1/responses` | Forward a Responses API request. |
| POST | `/v1/responses/compact` | Forward a compaction request. |
| GET | `/v1/models` | Forward the model list request. |

Inference routes also accept no prefix and the `/backend-api/codex` prefix.
The proxy accepts request bodies up to 128 MiB and forwards streamed events as they arrive.
It drops cookies, client API keys, actor credentials, and unspecified request headers.
Browser requests with an `Origin` header receive HTTP 403.
Custom upstreams require HTTPS.
HTTP is allowed only for numeric loopback addresses used by local tests.

## Development

Run the checks used by CI:

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --locked
python3 -B -m unittest discover -s scripts -p 'test_*.py'
```

Run the local end-to-end checks:

```sh
python3 scripts/e2e.py
python3 scripts/e2e.py --model-unavailable
python3 scripts/e2e.py --transient-error
python3 scripts/e2e.py --stream-overload server_is_overloaded
python3 scripts/e2e.py --stream-overload slow_down --overload-after-tool
python3 scripts/cli_e2e.py
python3 scripts/tui_e2e.py
```

The end-to-end tests use a local fake upstream.
They verify account fallback, tool execution, stream recovery, token totals, and account controls.
The test suite uses Codex CLI 0.153.4.

The optional live cache test uses real account quota:

```sh
python3 scripts/live_cache_e2e.py --live --config ~/.config/teamcodex/config.json --list-models
python3 scripts/live_cache_e2e.py --live --config ~/.config/teamcodex/config.json --model MODEL --output artifacts/live-cache.json
```

## Scope

TeamCodex supports the Codex Responses API over HTTP and SSE.
It does not implement WebSocket transport or a macOS menu application.
It supports browser OAuth login and external credential brokers.

Protocol references:

- [Codex provider configuration](https://github.com/openai/codex/blob/main/codex-rs/model-provider-info/src/lib.rs)
- [Codex quota headers and events](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/rate_limits.rs)
- [Codex authentication](https://developers.openai.com/codex/auth)
- [Codex browser OAuth flow](https://github.com/openai/codex/blob/rust-v0.153.4/codex-rs/login/src/server.rs)
- [Codex token refresh](https://github.com/openai/codex/blob/rust-v0.153.4/codex-rs/login/src/auth/manager.rs)

See [LICENSE](LICENSE) for license terms.
