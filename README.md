# TeamCodex

A local account pool and streaming proxy for Codex. The binary name is `tcx`.

TeamCodex selects an account for each request. It tracks quota windows and keeps sessions on eligible accounts.
It sends OpenAI Responses API streams without changing their bytes.

## Why a separate project

[teamclaude-rs](https://github.com/dhkts1/teamclaude-rs) implements the account pool concept for Anthropic.
Its authentication, quota headers, and stream events depend on Anthropic.
Its source uses the PolyForm Noncommercial license.

This project implements the concept independently in Rust. It uses the MIT license.
It does not copy source from teamclaude-rs or change that project.

## Features

- ChatGPT account tokens and OpenAI API keys through external credential sources.
- A credential command with bounded output, timeout, cache, and concurrent refresh control.
- Priority tiers, reserved groups, model restrictions, and session affinity.
- Primary, secondary, and configured model quota windows.
- Usage polling and quota updates from response headers and stream events.
- Account changes after explicit rate limits, rejected credentials, or connection failures.
- Streaming, tool calls, compaction, and account pinning for `previous_response_id`.
- Token totals, cached token totals, optional price estimates, and request outcomes.
- Terminal display, JSON status, and account controls.
- A Codex launcher that leaves existing configuration files intact.

## Install

Install through the Homebrew tap:

```sh
brew install yogevkr/tap/teamcodex
tcx --version
tcx example > config.json
tcx --config config.json check
```

The installed command is `tcx`. Install Codex CLI separately to use `tcx run`.
Edit `config.json` with your account names and credential sources before starting the proxy.
Homebrew installs the credential adapter under `$(brew --prefix teamcodex)/share/teamcodex/examples/`.

## Build

Use Rust 1.88 or newer, Python 3, and an installed Codex CLI.
The end-to-end test currently verifies Codex CLI 0.153.4.

```sh
cargo build --release --locked
./target/release/tcx example > config.json
./target/release/tcx --config config.json check
```

Update the example with your account names and credential commands.
Configuration files contain credential references, not credentials.
`check` validates configuration without reading credentials or contacting an upstream.

## Credentials

Set `client_token_env` to a variable containing a local proxy token.
The token must contain at least 16 visible ASCII characters.
Both the proxy and its clients require this token.

Each account chooses one credential source:

```json
{"type": "env", "name": "OPENAI_API_KEY"}
```

```json
{
  "type": "command",
  "argv": ["your-credential-broker", "token", "personal"],
  "cache_seconds": 240
}
```

The command runs directly, without a shell. Use absolute paths for scripts.
It returns this JSON through its private stdout pipe:

```json
{
  "access_token": "<token supplied by the credential broker>",
  "account_id": "<ChatGPT account ID>",
  "expires_at": 1900000000
}
```

`account_id` is required for ChatGPT accounts. An account configuration can also supply this value.
`expires_at` is an optional Unix timestamp in seconds.
The proxy rejects tokens that expire within five seconds.

The command owns account login, secure storage, and OAuth token renewal.
The proxy sets `TEAMCODEX_REFRESH=1` when a 401 response requires renewal.
Otherwise, it sets `TEAMCODEX_REFRESH=0`.
Concurrent requests share one refresh. Commands have a 15-second timeout and a 64-KiB output limit.
The proxy discards command stderr and never includes token output in errors or status.

Environment credentials remain fixed for the server process.
Use a command source when credentials can change during operation.
The proxy does not read Codex authentication files or change the active Codex account.

### opgate

The example adapter receives these variables from `opagent`:

- `CODEX_PERSONAL_ACCESS_TOKEN`
- `CODEX_PERSONAL_ACCOUNT_ID`
- `CODEX_PERSONAL_EXPIRES_AT`, when available

Add the values to the `agents` vault and its opgate configuration.
Set the adapter path in `examples/config.json` to your absolute checkout path.
The adapter returns the vault token. It does not renew OAuth tokens itself.
Use a refresh-aware credential broker for unattended OAuth renewal.

Start each command through opgate when the agent profile supplies `TEAMCODEX_PROXY_TOKEN`:

```sh
opagent ./target/release/tcx --config config.json server
opagent ./target/release/tcx --config config.json status
opagent ./target/release/tcx --config config.json run
opagent ./target/release/tcx --config config.json run -- exec "Explain this repository"
```

## Configuration

| Field | Behavior |
| --- | --- |
| `listen` | Loopback address. Default: `127.0.0.1:4269`. |
| `client_token_env` | Variable containing the local proxy token. Required. |
| `threshold_percent` | Stop selecting an account at this percentage. Default: `95`. |
| `probe_interval_seconds` | Usage polling interval. Default: `60`. Set `0` to disable polling. |
| `idle_timeout_seconds` | Upstream response and stream inactivity limit. Default: `300`. |
| `model_limits` | Map model names to additional quota bucket IDs. |
| `prices` | Optional model prices per million tokens, in USD. |
| `accounts` | Nonempty account list. Names must be unique. |

Each account has a `name`, `kind`, and `credential`.
`kind` is `chatgpt` or `api`.

| Account field | Behavior |
| --- | --- |
| `base_url` | Defaults to the ChatGPT Codex backend or OpenAI `/v1`. |
| `usage_url` | Optional usage endpoint on the same origin. |
| `account_id` | ChatGPT account identity. Overrides the credential command value. |
| `priority` | Lower values select first. Default: `0`. |
| `disabled` | Initial account state. Default: `false`. |
| `groups` | Reserved groups. Empty accounts serve requests without a group. |
| `models` | Exact allowed model names. Empty allows all models. |

ChatGPT accounts without an endpoint override poll `/backend-api/wham/usage`.
API accounts use response headers; they do not call the ChatGPT usage endpoint.
API request and token limits apply to the model that produced those headers.
The proxy does not infer shared API model-family limits from these headers.
Custom endpoints require HTTPS. HTTP is allowed only for numeric loopback addresses during local tests.
The proxy does not follow upstream redirects.

Configure model buckets explicitly. The proxy does not guess model names from bucket labels.
It always applies the default Codex bucket and adds the configured buckets.

```json
{
  "model_limits": {
    "your-spark-model": ["codex-spark"]
  },
  "prices": {
    "your-model": {
      "input_per_million": 1.0,
      "cached_input_per_million": 0.1,
      "output_per_million": 2.0
    }
  }
}
```

These prices are examples, not current OpenAI prices.
Price estimates describe API-equivalent token cost. They do not represent subscription charges.
`unpriced_requests` reports calls without usable pricing or usage data.

Within a priority tier, session affinity selects first.
Other requests prefer fewer active requests, then the earliest known reset, then the least recent selection.
A quota window becomes available when its recorded reset time passes.
Unknown quota remains unknown until the server reports it.

## Commands

```sh
tcx --config config.json server --headless
tcx --config config.json status
tcx --config config.json account personal disable
tcx --config config.json account personal enable
tcx --config config.json codex-config
tcx --config config.json run --group reserved -- exec "Run the tests"
```

Account controls change runtime state. Update the configuration to preserve a disabled state across restarts.
In the terminal display, use `j` and `k` to select an account.
Use Space to change its state. Use `q` to stop the server.

`codex-config` prints TOML settings for manual setup.
`run` passes these settings to Codex for one process.
It selects HTTP streaming and disables Codex request retries for this provider.
The proxy handles the permitted account changes.

## HTTP interface

All routes require `Authorization: Bearer <local proxy token>`.

| Method | Route | Result |
| --- | --- | --- |
| GET | `/health` | Server status and version. |
| GET | `/status` | Account quota, usage, errors, and recent outcomes. |
| POST | `/accounts/{name}/enabled` | Change account state with `{"enabled": true}`. |
| POST | `/v1/responses` | Forward a Responses API request. |
| POST | `/v1/responses/compact` | Forward a compaction request. |
| GET | `/v1/models` | Forward the model list request. |

Inference routes also accept no prefix or the `/backend-api/codex` prefix.
The proxy accepts JSON bodies up to 16 MiB.
It requires uncompressed request bodies.

The proxy replaces client authentication with the selected account token and account ID.
It drops cookies, client API keys, actor credentials, and unspecified request headers.
It drops upstream cookies and authentication headers from responses.
Browser requests with an `Origin` header receive 403.

## Retry and conversation rules

- A 401 response triggers one credential refresh and one retry on that account.
- A second 401 puts that account on hold and selects another eligible account.
- A 429 response records `Retry-After` and selects another eligible account.
- A connection failure can select another account before the request reaches the upstream.
- A timeout after connection has an unknown outcome. The proxy returns 502 without replay.
- Other upstream errors pass through once.
- Stream errors never trigger request replay after streaming starts.
- A stream rate-limit error puts the account on hold for later requests.

`previous_response_id` pins a request to the account that produced that response.
An unavailable pinned account returns 429.
An unknown response ID returns 409 and requires full conversation history.
Response and session records expire after 24 hours. Each record map holds at most 10,000 entries.

Usage, holds, affinity, and account controls remain in memory.
They reset when the server restarts. Status totals cover the current server process.
The proxy stores no prompts, response bodies, credentials, or usage ledger on disk.

## Verification

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --locked
python3 -B -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/e2e.py
python3 scripts/tui_e2e.py
```

The end-to-end test starts the compiled proxy and a local fake OpenAI server.
It runs the installed Codex CLI through `tcx run`.
The first account returns 429. The second account requests a file write in a temporary workspace.
Codex executes the command and returns its result through the proxy.
The test checks the final response, account change, token totals, and account controls.

`python3 scripts/e2e.py --skip-codex` runs the process smoke test without Codex.
It does not verify the Codex tool cycle.

The terminal test opens a real PTY. It checks rendering, the Space control, and `q` shutdown.

Unit and integration tests cover quota parsing, expired windows, stream boundaries, credential isolation, and concurrent refresh.
They also check conversation pinning, stream interruption, status, and terminal rendering.

Local tests use synthetic credentials. They do not prove live ChatGPT or OpenAI account access.
Live verification requires credentials supplied through the configured source.

## Scope

TeamCodex supports Codex through the Responses API over HTTP and SSE.
It does not implement WebSocket transport, a macOS menu application, or browser login.
Account login and OAuth renewal belong to the external credential broker.
The proxy uses each configured account's available quota and respects its reset and hold periods.

Protocol references:

- [Codex provider configuration](https://github.com/openai/codex/blob/main/codex-rs/model-provider-info/src/lib.rs)
- [Codex quota headers and events](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/rate_limits.rs)
- [Codex authentication](https://developers.openai.com/codex/auth)
