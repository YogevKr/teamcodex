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

- Browser login for each ChatGPT account, private token storage, and automatic OAuth renewal.
- Optional credential commands and environment variables for external brokers and OpenAI API keys.
- A credential command with bounded output, timeout, cache, and concurrent refresh control.
- Priority tiers, reserved groups, model restrictions, and session affinity.
- Automatic account selection after an explicit model access rejection.
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
tcx login --name personal
tcx accounts
tcx
```

The installed command is `tcx`. Install Codex CLI separately to use `tcx run`.
Repeat `tcx login --name another-account` for each account you want to add.
Select the intended account in the browser. Login does not change your existing Codex login.
In another terminal, launch Codex through the running pool:

```sh
tcx run -- --yolo
```

Without a running proxy, `tcx run` launches Codex directly with its normal login and configuration.
An explicit `--group` requires a running proxy. An occupied port with failed authentication never triggers direct launch.

Optional shell aliases:

```sh
alias tcxs='tcx server'
alias tcxy='tcx run -- --yolo'
```

The default configuration is `~/.config/teamcodex/config.json`. Use `--config /absolute/path/config.json` for another pool.
`tcx login` creates the configuration and local proxy token automatically. No environment variables are required for browser login.
`tcx login --no-browser` prints the login URL without opening the browser.
The callback uses loopback port 1455. Close other Codex login attempts if that port is occupied.
Restart a running server after adding accounts. Re-login to an existing account updates its token file automatically.

Native accounts use both the ChatGPT user ID and workspace ID. Users in the same workspace remain separate. A new login can repair invalid credential JSON for the same configured identity. File permission and symlink checks still apply. Temporary OAuth errors use a retry delay; explicit expired or revoked refresh tokens require login.

## Build

Use Rust 1.89 or newer, Python 3, and an installed Codex CLI.
The end-to-end test currently verifies Codex CLI 0.153.4.

```sh
cargo build --release --locked
./target/release/tcx example > config.json
./target/release/tcx --config config.json check
```

Update the example with your account names and credential commands.
Configuration files contain credential references, not credentials.
`check` validates configuration without reading credentials or contacting an upstream.

## Managed credentials

Browser login stores each account's access token, refresh token, account ID, and expiry in a separate private file.
These files live under `~/.config/teamcodex/config.state/accounts/` for the default configuration.
The local proxy token lives under the same state directory. Configuration contains only file references and account settings.
Token files use owner-only permissions (`0600`), atomic replacement, and file locks across processes.
Storage uses local files rather than an operating-system keychain. Keep these files out of source control and shared backups.

The server checks managed accounts every 30 seconds and refreshes access tokens within five minutes of expiry.
This loop runs even when quota probing is disabled. Concurrent callers share token renewal.
A 401 can force renewal immediately. A temporary refresh failure keeps an unexpired access token and delays further renewal attempts.
Rejected refresh credentials require `tcx login` again. Re-login preserves the account's priority, model list, groups, and enabled state.
`tcx accounts` displays account metadata and login status without displaying tokens.

## External credentials

External credential sources remain optional. `tcx example` prints a configuration for the opgate adapter and API fallback.
Homebrew installs the adapter under `$(brew --prefix teamcodex)/share/teamcodex/examples/`.

Set `client_token_env` to a variable containing a local proxy token, or set `client_token_file` to its private file.
When the variable is set, it takes precedence over the file.
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

For command sources, the external command owns account login, secure storage, and OAuth token renewal.
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
| `client_token_env` | Local proxy token variable. Default: `TEAMCODEX_PROXY_TOKEN`. Optional when a token file is configured. |
| `client_token_file` | Absolute path to a private local proxy token file. Browser login creates it automatically. |
| `threshold_percent` | Stop selecting an account at this percentage. Default: `95`. |
| `probe_interval_seconds` | Usage polling interval. Default: `60`. Set `0` to disable polling. |
| `idle_timeout_seconds` | Upstream response and stream inactivity limit. Default: `300`. |
| `model_limits` | Map model names to additional quota bucket IDs. |
| `prices` | Optional model prices per million tokens, in USD. |
| `accounts` | Account list. Names must be unique. The server requires at least one account. |

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
| `models` | Exact allowed model names. Empty allows any model not temporarily restricted by an upstream rejection. |

ChatGPT accounts without an endpoint override poll `/backend-api/wham/usage`.
API accounts use response headers; they do not call the ChatGPT usage endpoint.
API request and token limits apply to the model that produced those headers.
The proxy does not infer shared API model-family limits from these headers.
Custom endpoints require HTTPS. HTTP is allowed only for numeric loopback addresses during local tests.
The proxy does not follow upstream redirects.

Model access can differ between accounts. Set each account's `models` list when you know its available models.
An empty list means availability is unknown until the upstream responds.
On an explicit model access rejection, the proxy tries another eligible account with the same model and request body.
It remembers the rejected account and model for five minutes. Other models can still use that account.
After five minutes, the account becomes eligible for another attempt with that model.
The status field `unavailable_models` maps each rejected model to its retry time, in Unix seconds.
Each account stores at most 256 rejected models. This cache resets when the server restarts.

The proxy recognizes `model_not_found`, `model_access_denied`, and the explicit ChatGPT account model rejection message.
Generic permission, parameter, endpoint, and safety errors pass through without account changes.
If every matching account excludes the model, the proxy returns HTTP 404 with code `model_unavailable`.
Quota exhaustion still returns HTTP 429. The proxy does not substitute a different model.
That 429 body uses error type `usage_limit_reached` with code `pool_exhausted` and, when known, `resets_at` in Unix seconds.
Codex reads this type as a final usage-limit error and shows the reset time instead of retrying the bare status.

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

An established session stays on its eligible account, even when a higher-priority account recovers.
Priority tiers select new sessions. Affinity is separate for each model and group.
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
It selects HTTP streaming and leaves Codex request and stream retries at their normal defaults.
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

These rules describe proxy attempts. Codex controls client retries and can submit another request after a returned error.

- A 401 response triggers one credential refresh and one retry on that account.
- A second 401 puts that account on hold and selects another eligible account.
- A 429 response records `Retry-After` and selects another eligible account.
- An explicit model rejection with HTTP 400, 403, or 404 restricts that account and model, then selects another account.
- A connection failure can select another account before the request reaches the upstream.
- A timeout after connection has an unknown outcome. The proxy returns 502 without replay.
- A failed request before response headers keeps the `upstream_outcome_unknown` error code. Its message includes the failure category, I/O category when available, elapsed milliseconds, and configured timeout.
- Status records the failure category, such as `upstream_response_header_timeout` or `upstream_transport_error`. The server also writes a JSON error record to stderr with the account and timestamp. This record survives the 100-entry recent-status window when stderr is retained.
- Error records exclude raw error text, URLs, headers, credentials, and request bodies. A transport failure does not prove whether the upstream processed the request.
- Other upstream errors pass through once.
- Stream errors never trigger request replay after streaming starts.
- A stream rate-limit error puts the account on hold for later requests.
- A streamed model rejection restricts that account and model for later requests. The current stream is not replayed.

`previous_response_id` pins a request to the account that produced that response.
An unavailable pinned account returns 429.
An unknown response ID returns 409 and requires full conversation history.
A model rejection on a pinned account returns 404. Choose another model or resend full history without `previous_response_id`.
Response and session records expire after 24 hours. Each record map holds at most 10,000 entries.

The server saves session and response routing in `<config>.state/routing.jsonl` before using new bindings.
The journal uses private files, a single-writer lock, durable appends, and bounded compaction.
It stores hashes of routing keys and account bindings. It stores no prompts, response text, or credentials.
Bindings survive restarts and account reordering. Changed account identities or credential sources invalidate old bindings.
Unchanged bindings refresh at most once per minute. An incomplete final journal record is discarded after a crash.
A malformed complete record stops startup. A write failure blocks later requests instead of silently discarding affinity.
`tcx status` reports `routing_persistent` and `routing_healthy`.

The proxy forwards prompt bytes, `prompt_cache_key`, cache options, session headers, and turn-state headers unchanged.
It accepts Codex `session-id` and `thread-id` headers and legacy underscore spellings.
Diagnostic `x-client-request-id` values never override a cache key or create affinity.
Account fallback can require a new upstream cache. OpenAI controls cache placement, retention, and eviction.
See [OpenAI prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching).

Usage, holds, and account controls remain in memory and reset on restart. Status totals cover the current server process.
Upgrading from 0.2.0 cannot recover its in-memory bindings. Restart between active Codex sessions to avoid losing them.

## Verification

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --locked
python3 -B -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/e2e.py
python3 scripts/e2e.py --model-unavailable
python3 scripts/e2e.py --transient-error
python3 scripts/e2e.py --managed-credentials --model-unavailable --yolo
python3 scripts/cli_e2e.py
python3 scripts/tui_e2e.py
```

The end-to-end test starts the compiled proxy and a local fake OpenAI server.
It runs the installed Codex CLI through `tcx run`.
The first account returns 429. The second account requests a file write in a temporary workspace.
Codex executes the command and returns its result through the proxy.
The test checks the final response, account change, token totals, and account controls.
The `--model-unavailable` case replaces the first account's rate limit with a model rejection and repeats the tool cycle.
Local tests use Codex's `workspace-write` sandbox by default.
The disposable Linux CI runner blocks sandbox user namespaces, so that job passes `--sandbox danger-full-access`.
Its local mock supplies only the fixed marker command. This option does not change `tcx` defaults.

`python3 scripts/e2e.py --skip-codex` runs the process smoke test without Codex.
It does not verify the Codex tool cycle.

The terminal test opens a real PTY. It checks rendering, the Space control, and `q` shutdown.

Unit and integration tests cover quota parsing, expired windows, stream boundaries, credential isolation, and concurrent refresh.
They also check conversation pinning, stream interruption, status, and terminal rendering.

OAuth tests use a local authorization server to verify PKCE, callback validation, token exchange, and account registration.
Refresh tests verify token rotation, concurrent callers, expiry, temporary failures, and rejected credentials.
The CLI tests check direct launch, proxy authentication, YOLO argument order, and account-list redaction.
Local tests use synthetic credentials. They do not prove live ChatGPT or OpenAI account access.
An opt-in live test uses synthetic prompts through an isolated proxy and your configured native accounts:

```sh
python3 scripts/live_cache_e2e.py --live --config ~/.config/teamcodex/config.json --list-models
python3 scripts/live_cache_e2e.py --live --config ~/.config/teamcodex/config.json --model MODEL --output artifacts/live-cache.json
```

Select a model your account lists. The test uses real account quota.
It checks reported cached tokens, then repeats after restarting the test proxy and reversing the account order.
It leaves the existing proxy and sessions running. It reads configuration references; TeamCodex loads credentials.
Live verification requires completing `tcx login` or supplying an external credential source.

## Scope

TeamCodex supports Codex through the Responses API over HTTP and SSE.
It does not implement WebSocket transport or a macOS menu application.
Browser OAuth login and token renewal are built in. External credential brokers remain optional.
The proxy uses each configured account's available quota and respects its reset and hold periods.

Protocol references:

- [Codex provider configuration](https://github.com/openai/codex/blob/main/codex-rs/model-provider-info/src/lib.rs)
- [Codex quota headers and events](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/rate_limits.rs)
- [Codex authentication](https://developers.openai.com/codex/auth)
- [Codex browser OAuth flow](https://github.com/openai/codex/blob/rust-v0.153.4/codex-rs/login/src/server.rs)
- [Codex token refresh](https://github.com/openai/codex/blob/rust-v0.153.4/codex-rs/login/src/auth/manager.rs)
