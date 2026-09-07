#!/usr/bin/env python3
"""Executable-to-executable E2E: installed Codex -> tcx -> local fake OpenAI.

No OpenAI credentials or paid requests are used. All writes stay in a temporary
workspace. The fake provider requests one harmless file write, checks the tool
result, then returns a final response.
"""
import argparse
import http.server
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
CLIENT_TOKEN = "local-e2e-client-token-not-a-secret"


def request(url, path, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url + path, data=data, method=method,
        headers={"Authorization": "Bearer " + CLIENT_TOKEN, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def startup_diagnostics(process):
    """A live process holds stderr open. Stop it before waiting for EOF."""
    if process.poll() is None:
        process.terminate()
    try:
        return process.communicate(timeout=3)[1] or ""
    except subprocess.TimeoutExpired:
        process.kill()
        return process.communicate(timeout=3)[1] or ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default=str(ROOT / "target/debug/tcx"))
    parser.add_argument("--skip-codex", action="store_true")
    parser.add_argument("--model-unavailable", action="store_true",
        help="First account rejects the model instead of returning a rate limit")
    parser.add_argument("--managed-credentials", action="store_true", help="Use private account files instead of credential commands")
    parser.add_argument("--yolo", action="store_true", help="Test the YOLO alias argument order with the fixed local mock command")
    parser.add_argument("--sandbox", choices=("workspace-write", "danger-full-access"), default="workspace-write",
        help="Codex tool sandbox; CI runners without user namespaces require danger-full-access")
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    if not args.skip_codex and not shutil.which("codex"):
        raise SystemExit("Codex CLI is required; install it or use --skip-codex for the process smoke test")
    observed = []
    lock = threading.Lock()
    with tempfile.TemporaryDirectory(prefix="teamcodex-e2e-") as temporary:
        workspace = Path(temporary)
        marker = workspace / "verified.txt"

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def do_GET(self):
                data = json.dumps({"object": "list", "data": []}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                payload = json.loads(body)
                token = self.headers.get("Authorization")
                with lock:
                    observed.append({"token": token, "body": payload, "path": self.path})
                    number = len(observed)
                if token == "Bearer test-upstream-a":
                    failure = json.dumps({"error": {"code": "model_not_found"}}).encode() if args.model_unavailable else b""
                    self.send_response(404 if args.model_unavailable else 429)
                    self.send_header("Content-Type", "application/json")
                    if not args.model_unavailable:
                        self.send_header("Retry-After", "300")
                    self.send_header("Content-Length", str(len(failure)))
                    self.end_headers()
                    self.wfile.write(failure)
                    return
                assert token == "Bearer test-upstream-b", token
                response_id = "resp_e2e_" + str(number)
                tool_result = any(item.get("type") in ("function_call_output", "custom_tool_call_output")
                    for item in payload.get("input", []) if isinstance(item, dict))
                tools = payload.get("tools", [])
                tool = next((t for t in tools if t.get("name") in ("exec_command", "shell_command")), None)
                if tool and not tool_result:
                    name = tool["name"]
                    command = "printf TEAMCODEX_TOOL_OK > verified.txt"
                    arguments = {"cmd" if name == "exec_command" else "command": command}
                    if name == "exec_command":
                        arguments["workdir"] = str(workspace)
                    item = {"id": "fc_e2e", "type": "function_call", "call_id": "call_e2e",
                        "name": name, "arguments": json.dumps(arguments)}
                else:
                    text = "TEAMCODEX_E2E_OK" if tool_result else "TEAMCODEX_PROCESS_OK"
                    item = {"id": "msg_e2e", "type": "message", "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": text, "annotations": []}]}
                response = {"id": response_id, "object": "response", "status": "completed", "output": [item],
                    "usage": {"input_tokens": 20, "output_tokens": 9, "total_tokens": 29, "input_tokens_details": {"cached_tokens": 5}}}
                events = [
                    {"type": "response.created", "response": {"id": response_id, "object": "response", "status": "in_progress", "output": []}},
                    {"type": "response.output_item.added", "output_index": 0, "item": item},
                    {"type": "response.output_item.done", "output_index": 0, "item": item},
                    {"type": "response.completed", "response": response},
                ]
                stream = "".join("data: " + json.dumps(event) + "\n\n" for event in events).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(stream)))
                self.send_header("x-codex-primary-used-percent", "12")
                self.send_header("x-codex-primary-reset-at", str(int(time.time()) + 3600))
                self.end_headers()
                for offset in range(0, len(stream), 7):
                    self.wfile.write(stream[offset:offset + 7])
                    self.wfile.flush()

        upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        upstream.daemon_threads = True
        upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        upstream_thread.start()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            proxy_port = reservation.getsockname()[1]
        url = f"http://127.0.0.1:{proxy_port}"
        config = {
            "listen": f"127.0.0.1:{proxy_port}", "client_token_env": "TEAMCODEX_E2E_TOKEN", "probe_interval_seconds": 0,
            "accounts": [{"name": name, "kind": "api", "base_url": f"http://127.0.0.1:{upstream.server_port}",
                "credential": {"type": "command", "argv": [shutil.which("python3"), "-c",
                    "import json; print(json.dumps({'access_token':'test-upstream-" + name + "'}))"]}}
                for name in ("a", "b")],
        }
        config_path = workspace / "config.json"
        if args.managed_credentials:
            token_path = workspace / "proxy.token"
            token_path.write_text(CLIENT_TOKEN)
            token_path.chmod(0o600)
            config["client_token_file"] = str(token_path)
            for account in config["accounts"]:
                path = workspace / (account["name"] + ".json")
                account_id = "test-account-" + account["name"]
                path.write_text(json.dumps({"access_token": "test-upstream-" + account["name"],
                    "refresh_token": "synthetic-unused-refresh", "account_id": account_id, "user_id": "user-a",
                    "expires_at": int(time.time()) + 3600, "email": None}))
                path.chmod(0o600)
                account.update(kind="chatgpt", account_id=account_id, user_id="user-a", credential={"type": "managed", "path": str(path)})
        config_path.write_text(json.dumps(config))
        # If Codex loses the selected provider, its default endpoint must still
        # stay local. The upstream assertions also require the selected account.
        env = dict(os.environ, TEAMCODEX_E2E_TOKEN=CLIENT_TOKEN, OPENAI_BASE_URL=url + "/v1")
        if args.managed_credentials:
            env.pop("TEAMCODEX_E2E_TOKEN", None)
        command = [binary, "--config", str(config_path)]
        subprocess.run(command + ["check"], check=True, capture_output=True, env=env)
        process = subprocess.Popen(command + ["server", "--headless"], env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 15
            while True:
                try:
                    health = request(url, "/health")
                    assert health["status"] == "ok"
                    break
                except (OSError, urllib.error.URLError):
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise AssertionError("proxy startup failed: " + startup_diagnostics(process))
                    time.sleep(0.05)
            if args.skip_codex:
                req = urllib.request.Request(url + "/v1/responses", data=b'{"model":"test","stream":true}',
                    headers={"Authorization": "Bearer " + CLIENT_TOKEN, "Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=10) as response:
                    assert b"TEAMCODEX_PROCESS_OK" in response.read()
            else:
                launch = ["--yolo", "exec"] if args.yolo else ["exec", "--sandbox", args.sandbox]
                result = subprocess.run(command + ["run", "--"] + launch + ["--ignore-user-config", "--ignore-rules",
                    "--ephemeral", "--skip-git-repo-check", "--json",
                    "-c", 'cli_auth_credentials_store="ephemeral"',
                    "-c", 'model="gpt-5.3-codex"',
                    "-c", 'model_reasoning_effort="low"',
                    "-C", str(workspace), "Write the requested test marker, then return the test result."],
                    env=env, cwd=workspace, capture_output=True, text=True, timeout=90)
                if result.returncode or "TEAMCODEX_E2E_OK" not in result.stdout:
                    raise AssertionError("Codex tool cycle failed:\n" + result.stdout[-6000:] + "\n" + result.stderr[-6000:])
                if not marker.is_file() or marker.read_text() != "TEAMCODEX_TOOL_OK":
                    outputs = [item for call in observed for item in call["body"].get("input", [])
                        if isinstance(item, dict) and item.get("type") in ("function_call_output", "custom_tool_call_output")]
                    raise AssertionError("Codex did not create the test marker:\n"
                        + json.dumps(outputs)[-6000:] + "\n" + result.stdout[-6000:] + "\n" + result.stderr[-6000:])
                assert len(observed) >= 3, observed
                assert any(any(item.get("type") == "function_call_output" for item in call["body"].get("input", [])
                    if isinstance(item, dict)) for call in observed), "No tool output returned through proxy"
            assert observed[0]["token"] == "Bearer test-upstream-a"
            assert all(call["token"] == "Bearer test-upstream-b" for call in observed[1:])
            assert observed[0]["body"] == observed[1]["body"], "Failover changed the request body"
            status_result = subprocess.run(command + ["status"], env=env, capture_output=True, text=True, check=True)
            status = json.loads(status_result.stdout)
            if args.model_unavailable:
                assert status["accounts"][0]["unavailable_models"][observed[0]["body"]["model"]] > time.time()
                assert status["accounts"][0]["hold_until"] == 0
            else:
                assert status["accounts"][0]["hold_until"] > time.time()
            assert status["accounts"][1]["input_tokens"] >= 20
            assert status["accounts"][1]["in_flight"] == 0
            assert "test-upstream" not in status_result.stdout
            accounts_result = subprocess.run(command + ["accounts"], env=env, capture_output=True, text=True, check=True)
            assert "test-upstream" not in accounts_result.stdout
            assert "synthetic-unused-refresh" not in accounts_result.stdout
            subprocess.run(command + ["account", "b", "disable"], env=env, capture_output=True, check=True)
            assert request(url, "/status")["accounts"][1]["disabled"]
            subprocess.run(command + ["account", "b", "enable"], env=env, capture_output=True, check=True)
            assert not request(url, "/status")["accounts"][1]["disabled"]
            print(json.dumps({"result": "PASS", "codex_tool_cycle": not args.skip_codex,
                "upstream_requests": len(observed), "failover": True, "status": True,
                "failover_reason": "model_unavailable" if args.model_unavailable else "rate_limit",
                "managed_credentials": args.managed_credentials, "yolo_launch": args.yolo,
                "account_controls": True, "input_tokens": status["accounts"][1]["input_tokens"]}, indent=2))
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            process.stderr.close()
            upstream.shutdown()
            upstream.server_close()


if __name__ == "__main__":
    main()
