#!/usr/bin/env python3
"""Verify direct launch and account listing with synthetic local fixtures."""
import argparse
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default=str(Path(__file__).resolve().parents[1] / "target/debug/tcx"))
    binary = str(Path(parser.parse_args().binary).resolve())
    with tempfile.TemporaryDirectory(prefix="teamcodex-cli-e2e-") as temporary:
        workspace = Path(temporary)
        stub = workspace / "codex"
        stub.write_text('#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n')
        stub.chmod(0o700)
        env = dict(os.environ, PATH=str(workspace) + os.pathsep + os.environ["PATH"])
        env.pop("TEAMCODEX_CLI_TEST_TOKEN", None)
        path = workspace / "config.json"

        def run(*args):
            return subprocess.run([binary, "--config", str(path), *args], env=env,
                capture_output=True, text=True, timeout=15)

        assert run("accounts").stdout.strip() == "[]"
        assert "tcx login" in run().stderr
        # Missing configuration uses the default port. Use a known unused port
        # for the launch test so an existing local TeamCodex server cannot affect it.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        config = {"listen": f"127.0.0.1:{port}", "client_token_env": "TEAMCODEX_CLI_TEST_TOKEN", "accounts": []}
        path.write_text(json.dumps(config))
        direct = run("run", "--", "--yolo", "exec", "hello")
        assert direct.returncode == 0, direct.stderr
        assert json.loads(direct.stdout) == ["--yolo", "exec", "hello"]
        assert "launching Codex directly" in direct.stderr
        assert run("run", "--group", "missing", "--", "hello").returncode != 0
        assert run("login", "--name", "../invalid", "--no-browser").returncode != 0

        class Health(http.server.BaseHTTPRequestHandler):
            status = 401

            def log_message(self, *_args):
                pass

            def do_GET(self):
                body = json.dumps({"status": "ok", "version": "synthetic"}).encode()
                self.send_response(self.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Health)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            token_path = workspace / "proxy.token"
            token_path.write_text("synthetic-local-proxy-token")
            token_path.chmod(0o600)
            config.update(listen=f"127.0.0.1:{server.server_port}", client_token_file=str(token_path))
            path.write_text(json.dumps(config))
            rejected = run("run", "--", "--yolo")
            assert rejected.returncode != 0
            assert not rejected.stdout, "A failed health check must not launch Codex directly"
            assert "synthetic-local-proxy-token" not in rejected.stderr
            Health.status = 200
            proxied = run("run", "--", "--yolo", "exec", "hello")
            assert proxied.returncode == 0, proxied.stderr
            forwarded = json.loads(proxied.stdout)
            assert forwarded[0] == "exec"
            assert 'model_provider="teamcodex"' in forwarded
            assert forwarded[-2:] == ["--yolo", "hello"]
        finally:
            server.shutdown()
            server.server_close()

        account = workspace / "account.json"
        account.write_text(json.dumps({"access_token": "synthetic-access-private", "refresh_token": "synthetic-refresh-private",
            "account_id": "test-account", "user_id": "user-a", "expires_at": int(time.time())+3600, "email": None}))
        account.chmod(0o600)
        config["accounts"] = [{"name": "personal", "kind": "chatgpt", "account_id": "test-account", "user_id": "user-a",
            "credential": {"type": "managed", "path": str(account)}}]
        path.write_text(json.dumps(config))
        listed = run("accounts")
        assert listed.returncode == 0, listed.stderr
        assert json.loads(listed.stdout)[0]["login_required"] is False
        assert "synthetic-access-private" not in listed.stdout + listed.stderr
        assert "synthetic-refresh-private" not in listed.stdout + listed.stderr
        # A valid environment token wins even when the configured file is invalid.
        token_path.write_text("invalid")
        config["accounts"][0]["base_url"] = "http://127.0.0.1:1"
        config["probe_interval_seconds"] = 0
        path.write_text(json.dumps(config))
        server_env = dict(env, TEAMCODEX_CLI_TEST_TOKEN="synthetic-valid-environment-token")
        process = subprocess.Popen([binary, "--config", str(path), "server", "--headless"], env=server_env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    request = urllib.request.Request("http://" + config["listen"] + "/health",
                        headers={"Authorization": "Bearer synthetic-valid-environment-token"})
                    with urllib.request.urlopen(request, timeout=1) as response:
                        assert json.load(response)["status"] == "ok"
                    break
                except (OSError, urllib.error.URLError):
                    assert process.poll() is None and time.monotonic() < deadline, "Environment token did not override the invalid file"
                    time.sleep(0.05)
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
            process.stderr.close()
        print("PASS: direct launch, authenticated proxy launch, YOLO exec arguments, redacted account listing, and token precedence")


if __name__ == "__main__":
    main()
