#!/usr/bin/env python3
"""Opt-in live cache test. Uses account references, synthetic prompts, and an isolated proxy."""
import argparse
import copy
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Authorize real OpenAI requests and account quota use")
    parser.add_argument("--config", required=True, type=Path, help="Configuration references only; tcx loads credentials")
    parser.add_argument("--binary", default=str(Path(__file__).resolve().parents[1] / "target/debug/tcx"))
    parser.add_argument("--model", help="Choose a model returned by --list-models")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--list-models", action="store_true", help="Only list models from the real endpoint")
    args = parser.parse_args()
    if not args.live:
        parser.error("--live is required; this test uses real OpenAI endpoints and account quota")
    if not args.model and not args.list_models:
        parser.error("--model is required; use --list-models to check account availability")
    binary = str(Path(args.binary).resolve())
    source = json.loads(args.config.read_text())
    accounts = [copy.deepcopy(a) for a in source["accounts"]
        if not a.get("disabled", False) and a["credential"]["type"] == "managed"
        and a["kind"] == "chatgpt" and not a.get("base_url")
        and (args.list_models or not a.get("models") or args.model in a["models"])]
    assert len(accounts) >= 2, "Two enabled native ChatGPT accounts must allow the test model"
    accounts = accounts[:2]
    for index, account in enumerate(accounts):
        account.update(name=f"live-account-{index+1}", groups=[], priority=0)
    client_token = secrets.token_urlsafe(32)
    env = dict(os.environ, TEAMCODEX_LIVE_TEST_TOKEN=client_token)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    results = []
    session = "tcx-cache-test-" + str(uuid.uuid4())
    prompt = "\n".join(f"Synthetic reference row {i:04}: the blue square is beside the green circle." for i in range(384))
    body = json.dumps({"model": args.model, "instructions": "Read the synthetic reference. Reply only OK. Do not call tools.",
        "input": [{"role": "user", "content": prompt + "\nReply OK."}],
        "stream": True, "store": False, "prompt_cache_key": session, "reasoning": {"effort": "low"}}).encode()
    turn_state = None
    with tempfile.TemporaryDirectory(prefix="teamcodex-live-cache-") as temporary:
        workspace = Path(temporary)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        config = {"listen": f"127.0.0.1:{port}", "client_token_env": "TEAMCODEX_LIVE_TEST_TOKEN",
            "probe_interval_seconds": 0, "idle_timeout_seconds": 90, "accounts": accounts}
        path = workspace / "config.json"
        process = None

        def request(endpoint, data=None, headers=None):
            req = urllib.request.Request(url + endpoint, data=data,
                headers={"Authorization": "Bearer " + client_token, **(headers or {})})
            return opener.open(req, timeout=90)

        def start():
            path.write_text(json.dumps(config))
            child = subprocess.Popen([binary, "--config", str(path), "server", "--headless"], env=env,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            deadline = time.monotonic() + 15
            try:
                while time.monotonic() < deadline:
                    assert child.poll() is None, "Isolated proxy exited during startup"
                    try:
                        with request("/health") as response:
                            assert json.load(response)["status"] == "ok"
                        return child
                    except urllib.error.URLError:
                        time.sleep(0.1)
                raise RuntimeError("Isolated proxy did not start")
            except BaseException:
                stop(child)
                raise

        def stop(child):
            if child is not None and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)

        try:
            process = start()
            if args.list_models:
                with request("/v1/models?client_version=0.153.4") as response:
                    listing = json.load(response)
                rows = listing.get("models", listing.get("data", []))
                print(json.dumps([row.get("slug", row.get("id")) for row in rows]))
                return
            for index in range(3):
                if index == 2:
                    stop(process)
                    config["accounts"].reverse()
                    process = start()
                headers = {"Content-Type": "application/json", "session-id": session, "thread-id": session,
                    "x-client-request-id": str(uuid.uuid4()), "originator": "codex_cli_rs"}
                if turn_state:
                    headers["x-codex-turn-state"] = turn_state
                terminal = None
                started = time.monotonic()
                try:
                    with request("/v1/responses", body, headers) as response:
                        turn_state = response.headers.get("x-codex-turn-state", turn_state)
                        for line in response:
                            if not line.startswith(b"data:"):
                                continue
                            data = line[5:].strip()
                            if data == b"[DONE]":
                                continue
                            event = json.loads(data)
                            kind = event.get("type")
                            assert kind not in ("error", "response.failed", "response.incomplete"), "Live upstream reported a failed response"
                            if kind == "response.completed":
                                terminal = event.get("response", {})
                except urllib.error.HTTPError as error:
                    try:
                        details = json.loads(error.read(8192)).get("error", {})
                        code = details.get("code", "unknown") if isinstance(details, dict) else "unknown"
                    except (ValueError, AttributeError):
                        code = "unknown"
                    raise RuntimeError(f"Live upstream returned HTTP {error.code}, code={code}") from None
                assert terminal is not None, "No completed Responses API event"
                usage = terminal.get("usage") or {}
                with request("/status") as response:
                    status = json.load(response)
                assert status["routing_persistent"] and status["routing_healthy"]
                served = status["recent"][-1]
                assert served["status"] == 200 and served["outcome"] == "complete"
                row = {"request": index+1, "after_restart": index == 2, "account": served["account"],
                    "input_tokens": usage.get("input_tokens", 0),
                    "cached_tokens": usage.get("input_tokens_details", {}).get("cached_tokens", 0),
                    "output_tokens": usage.get("output_tokens", 0), "seconds": round(time.monotonic()-started, 2)}
                results.append(row)
                print(json.dumps(row), flush=True)
                time.sleep(2)
        finally:
            stop(process)
    evidence = {"model": args.model, "upstream": "https://chatgpt.com/backend-api/codex/responses",
        "synthetic_prompts_only": True, "same_account": len({r["account"] for r in results}) == 1,
        "warm_cache_hit": results[1]["cached_tokens"] > 0,
        "cache_hit_after_restart": results[2]["cached_tokens"] > 0, "requests": results}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2)+"\n")
    assert evidence["same_account"], "Account affinity changed"
    assert evidence["warm_cache_hit"] and evidence["cache_hit_after_restart"], "OpenAI did not report both expected cache hits"
    print("PASS: real OpenAI cache hits, including after proxy restart and account reordering")


if __name__ == "__main__":
    main()
