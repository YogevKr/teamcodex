#!/usr/bin/env python3
"""Check terminal rendering, Space controls, and q shutdown on a real PTY."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import signal
import socket
import struct
import tempfile
import termios
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default=str(Path(__file__).resolve().parents[1] / "target/debug/tcx"))
    binary = str(Path(parser.parse_args().binary).resolve())
    with tempfile.TemporaryDirectory(prefix="tcx-tui-e2e-") as directory:
        path = Path(directory) / "config.json"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        token = "synthetic-tui-test-token"
        path.write_text(json.dumps({
            "listen": f"127.0.0.1:{port}", "client_token_env": "TCX_TUI_TEST_TOKEN", "probe_interval_seconds": 0,
            "accounts": [{"name": "demo", "kind": "api", "credential": {"type": "env", "name": "TCX_UNUSED_TEST_TOKEN"}}],
        }))
        pid, master = pty.fork()
        if pid == 0:
            os.execve(binary, [binary, "--config", str(path), "server"],
                dict(os.environ, TERM="xterm-256color", TCX_TUI_TEST_TOKEN=token))
        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 140, 0, 0))
        done = False
        output = bytearray()

        def drain(seconds):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                if select.select([master], [], [], 0.05)[0]:
                    try:
                        chunk = os.read(master, 65536)
                    except OSError:
                        return
                    if not chunk:
                        return
                    output.extend(chunk)

        def status():
            request = urllib.request.Request(f"http://127.0.0.1:{port}/status",
                headers={"Authorization": "Bearer " + token})
            with urllib.request.urlopen(request, timeout=2) as response:
                return json.load(response)

        try:
            deadline = time.monotonic() + 10
            while b"TeamCodex" not in output and time.monotonic() < deadline:
                drain(0.1)
            assert b"TeamCodex" in output, "Terminal display did not start"
            assert not status()["accounts"][0]["disabled"]
            os.write(master, b" ")
            deadline = time.monotonic() + 3
            while not status()["accounts"][0]["disabled"] and time.monotonic() < deadline:
                drain(0.1)
            assert status()["accounts"][0]["disabled"], "Space did not disable account"
            os.write(master, b"q")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                result = os.waitpid(pid, os.WNOHANG)
                if result[0]:
                    done = True
                    assert os.waitstatus_to_exitcode(result[1]) == 0, "Server exit failed"
                    break
                drain(0.1)
            assert done, "q did not stop server"
            print("PASS: real terminal rendering, Space control, q shutdown")
        finally:
            if not done:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            os.close(master)


if __name__ == "__main__":
    main()
