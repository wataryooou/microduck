#!/usr/bin/env python3
"""A duck's controls in a browser, on localhost — for the simulated duck, or any `robotd` socket.

    scripts/duck-sim web                       # http://127.0.0.1:8765
    scripts/duck-web/server.py --socket /run/robotd.sock

The page talks HTTP to this process and this process talks JSON-RPC to `robotd`'s unix socket — the
same calls `scripts/duck-sim drive` and `robotctl` send. Standard library only, so it runs from any
python3 with nothing installed.

**Not the console.** `mediad` serves the real one — camera, WebRTC, the same control channel a
remote peer uses — and that is the page for a robot. This one exists because the console needs
GStreamer and a rendered camera before it will load, and driving a simulated duck around a floor
needs neither.

**Why the method list and the Origin check.** This process holds a socket that can power the joints
and let the robot collapse. A browser will send a cross-site POST to localhost from any page it has
open, so a request is answered only when it carries JSON (which a cross-site form cannot send
without a preflight this server never answers) and, when the browser names an Origin, only when it
is this server's own. The method list keeps the page to driving: nothing here reaches the updater,
the wifi or the policy slots.
"""

import argparse
import json
import os
import socket
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PAGE = Path(__file__).with_name("index.html")

ALLOWED = {
    "robot.move",
    "robot.stop",
    "robot.enable",
    "robot.init",
    "robot.relax",
    "robot.do",
    "robot.sound",
    "robot.health",
    "robot.policies",
    "robot.mode",
}

# The state stream's rate. The page draws the trail and the numbers from it; 10 Hz is smooth to
# look at and a fifth of the loop's own rate, so the daemon is not asked to serialise every tick.
STATE_HZ = 10


def connect(path: str, timeout: float | None = 5.0) -> socket.socket:
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(timeout)
    s.connect(path)
    return s


class Handler(BaseHTTPRequestHandler):
    server_version = "duck-web"
    robot_socket = ""
    origins: set[str] = set()

    def log_message(self, format, *args):  # noqa: A002 — the base class's name
        # Driving is ten POSTs a second; logging each one buries anything worth reading.
        if not (self.command == "POST" and args and str(args[1]) in ("200", "204")):
            super().log_message(format, *args)

    def send_json(self, code: int, body) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            data = PAGE.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/events":
            self.stream_state()
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path != "/rpc":
            return self.send_error(404)
        origin = self.headers.get("Origin")
        if origin is not None and origin not in self.origins:
            return self.send_json(403, {"error": f"origin {origin} is not this page"})
        if not self.headers.get("Content-Type", "").startswith("application/json"):
            return self.send_json(415, {"error": "JSON only"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length) or b"{}")
            method = request["method"]
            params = request.get("params") or {}
            notify = bool(request.get("notify"))
        except (ValueError, KeyError, TypeError) as error:
            return self.send_json(400, {"error": f"bad request: {error}"})
        if method not in ALLOWED:
            return self.send_json(403, {"error": f"{method} is not a driving call"})

        message = {"jsonrpc": "2.0", "method": method, "params": params}
        if not notify:
            message["id"] = 1
        try:
            with connect(self.robot_socket) as s:
                s.sendall((json.dumps(message) + "\n").encode())
                if notify:
                    self.send_response(204)
                    self.end_headers()
                    return
                answer = json.loads(s.makefile("r").readline() or "null")
        except (OSError, ValueError) as error:
            return self.send_json(502, {"error": f"robotd at {self.robot_socket}: {error}"})
        if answer is None:
            return self.send_json(502, {"error": "robotd closed the connection without answering"})
        self.send_json(200, answer)

    def stream_state(self):
        """Server-sent events, one per `robot.state` notification."""
        try:
            s = connect(self.robot_socket, timeout=None)
        except OSError as error:
            return self.send_json(502, {"error": f"robotd at {self.robot_socket}: {error}"})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        with s:
            s.sendall((json.dumps({"jsonrpc": "2.0", "id": 1, "method": "robot.subscribe",
                                   "params": {"hz": STATE_HZ}}) + "\n").encode())
            try:
                for line in s.makefile("r"):
                    message = json.loads(line)
                    if message.get("method") != "robot.state":
                        continue
                    self.wfile.write(b"data: " + json.dumps(message["params"]).encode() + b"\n\n")
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass  # the page went away; the unix socket closes with the `with`


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--socket", default=os.path.expanduser("~/.cache/duck-sim/duck.sock"),
                        help="robotd's socket (default: the simulated duck's)")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    if not os.path.exists(args.socket):
        print(f"no robotd socket at {args.socket} — `scripts/duck-sim` first, or pass --socket",
              file=sys.stderr)
        return 1

    # Loopback only. The method list is a guard against a stray page, not an authorisation
    # scheme, and nothing here is fit to face a network.
    host = "127.0.0.1"
    Handler.robot_socket = args.socket
    Handler.origins = {f"http://{name}:{args.port}" for name in (host, "localhost")}
    server = ThreadingHTTPServer((host, args.port), Handler)
    server.daemon_threads = True
    print(f"duck-web: http://{host}:{args.port}  →  {args.socket}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
