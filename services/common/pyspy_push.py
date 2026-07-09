"""Continuous profiling bridge: py-spy -> Grafana Pyroscope.

Runs as a sidecar container that shares the target service's PID namespace
(``pid: "service:<name>"`` in compose). In a loop it records ``PYSPY_INTERVAL``
seconds of stacks from the target process with py-spy, then POSTs the folded
output to Pyroscope's ``/ingest`` endpoint, where Grafana renders it as a flame
graph.

py-spy runs *out of process* and in ``--nonblocking`` mode, so it never holds
the target's GIL — the profiling overhead lands on this sidecar, not on the
event loop being measured. Env:

    PYROSCOPE_APP             app name in Pyroscope, e.g. "eyebench.gateway"
    SERVICE                   value for the {service=...} tag (optional)
    PYROSCOPE_SERVER_ADDRESS  default http://pyroscope:4040
    PYROSCOPE_SAMPLE_RATE     samples/sec passed to py-spy (default 100)
    PYSPY_INTERVAL            seconds per record/push cycle (default 10)
    TARGET_PID                pid to attach to in the shared namespace (default 1)
"""
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

APP = os.environ["PYROSCOPE_APP"]
SERVICE = os.getenv("SERVICE", "")
SERVER = os.getenv("PYROSCOPE_SERVER_ADDRESS", "http://pyroscope:4040").rstrip("/")
RATE = int(os.getenv("PYROSCOPE_SAMPLE_RATE", "100"))
INTERVAL = int(os.getenv("PYSPY_INTERVAL", "10"))
PID = os.getenv("TARGET_PID", "1")

OUT = "/tmp/pyspy.folded"
# Pyroscope's /ingest adapter reads the app-name suffix as the profile type, so
# the name MUST end in a known type (".cpu" for py-spy CPU samples); the app
# then shows up as APP with a "cpu" profile. Tags follow in the {k=v} block.
NAME = f"{APP}.cpu{{service={SERVICE}}}" if SERVICE else f"{APP}.cpu"


def _log(msg: str) -> None:
    print(f"[pyspy-push {APP}] {msg}", flush=True)


def _record() -> bytes:
    """Record one INTERVAL-long window and return folded stacks (may be empty)."""
    proc = subprocess.run(
        [
            "py-spy", "record",
            "--pid", PID,
            "--format", "raw",       # raw == collapsed/folded stacks
            "--rate", str(RATE),
            "--duration", str(INTERVAL),
            "--nonblocking",         # don't pause the target (keep it off the hot path)
            "--output", OUT,
        ],
        capture_output=True,
        text=True,
        timeout=INTERVAL + 30,
    )
    if proc.returncode != 0:
        _log(f"py-spy exited {proc.returncode}: {proc.stderr.strip()[:200]}")
        return b""
    try:
        with open(OUT, "rb") as fh:
            return fh.read()
    except FileNotFoundError:
        return b""


def _push(data: bytes, start: int, until: int) -> None:
    qs = urllib.parse.urlencode(
        {
            "name": NAME,
            "from": start,
            "until": until,
            "sampleRate": RATE,
            "spyName": "pyspy",
            "format": "folded",
            "units": "samples",
            "aggregationType": "sum",
        }
    )
    req = urllib.request.Request(f"{SERVER}/ingest?{qs}", data=data, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status not in (200, 204):
            _log(f"ingest returned HTTP {resp.status}")


def main() -> None:
    _log(f"profiling pid {PID} -> {SERVER} (rate={RATE}Hz interval={INTERVAL}s)")
    while True:
        start = int(time.time())
        try:
            data = _record()
        except subprocess.TimeoutExpired:
            _log("py-spy timed out; retrying")
            continue
        except Exception as exc:  # noqa: BLE001 — keep the sidecar alive
            _log(f"record error: {exc}")
            time.sleep(INTERVAL)
            continue

        if not data.strip():
            # Idle process (e.g. the worker between refreshes) samples to nothing
            # in --nonblocking mode; nothing to push this window.
            continue

        until = int(time.time())
        try:
            _push(data, start, until)
        except urllib.error.URLError as exc:
            _log(f"push failed: {exc}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
