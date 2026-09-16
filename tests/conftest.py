"""Shared helpers for the EdgeFaultLab test suite.

Everything here talks TCP for real: EdgeFaultLab is a proxy, so a test that
mocks the socket tests nothing that matters.
"""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEMO_SYSTEM = ROOT / "examples" / "demo_system"
if str(ROOT) not in sys.path:  # allows running pytest without installing
    sys.path.insert(0, str(ROOT))

from edgefaultlab.recorder import EventRecorder  # noqa: E402
from edgefaultlab.runner import ScenarioRunner  # noqa: E402
from edgefaultlab.scenario import (  # noqa: E402
    FaultSpec,
    Link,
    Scenario,
    load_scenario,
)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def free_ports(count: int) -> list[int]:
    """Reserve ``count`` distinct ports and release them again."""
    sockets = []
    ports: list[int] = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
            ports.append(int(sock.getsockname()[1]))
    finally:
        for sock in sockets:
            sock.close()
    return ports


async def wait_until(predicate, timeout: float = 3.0, interval: float = 0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return bool(predicate())


def make_link(name: str, listen_port: int, upstream_port: int) -> Link:
    return Link(
        name=name,
        listen_host="127.0.0.1",
        listen_port=listen_port,
        upstream_host="127.0.0.1",
        upstream_port=upstream_port,
    )


def make_fault(index: int, action: str, **kwargs) -> FaultSpec:
    return FaultSpec(index=index, action=action, **kwargs)


class Collector:
    """A TCP server that records what reaches it, like the far side of a link."""

    def __init__(self, reply: dict | None = None):
        self.received: list[dict] = []
        self.raw: list[bytes] = []
        self.connections = 0
        self.reply = reply
        self._server: asyncio.AbstractServer | None = None

    async def start(self, port: int) -> "Collector":
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", port)
        return self

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                payload = line.strip()
                if not payload:
                    continue
                self.raw.append(payload)
                try:
                    self.received.append(json.loads(payload))
                except json.JSONDecodeError:
                    self.received.append({"__malformed__": payload.decode("utf-8", "replace")})
                if self.reply is not None:
                    writer.write(json.dumps(self.reply).encode("utf-8") + b"\n")
                    await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            return
        finally:
            writer.close()


def write_scenario(tmp_path: Path, data: dict, name: str = "scenario.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return path


def run_scenario_file(
    path: Path, output: Path | None = None, *, quiet: bool = True
) -> tuple[int, ScenarioRunner]:
    """Run a scenario synchronously and hand back the runner for inspection."""
    scenario: Scenario = load_scenario(path)
    runner = ScenarioRunner(scenario, output=output, echo=None if quiet else print)
    exit_code = asyncio.run(runner.run())
    return exit_code, runner


def read_events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def silent_recorder(path: Path | None = None) -> EventRecorder:
    return EventRecorder(str(path) if path is not None else None)
