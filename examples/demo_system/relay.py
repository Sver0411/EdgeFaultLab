"""Relay node of the demo system: a gateway that forwards to the cloud side.

    python relay.py --listen 127.0.0.1:9600 --upstream 127.0.0.1:9601

The relay is deliberately unambitious: it forwards newline JSON in both
directions and reconnects to its upstream when that connection fails.  It is
also the process that scenarios kill with ``process_kill``, because a gateway
that dies is the failure mode worth testing.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} [relay] {message}", flush=True)


def parse_address(value: str) -> tuple[str, int]:
    host, _, port = value.rpartition(":")
    return host or "127.0.0.1", int(port)


class Relay:
    def __init__(self, listen: tuple[str, int], upstream: tuple[str, int], connect_timeout: float):
        self.listen = listen
        self.upstream = upstream
        self.connect_timeout = connect_timeout
        self.forwarded = 0

    async def start(self) -> None:
        server = await asyncio.start_server(self.handle, self.listen[0], self.listen[1])
        log(f"listening on {self.listen[0]}:{self.listen[1]}, upstream "
            f"{self.upstream[0]}:{self.upstream[1]}")
        async with server:
            await server.serve_forever()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        log("node connected")
        upstream = await self.connect_upstream()
        if upstream is None:
            log("giving up: upstream unreachable")
            writer.close()
            return
        up_reader, up_writer = upstream
        try:
            await asyncio.gather(
                self.pump(reader, up_writer, "downstream->upstream"),
                self.pump(up_reader, writer, "upstream->downstream"),
            )
        finally:
            for pending in (writer, up_writer):
                pending.close()
            log("node disconnected")

    async def connect_upstream(self):
        deadline = time.monotonic() + self.connect_timeout
        while time.monotonic() < deadline:
            try:
                reader, writer = await asyncio.open_connection(*self.upstream)
                log("upstream connected")
                return reader, writer
            except OSError as exc:
                log(f"upstream not ready ({exc.__class__.__name__}), retrying")
                await asyncio.sleep(0.5)
        return None

    async def pump(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, label: str
    ) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                if not line.strip():
                    continue
                writer.write(line if line.endswith(b"\n") else line + b"\n")
                await writer.drain()
                self.forwarded += 1
                self.describe(label, line)
        except (ConnectionResetError, BrokenPipeError):
            log(f"{label} connection reset")

    def describe(self, label: str, line: bytes) -> None:
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            log(f"{label} malformed message forwarded")
            return
        log(
            f"{label} {message.get('type')} "
            f"{(message.get('payload') or {}).get('command_id', '')}".rstrip()
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="EdgeFaultLab demo relay node")
    parser.add_argument("--listen", default="127.0.0.1:9600")
    parser.add_argument("--upstream", default="127.0.0.1:9601")
    parser.add_argument("--connect-timeout", type=float, default=30.0)
    args = parser.parse_args(argv)
    relay = Relay(
        parse_address(args.listen), parse_address(args.upstream), args.connect_timeout
    )
    try:
        asyncio.run(relay.start())
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
