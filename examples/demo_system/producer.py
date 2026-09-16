"""Producer node of the demo system: sends commands and retries until ACKed.

    python producer.py --connect 127.0.0.1:9501

The producer reconnects when its connection dies, and retries a command with
the *same* ``message_id`` and ``command_id`` - that is what a real gateway does,
and it is also what makes "executed at most once" a property worth asserting.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

CONTROL_COMMAND = "CONTROL_COMMAND"
ACK = "ACK"
CONTROL_RESULT = "CONTROL_RESULT"


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} [producer] {message}", flush=True)


def parse_address(value: str) -> tuple[str, int]:
    host, _, port = value.rpartition(":")
    return host or "127.0.0.1", int(port)


class Producer:
    def __init__(self, target: tuple[str, int], args: argparse.Namespace):
        self.target = target
        self.interval = args.interval
        self.ack_timeout = args.ack_timeout
        self.retries = args.retries
        self.start_delay = args.start_delay
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.reader_task: asyncio.Task | None = None
        self.sent = 0
        self.acked = 0
        self.retried = 0
        self.executed = 0
        self.rejected = 0

    async def run(self) -> None:
        await asyncio.sleep(self.start_delay)
        index = 0
        while True:
            index += 1
            command = self.build_command(index)
            await self.deliver(command)
            await asyncio.sleep(self.interval)

    def build_command(self, index: int) -> dict:
        return {
            "type": CONTROL_COMMAND,
            "source": "B1",
            "target": "C1",
            "timestamp": time.time(),
            "message_id": f"msg-{index:04d}",
            "payload": {
                "command_id": f"cmd-{index:04d}",
                "action": "irrigate",
                "duration_s": 1,
            },
        }

    async def deliver(self, command: dict) -> None:
        command_id = command["payload"]["command_id"]
        for attempt in range(1, self.retries + 1):
            if not await self.ensure_connection():
                await asyncio.sleep(0.5)
                continue
            if attempt > 1:
                self.retried += 1
                log(f"retry {attempt}/{self.retries} for {command_id}")
            try:
                command["timestamp"] = time.time()
                self.writer.write(json.dumps(command).encode("utf-8") + b"\n")
                await self.writer.drain()
                self.sent += 1
            except OSError as exc:
                log(f"send failed ({exc.__class__.__name__}), reconnecting")
                await self.drop_connection()
                continue
            if await self.wait_for_ack(command_id):
                return
            log(f"no ACK for {command_id} within {self.ack_timeout}s")
        log(f"giving up on {command_id} after {self.retries} attempts")

    async def ensure_connection(self) -> bool:
        if self.reader_task is not None and self.reader_task.done():
            # The reader loop already saw the connection go away.
            self.writer = None
            self.reader_task = None
        if self.writer is not None and not self.writer.is_closing():
            return True
        try:
            self.reader, self.writer = await asyncio.open_connection(*self.target)
        except OSError as exc:
            log(f"cannot reach {self.target[0]}:{self.target[1]} ({exc.__class__.__name__})")
            return False
        self.reader_task = asyncio.create_task(self.read_loop())
        log(f"connected to {self.target[0]}:{self.target[1]}")
        return True

    async def drop_connection(self) -> None:
        if self.reader_task is not None:
            self.reader_task.cancel()
            self.reader_task = None
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        self.reader = None

    async def read_loop(self) -> None:
        assert self.reader is not None
        try:
            while True:
                line = await self.reader.readline()
                if not line:
                    raise ConnectionError("peer closed the connection")
                line = line.strip()
                if not line:
                    continue
                try:
                    self.inbox.put_nowait(json.loads(line))
                except json.JSONDecodeError:
                    log("ignoring malformed message from peer")
        except (ConnectionError, OSError):
            log("connection lost")
            if self.writer is not None:
                self.writer.close()
            self.writer = None

    async def wait_for_ack(self, command_id: str) -> bool:
        deadline = time.monotonic() + self.ack_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            try:
                message = await asyncio.wait_for(self.inbox.get(), timeout=remaining)
            except asyncio.TimeoutError:
                return False
            self.describe(message)
            payload = message.get("payload") or {}
            if str(payload.get("command_id")) != command_id:
                continue
            if message.get("type") == ACK:
                self.acked += 1
                return True
            if message.get("type") == CONTROL_RESULT:
                if payload.get("status") == "EXECUTED":
                    self.executed += 1
                else:
                    self.rejected += 1
                return True

    def describe(self, message: dict) -> None:
        payload = message.get("payload") or {}
        log(
            f"received {message.get('type')} {payload.get('command_id', '')} "
            f"{payload.get('status', '')}".rstrip()
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="EdgeFaultLab demo producer node")
    parser.add_argument("--connect", default="127.0.0.1:9501")
    parser.add_argument("--interval", type=float, default=3.0, help="seconds between commands")
    parser.add_argument("--ack-timeout", type=float, default=1.5)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--start-delay", type=float, default=1.5)
    args = parser.parse_args(argv)
    producer = Producer(parse_address(args.connect), args)
    try:
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
