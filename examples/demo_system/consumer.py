"""Consumer node of the demo system: executes commands, exactly once.

    python consumer.py --port 9601

Behaviour
---------
``CONTROL_COMMAND`` is answered with ``ACK`` (received) and later with
``CONTROL_RESULT`` (executed).  Both are separate messages on purpose: a system
that conflates "I got it" with "I did it" cannot tell a lost result from a lost
command.

The node is idempotent: a ``command_id`` is executed at most once, and a repeat
is answered with ``status: REJECTED, reason: DUPLICATE``.  A command whose
``timestamp`` is older than ``--ttl`` seconds is refused with
``reason: EXPIRED``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

CONTROL_COMMAND = "CONTROL_COMMAND"
CONTROL_RESULT = "CONTROL_RESULT"
ACK = "ACK"


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} [consumer] {message}", flush=True)


class Consumer:
    def __init__(self, port: int, execute_seconds: float, ttl: float):
        self.port = port
        self.execute_seconds = execute_seconds
        self.ttl = ttl
        self.executed: set[str] = set()
        self.executions = 0
        self.rejected_expired = 0
        self.rejected_duplicate = 0

    async def start(self) -> None:
        server = await asyncio.start_server(self.handle, "127.0.0.1", self.port)
        log(f"listening on 127.0.0.1:{self.port}")
        async with server:
            await server.serve_forever()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        log("producer connected")
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as exc:
                    log(f"ignoring malformed message: {exc}")
                    continue
                await self.on_message(message, writer)
        except (ConnectionResetError, BrokenPipeError):
            log("connection reset by peer")
        finally:
            writer.close()
            log("producer disconnected")

    async def on_message(self, message: dict, writer: asyncio.StreamWriter) -> None:
        if message.get("type") != CONTROL_COMMAND:
            return
        payload = message.get("payload") or {}
        command_id = str(payload.get("command_id"))
        requester = message.get("source", "?")

        age = time.time() - float(message.get("timestamp", time.time()))
        if age > self.ttl:
            self.rejected_expired += 1
            log(f"command_id={command_id} refused: age {age:.1f}s > ttl {self.ttl:.1f}s")
            await self.send(
                writer,
                CONTROL_RESULT,
                requester,
                {"command_id": command_id, "status": "REJECTED", "reason": "EXPIRED"},
            )
            return

        if command_id in self.executed:
            self.rejected_duplicate += 1
            log(f"command_id={command_id} refused: duplicate (idempotency)")
            await self.send(
                writer,
                CONTROL_RESULT,
                requester,
                {"command_id": command_id, "status": "REJECTED", "reason": "DUPLICATE"},
            )
            return

        self.executed.add(command_id)
        await self.send(writer, ACK, requester, {"command_id": command_id})
        await asyncio.sleep(self.execute_seconds)
        self.executions += 1
        log(f"command_id={command_id} executed (executions={self.executions})")
        await self.send(
            writer,
            CONTROL_RESULT,
            requester,
            {
                "command_id": command_id,
                "status": "EXECUTED",
                "duration_s": self.execute_seconds,
            },
        )

    async def send(
        self, writer: asyncio.StreamWriter, message_type: str, target: str, payload: dict
    ) -> None:
        envelope = {
            "type": message_type,
            "source": "C1",
            "target": target,
            "timestamp": time.time(),
            "message_id": f"{message_type.lower()}-{payload.get('command_id')}",
            "payload": payload,
        }
        writer.write(json.dumps(envelope).encode("utf-8") + b"\n")
        await writer.drain()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="EdgeFaultLab demo consumer node")
    parser.add_argument("--port", type=int, default=9601)
    parser.add_argument("--execute-seconds", type=float, default=0.4)
    parser.add_argument("--ttl", type=float, default=10.0, help="command TTL in seconds")
    args = parser.parse_args(argv)
    try:
        asyncio.run(Consumer(args.port, args.execute_seconds, args.ttl).start())
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
