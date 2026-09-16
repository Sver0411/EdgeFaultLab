"""Real TCP tests of the proxy: forwarding, every message fault, and the edges."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time

from edgefaultlab.faults import FaultEngine
from edgefaultlab.proxy import MAX_MESSAGE_SIZE, LinkProxy
from edgefaultlab.recorder import EventRecorder
from tests.conftest import Collector, free_port, make_fault, make_link, wait_until


@contextlib.asynccontextmanager
async def proxied(tmp_path, faults=(), reply=None):
    """A link with a collector on the far side, both on free ports."""
    listen_port, upstream_port = free_port(), free_port()
    collector = Collector(reply=reply)
    await collector.start(upstream_port)
    recorder = EventRecorder()
    engine = FaultEngine(tuple(faults), recorder, 42)
    engine.start()
    proxy = LinkProxy(make_link("l", listen_port, upstream_port), engine, recorder)
    await proxy.start()
    try:
        yield listen_port, collector, recorder, proxy
    finally:
        await proxy.stop()
        await collector.stop()
        recorder.close()


def test_messages_are_forwarded_in_both_directions(tmp_path):
    async def scenario():
        async with proxied(tmp_path, reply={"type": "PONG", "message_id": "reply-1"}) as (
            port,
            collector,
            recorder,
            _proxy,
        ):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b'{"type":"PING","message_id":"m1"}\n')
            await writer.drain()

            forward = await asyncio.wait_for(reader.readline(), timeout=3)
            assert json.loads(forward)["type"] == "PONG", "the reverse direction works"
            assert await wait_until(lambda: collector.received)
            assert collector.received[0]["message_id"] == "m1"
            assert recorder.count("MESSAGE_FORWARDED") == 2
            assert len(recorder.observations) == 2

            writer.close()

    asyncio.run(scenario())


def test_drop_removes_one_message_and_lets_the_next_one_through(tmp_path):
    async def scenario():
        faults = [make_fault(0, "drop", link="l", match={"type": "PING"}, count=1)]
        async with proxied(tmp_path, faults) as (port, collector, recorder, _proxy):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b'{"type":"PING","message_id":"m1"}\n')
            await writer.drain()
            assert not await wait_until(lambda: collector.received, timeout=0.4)

            writer.write(b'{"type":"PING","message_id":"m2"}\n')
            await writer.drain()
            assert await wait_until(lambda: collector.received)
            assert [m["message_id"] for m in collector.received] == ["m2"]
            assert recorder.count("MESSAGE_DROPPED") == 1
            assert recorder.of_type("MESSAGE_DROPPED")[0]["message_id"] == "m1"
            writer.close()

    asyncio.run(scenario())


def test_delay_holds_a_message_back_and_then_sends_it(tmp_path):
    async def scenario():
        faults = [
            make_fault(
                0,
                "delay",
                link="l",
                match={"type": "PING"},
                delay_ms=400,
                count=1,
            )
        ]
        async with proxied(tmp_path, faults) as (port, collector, recorder, _proxy):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            start = time.monotonic()
            started_at = recorder.now()
            writer.write(b'{"type":"PING","message_id":"m1"}\n')
            await writer.drain()
            assert await wait_until(lambda: collector.received, timeout=3)
            elapsed = time.monotonic() - start
            assert elapsed >= 0.35, f"message arrived after only {elapsed:.3f}s"
            assert recorder.count("MESSAGE_DELAYED") == 1
            assert recorder.observations[0]["time"] - started_at >= 0.35
            writer.close()

    asyncio.run(scenario())


def test_duplicate_sends_the_same_message_twice(tmp_path):
    async def scenario():
        faults = [
            make_fault(
                0, "duplicate", link="l", match={"type": "PING"}, copies=1, count=1
            )
        ]
        async with proxied(tmp_path, faults) as (port, collector, recorder, _proxy):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            body = b'{"type":"PING","message_id":"m1","payload":{"command_id":"c1"}}\n'
            writer.write(body)
            await writer.drain()
            assert await wait_until(lambda: len(collector.raw) >= 2)
            assert collector.raw[0] == collector.raw[1] == body.strip()
            assert recorder.count("MESSAGE_DUPLICATED") == 1
            writer.close()

    asyncio.run(scenario())


def test_malformed_and_oversized_messages_are_handled_explicitly(tmp_path):
    async def scenario():
        async with proxied(tmp_path) as (port, collector, recorder, _proxy):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"this is not json\n")
            await writer.drain()
            assert await wait_until(lambda: collector.raw)
            assert collector.raw[0] == b"this is not json"
            assert recorder.count("MALFORMED_MESSAGE") == 1
            assert recorder.count("MESSAGE_FORWARDED") == 1

            oversized_reader, oversized_writer = await asyncio.open_connection(
                "127.0.0.1", port
            )
            oversized_writer.write(b"x" * (MAX_MESSAGE_SIZE + 128))
            await oversized_writer.drain()
            assert (
                await asyncio.wait_for(oversized_reader.read(64), timeout=5) == b""
            ), "the proxy must close the connection instead of buffering forever"
            assert recorder.count("MESSAGE_TOO_LARGE") == 1
            assert collector.raw == [b"this is not json"]
            oversized_writer.close()
            writer.close()

    asyncio.run(scenario())


def test_link_down_rejects_new_connections_and_disconnect_drops_the_open_one(tmp_path):
    async def scenario():
        async with proxied(tmp_path) as (port, collector, recorder, proxy):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b'{"type":"PING","message_id":"m1"}\n')
            await writer.drain()
            assert await wait_until(lambda: collector.received)

            assert await proxy.disconnect("test") == 1
            assert await asyncio.wait_for(reader.read(64), timeout=3) == b""
            assert recorder.count("LINK_DISCONNECTED") == 1

            await proxy.set_down(True)
            rejected_reader, rejected_writer = await asyncio.open_connection("127.0.0.1", port)
            assert await asyncio.wait_for(rejected_reader.read(64), timeout=3) == b""
            rejected_writer.close()
            assert recorder.count("CONNECTION_REJECTED") == 1

            await proxy.set_down(False)
            recovered_reader, recovered_writer = await asyncio.open_connection(
                "127.0.0.1", port
            )
            recovered_writer.write(b'{"type":"PING","message_id":"m2"}\n')
            await recovered_writer.drain()
            assert await wait_until(lambda: len(collector.received) == 2)
            assert recorder.count("LINK_DOWN") == 1 and recorder.count("LINK_UP") == 1
            recovered_writer.close()
            writer.close()

    asyncio.run(scenario())
