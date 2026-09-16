"""Real TCP tests of the proxy: forwarding, every message fault, and the edges."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time

from edgefaultlab.faults import FaultEngine
from edgefaultlab.proxy import MAX_MESSAGE_SIZE, LinkProxy, MessageTooLarge, read_lines
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


async def collect_lines(data: bytes, max_size: int) -> tuple[list[bytes], Exception | None]:
    """Run read_lines over ``data`` and report what came out (and what blew up)."""
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    lines: list[bytes] = []
    try:
        async for line in read_lines(reader, max_size=max_size):
            lines.append(line)
    except MessageTooLarge as exc:
        return lines, exc
    return lines, None


def test_message_size_limit_is_exact_at_the_boundary():
    """The limit applies to the message, not to the framing around it."""

    async def scenario():
        limit = 64
        payload = b"x" * limit

        lines, error = await collect_lines(payload + b"\n", limit)
        assert lines == [payload] and error is None, "exactly max_size is allowed"

        lines, error = await collect_lines(b"x" * (limit + 1) + b"\n", limit)
        assert lines == [] and isinstance(error, MessageTooLarge), (
            "a framed message one byte over the limit must be rejected"
        )

        lines, error = await collect_lines(b"x" * (limit + 1), limit)
        assert lines == [] and isinstance(error, MessageTooLarge), (
            "an unframed message one byte over the limit must be rejected"
        )

        lines, error = await collect_lines(payload, limit)
        assert lines == [payload] and error is None, "a final EOF line may fill the limit"

        lines, error = await collect_lines(b"x" * (limit + 1), limit)
        assert lines == [] and isinstance(error, MessageTooLarge), (
            "an oversized final EOF line must be rejected"
        )

        lines, error = await collect_lines(b"ok\n" + b"y" * (limit + 1) + b"\n", limit)
        assert lines == [b"ok"] and isinstance(error, MessageTooLarge), (
            "a small line before an oversized one is still delivered"
        )

        lines, error = await collect_lines(b"z" * MAX_MESSAGE_SIZE + b"\n", MAX_MESSAGE_SIZE)
        assert lines == [b"z" * MAX_MESSAGE_SIZE] and error is None

        lines, error = await collect_lines(
            b"z" * (MAX_MESSAGE_SIZE + 1) + b"\n", MAX_MESSAGE_SIZE
        )
        assert lines == [] and isinstance(error, MessageTooLarge)

    asyncio.run(scenario())


def test_reorder_residual_never_leaves_its_own_link(tmp_path):
    """Two links share one fault engine; stopping one must not leak into the other."""

    async def scenario():
        listen_a, upstream_a = free_port(), free_port()
        listen_b, upstream_b = free_port(), free_port()
        collector_a = await Collector().start(upstream_a)
        collector_b = await Collector().start(upstream_b)

        recorder = EventRecorder()
        # No link on the fault, so it buffers on both links - each with its own key.
        engine = FaultEngine((make_fault(0, "reorder", match={"type": "PING"}),), recorder, 42)
        engine.start()
        proxy_a = LinkProxy(make_link("link_a", listen_a, upstream_a), engine, recorder)
        proxy_b = LinkProxy(make_link("link_b", listen_b, upstream_b), engine, recorder)
        await proxy_a.start()
        await proxy_b.start()

        writer_a = (await asyncio.open_connection("127.0.0.1", listen_a))[1]
        writer_b = (await asyncio.open_connection("127.0.0.1", listen_b))[1]
        writer_a.write(b'{"type":"PING","message_id":"a1"}\n')
        writer_b.write(b'{"type":"PING","message_id":"b1"}\n')
        await writer_a.drain()
        await writer_b.drain()
        await asyncio.sleep(0.2)
        assert collector_a.raw == [] and collector_b.raw == [], "both windows are half full"
        assert engine.buffered_messages == 2

        await proxy_a.stop()
        assert await wait_until(lambda: collector_a.raw)
        assert [message["message_id"] for message in collector_a.received] == ["a1"]
        assert collector_b.raw == [], "link B's residue must not ride along on link A"
        assert engine.buffered_messages == 1, "link B's buffer is still there"

        await proxy_b.stop()
        assert await wait_until(lambda: collector_b.raw)
        assert [message["message_id"] for message in collector_b.received] == ["b1"]
        assert [message["message_id"] for message in collector_a.received] == ["a1"]
        assert engine.buffered_messages == 0

        await collector_a.stop()
        await collector_b.stop()
        recorder.close()
        writer_b.close()

    asyncio.run(scenario())
