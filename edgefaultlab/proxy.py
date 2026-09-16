"""The TCP proxy: one link between two nodes, with faults injected in between.

    Node A  ->  EdgeFaultLab proxy  ->  Node B

The proxy speaks newline-delimited JSON.  It never parses the *semantics* of a
message - it decodes the JSON, hands it to the fault engine, and writes the
resulting bytes on.  Three rules keep it honest as a test tool:

* a message it cannot decode is recorded as ``MALFORMED_MESSAGE`` and forwarded
  **unchanged**; EdgeFaultLab must not be the thing that breaks the test,
* a line that grows past :data:`MAX_MESSAGE_SIZE` is recorded as
  ``MESSAGE_TOO_LARGE`` and the connection is closed safely, so memory is bounded,
* delay and reorder buffers are bounded too; overflowing one is an explicit
  error, never a silent out-of-memory.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any

from .faults import FaultEngine, ReorderedBatch
from .recorder import EventRecorder, encode_message
from .scenario import Link

__all__ = [
    "MAX_BUFFERED_BYTES",
    "MAX_BUFFERED_MESSAGES",
    "MAX_MESSAGE_SIZE",
    "BufferOverflow",
    "LinkProxy",
    "MessageTooLarge",
    "ProxyError",
    "read_lines",
]

#: A single JSON line may not exceed this (bytes).
MAX_MESSAGE_SIZE = 1024 * 1024

#: Delay + reorder buffers may hold at most this many messages...
MAX_BUFFERED_MESSAGES = 1000

#: ...and at most this many bytes.
MAX_BUFFERED_BYTES = 4 * 1024 * 1024

_READ_CHUNK = 65536
_DELAY_GRACE_SECONDS = 1.0
#: How long the listening socket may take to finish closing its handlers.
_SERVER_CLOSE_GRACE_SECONDS = 2.0


class ProxyError(Exception):
    """Base class for proxy problems that must be reported, not swallowed."""


class MessageTooLarge(ProxyError):
    """A line exceeded :data:`MAX_MESSAGE_SIZE` with no newline in sight."""

    def __init__(self, size: int, limit: int):
        self.size = size
        self.limit = limit
        super().__init__(f"message exceeds {limit} bytes (buffered {size} bytes without a newline)")


class BufferOverflow(ProxyError):
    """The delay/reorder buffer hit its bound."""

    def __init__(self, messages: int, byte_count: int):
        self.messages = messages
        self.byte_count = byte_count
        super().__init__(
            f"delay/reorder buffer overflow: {messages} messages / {byte_count} bytes "
            f"(limits: {MAX_BUFFERED_MESSAGES} messages, {MAX_BUFFERED_BYTES} bytes)"
        )


async def read_lines(
    reader: asyncio.StreamReader, max_size: int = MAX_MESSAGE_SIZE
) -> AsyncIterator[bytes]:
    """Yield newline terminated lines (without the newline) from ``reader``.

    Raises :class:`MessageTooLarge` as soon as one line grows past ``max_size``,
    which is what keeps a broken producer from eating all the memory.
    """
    buffer = bytearray()
    while True:
        chunk = await reader.read(_READ_CHUNK)
        if not chunk:
            if buffer:
                # A final line without a trailing newline is still a message.
                yield bytes(buffer)
            return
        buffer.extend(chunk)
        while True:
            index = buffer.find(b"\n")
            if index < 0:
                break
            line = bytes(buffer[:index])
            del buffer[: index + 1]
            yield line
        if len(buffer) > max_size:
            raise MessageTooLarge(len(buffer), max_size)


@dataclass
class _Delivery:
    """One message, ready to go out on the wire."""

    message: Any
    data: bytes
    delay_ms: float = 0.0
    copies: int = 0
    fault_ids: list[str] = field(default_factory=list)
    from_reorder: bool = False


class _Connection:
    """One proxied TCP connection: a client socket paired with an upstream one."""

    def __init__(self, connection_id: int, client, upstream):
        self.id = connection_id
        self.client = client
        self.upstream = upstream
        self.write_lock = asyncio.Lock()
        self.closed = False
        self.close_reason: str | None = None

    def close(self, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        self.close_reason = reason
        for writer in (self.client, self.upstream):
            try:
                writer.close()
            except Exception:  # pragma: no cover - closing is best effort
                pass


class LinkProxy:
    """Listens on ``link.listen``, dials ``link.upstream`` and injects faults."""

    def __init__(
        self,
        link: Link,
        engine: FaultEngine,
        recorder: EventRecorder,
        on_error: Callable[[Exception], None] | None = None,
    ):
        self.link = link
        self.engine = engine
        self.recorder = recorder
        self.on_error = on_error

        self.failures: list[Exception] = []
        self.connections_accepted = 0
        self.messages_forwarded = 0

        self._server: asyncio.AbstractServer | None = None
        self._connections: set[_Connection] = set()
        self._delayed: set[asyncio.Task] = set()
        self._down = False
        self._stopped = False
        self._buffered_messages = 0
        self._buffered_bytes = 0
        self._connection_ids = 0

    # -- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        try:
            self._server = await asyncio.start_server(
                self._on_client, self.link.listen_host, self.link.listen_port
            )
        except OSError as exc:
            raise ProxyError(
                f"link {self.link.name!r} cannot listen on {self.link.listen}: "
                f"{exc.strerror or exc} (is the port already in use?)"
            ) from exc
        self.recorder.event(
            "LINK_LISTENING",
            link=self.link.name,
            details={"listen": self.link.listen, "upstream": self.link.upstream},
        )

    async def stop(self) -> None:
        """Stop accepting, drain the buffers, close the sockets.

        The order matters: ``Server.wait_closed()`` only returns once every
        connection handler has finished, so the connections have to be closed
        *before* it is awaited, or a run would never end.
        """
        self._stopped = True
        server = self._server
        self._server = None
        if server is not None:
            server.close()
        await self.flush_reorder_buffers()
        await self._wait_for_delayed()
        self._close_connections("run finished")
        if server is not None:
            try:
                await asyncio.wait_for(
                    server.wait_closed(), timeout=_SERVER_CLOSE_GRACE_SECONDS
                )
            except asyncio.TimeoutError:
                self.recorder.event(
                    "LINK_CLOSE_TIMEOUT",
                    link=self.link.name,
                    details={"grace_seconds": _SERVER_CLOSE_GRACE_SECONDS},
                )
            except Exception as exc:  # pragma: no cover - platform dependent
                self.recorder.event(
                    "LINK_CLOSE_ERROR",
                    link=self.link.name,
                    details={"error": f"{type(exc).__name__}: {exc}"},
                )

    def report_failure(self, exc: Exception) -> None:
        self.failures.append(exc)
        if self.on_error is not None:
            self.on_error(exc)

    # -- link level faults ------------------------------------------------

    async def set_down(self, down: bool, reason: str = "link_down") -> None:
        """Make the link unusable (``link_down``) or usable again."""
        if self._down == down:
            return
        self._down = down
        if down:
            self.recorder.event(
                "LINK_DOWN", link=self.link.name, details={"reason": reason}
            )
            self._close_connections(reason)
        else:
            self.recorder.event("LINK_UP", link=self.link.name, details={"reason": reason})

    async def disconnect(self, reason: str = "disconnect") -> int:
        """Drop the TCP connections that are currently open on this link."""
        closed = self._close_connections(reason)
        self.recorder.event(
            "LINK_DISCONNECTED", link=self.link.name, details={"reason": reason, "connections": closed}
        )
        return closed

    @property
    def is_down(self) -> bool:
        return self._down

    @property
    def open_connections(self) -> int:
        return len(self._connections)

    def _close_connections(self, reason: str) -> int:
        connections = list(self._connections)
        for connection in connections:
            connection.close(reason)
        return len(connections)

    # -- connection handling ----------------------------------------------

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        if self._stopped:
            writer.close()
            return
        if self._down:
            self.recorder.event(
                "CONNECTION_REJECTED",
                link=self.link.name,
                details={"peer": _peer(peer), "reason": "link is down"},
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # pragma: no cover - best effort
                pass
            return

        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self.link.upstream_host, self.link.upstream_port
            )
        except OSError as exc:
            self.recorder.event(
                "CONNECTION_FAILED",
                link=self.link.name,
                details={
                    "peer": _peer(peer),
                    "upstream": self.link.upstream,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
            self.recorder.event(
                "CONNECTION_CLOSE",
                link=self.link.name,
                details={"reason": "upstream unreachable", "peer": _peer(peer)},
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # pragma: no cover - best effort
                pass
            return

        self._connection_ids += 1
        self.connections_accepted += 1
        connection = _Connection(self._connection_ids, writer, upstream_writer)
        self._connections.add(connection)
        self.recorder.event(
            "CONNECTION_OPEN",
            link=self.link.name,
            details={
                "connection": connection.id,
                "peer": _peer(peer),
                "upstream": self.link.upstream,
            },
        )

        try:
            await asyncio.gather(
                self._pump(connection, reader, upstream_writer, "forward"),
                self._pump(connection, upstream_reader, writer, "reverse"),
            )
        finally:
            connection.close("connection closed")
            self._connections.discard(connection)
            self.recorder.event(
                "CONNECTION_CLOSE",
                link=self.link.name,
                details={"connection": connection.id, "reason": connection.close_reason},
            )
            for pending in (writer, upstream_writer):
                try:
                    await pending.wait_closed()
                except Exception:  # pragma: no cover - best effort
                    pass

    async def _pump(
        self,
        connection: _Connection,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        direction: str,
    ) -> None:
        """Copy one direction of the conversation, applying faults on the way.

        When this direction ends cleanly the far side is told with a TCP
        half-close, so a peer that is waiting for a reply still gets one.  When
        it ends badly the whole proxied connection is dropped: a node whose
        upstream is gone must notice, or it would keep talking into a socket
        that goes nowhere.
        """
        try:
            async for line in read_lines(reader):
                if connection.closed:
                    break
                if not line.strip():
                    continue  # keepalive / blank line, nothing to forward
                await self._handle_line(connection, line, writer, direction)
        except MessageTooLarge as exc:
            self.recorder.event(
                "MESSAGE_TOO_LARGE",
                link=self.link.name,
                direction=direction,
                details={"size": exc.size, "limit": exc.limit, "connection": connection.id},
            )
            connection.close("message too large")
        except BufferOverflow as exc:
            self.recorder.event(
                "BUFFER_OVERFLOW",
                link=self.link.name,
                direction=direction,
                details={"error": str(exc), "connection": connection.id},
            )
            connection.close("buffer overflow")
            self.report_failure(exc)
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as exc:
            self.recorder.event(
                "CONNECTION_RESET",
                link=self.link.name,
                direction=direction,
                details={"error": type(exc).__name__, "connection": connection.id},
            )
            connection.close("connection reset by peer")
        except OSError as exc:
            self.recorder.event(
                "CONNECTION_ERROR",
                link=self.link.name,
                direction=direction,
                details={"error": f"{type(exc).__name__}: {exc}", "connection": connection.id},
            )
            connection.close(f"connection error: {exc}")
        finally:
            if not connection.closed:
                try:
                    writer.write_eof()
                except (OSError, RuntimeError, NotImplementedError):
                    connection.close("half-close not supported by this transport")

    # -- message handling --------------------------------------------------

    async def _handle_line(
        self,
        connection: _Connection,
        line: bytes,
        writer: asyncio.StreamWriter,
        direction: str,
    ) -> None:
        decoded = _decode(line)
        if decoded is None:
            raw, truncated = encode_message(line.decode("utf-8", "replace"))
            self.recorder.event(
                "MALFORMED_MESSAGE",
                link=self.link.name,
                direction=direction,
                details={
                    "error": _decode_error(line),
                    "raw": raw,
                    "truncated": truncated,
                    "connection": connection.id,
                },
            )
            # Forwarded byte for byte: the system under test sent it, it should
            # see the consequences of that.
            await self._send(connection, writer, _Delivery(message=None, data=line), direction)
            return

        self.recorder.event(
            "MESSAGE_RECEIVED",
            link=self.link.name,
            direction=direction,
            message=decoded,
            details={"connection": connection.id},
        )

        plan = self.engine.plan(decoded, link=self.link.name, direction=direction)
        data = (
            json.dumps(decoded, ensure_ascii=False).encode("utf-8")
            if plan.mutated
            else line
        )
        delivery = _Delivery(
            message=decoded,
            data=data,
            delay_ms=plan.delay_ms,
            copies=plan.copies,
            fault_ids=list(plan.fault_ids),
        )

        if plan.dropped:
            self.recorder.event(
                "MESSAGE_DROPPED",
                link=self.link.name,
                direction=direction,
                message=decoded,
                details={"fault_id": plan.dropped_by},
            )
            return

        if plan.copies:
            self.recorder.event(
                "MESSAGE_DUPLICATED",
                link=self.link.name,
                direction=direction,
                message=decoded,
                details={"fault_id": plan.fault_ids[-1] if plan.fault_ids else None,
                         "copies": plan.copies,
                         "total": plan.copies + 1},
            )

        if plan.reorder is not None:
            self._buffer(delivery)
            batch = plan.reorder.buffer((self.link.name, direction), (delivery, data))
            self.recorder.event(
                "MESSAGE_REORDER_BUFFERED",
                link=self.link.name,
                direction=direction,
                message=decoded,
                details={"fault_id": plan.reorder.id,
                         "window": plan.reorder_window},
            )
            if batch is not None:
                await self._release(batch, writer, direction)
            return

        if plan.delay_ms:
            self.recorder.event(
                "MESSAGE_DELAYED",
                link=self.link.name,
                direction=direction,
                message=decoded,
                details={"delay_ms": plan.delay_ms, "fault_id": plan.fault_ids[-1] if plan.fault_ids else None},
            )
        await self._deliver(connection, writer, delivery, direction)

    async def _release(
        self, batch: ReorderedBatch, writer: asyncio.StreamWriter, direction: str
    ) -> None:
        """Send a released reorder window in the new order."""
        for delivery, _raw in batch.items:
            self._unbuffer(len(delivery.data))
        self.recorder.event(
            "MESSAGE_REORDERED",
            link=self.link.name,
            direction=direction,
            details={"fault_id": batch.fault_id, "messages": len(batch.items)},
        )
        for delivery, _raw in batch.items:
            delivery.from_reorder = True
            await self._deliver(None, writer, delivery, direction)

    async def flush_reorder_buffers(self) -> None:
        """Release partially filled reorder windows at the end of a run.

        A window that never filled is sent on in its original order: EdgeFaultLab
        does not get to keep a message hostage just because the run ended.
        """
        batches = self.engine.flush()
        if not batches:
            return
        for batch in batches:
            _link_name, direction = batch.key
            writer = self._writer_for(direction)
            for delivery, _raw in batch.items:
                self._unbuffer(len(delivery.data))
                if writer is None:
                    self.recorder.event(
                        "MESSAGE_FORWARD_FAILED",
                        link=self.link.name,
                        direction=direction,
                        message=delivery.message,
                        details={
                            "fault_id": batch.fault_id,
                            "reason": "reorder buffer flushed with no open connection",
                        },
                    )
                    continue
                self.recorder.event(
                    "REORDER_BUFFER_FLUSHED",
                    link=self.link.name,
                    direction=direction,
                    message=delivery.message,
                    details={"fault_id": batch.fault_id},
                )
                delivery.from_reorder = True
                await self._send(None, writer, delivery, direction)

    def _writer_for(self, direction: str) -> asyncio.StreamWriter | None:
        """The writer of a still-open connection for ``direction``, if any."""
        for connection in list(self._connections):
            if connection.closed:
                continue
            writer = connection.upstream if direction == "forward" else connection.client
            if not writer.is_closing():
                return writer
        return None

    async def _deliver(
        self,
        connection: _Connection | None,
        writer: asyncio.StreamWriter,
        delivery: _Delivery,
        direction: str,
    ) -> None:
        if delivery.delay_ms > 0:
            self._buffer(delivery)
            task = asyncio.create_task(
                self._delayed_send(writer, delivery, direction, delivery.delay_ms / 1000.0)
            )
            self._delayed.add(task)
            task.add_done_callback(self._delayed.discard)
            return
        await self._send(connection, writer, delivery, direction)

    async def _delayed_send(
        self, writer: asyncio.StreamWriter, delivery: _Delivery, direction: str, delay: float
    ) -> None:
        try:
            await asyncio.sleep(delay)
            await self._send(None, writer, delivery, direction)
        finally:
            self._unbuffer(len(delivery.data))

    async def _send(
        self,
        connection: _Connection | None,
        writer: asyncio.StreamWriter,
        delivery: _Delivery,
        direction: str,
    ) -> None:
        total = 1 + delivery.copies
        for copy_index in range(total):
            if writer.is_closing():
                self.recorder.event(
                    "MESSAGE_FORWARD_FAILED",
                    link=self.link.name,
                    direction=direction,
                    message=delivery.message,
                    details={"copy": copy_index, "reason": "connection closed"},
                )
                if connection is not None:
                    connection.close("forwarding peer is gone")
                return
            try:
                async with self._write_lock(connection, writer):
                    writer.write(delivery.data + b"\n")
                    await writer.drain()
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, OSError) as exc:
                self.recorder.event(
                    "MESSAGE_FORWARD_FAILED",
                    link=self.link.name,
                    direction=direction,
                    message=delivery.message,
                    details={"copy": copy_index, "reason": f"{type(exc).__name__}: {exc}"},
                )
                if connection is not None:
                    connection.close("forwarding peer is gone")
                return
            self.messages_forwarded += 1
            self.recorder.event(
                "MESSAGE_FORWARDED",
                link=self.link.name,
                direction=direction,
                message=delivery.message,
                details={
                    "copy": copy_index,
                    "faults": delivery.fault_ids,
                    "from_reorder": delivery.from_reorder,
                },
            )
            if delivery.message is not None:
                self.recorder.observe(
                    delivery.message, link=self.link.name, direction=direction
                )

    def _write_lock(self, connection: _Connection | None, writer: asyncio.StreamWriter):
        if connection is not None:
            return connection.write_lock
        return _NULL_LOCK

    # -- bounded buffers ---------------------------------------------------

    def _buffer(self, delivery: _Delivery) -> None:
        size = len(delivery.data)
        if (
            self._buffered_messages + 1 > MAX_BUFFERED_MESSAGES
            or self._buffered_bytes + size > MAX_BUFFERED_BYTES
        ):
            raise BufferOverflow(self._buffered_messages + 1, self._buffered_bytes + size)
        self._buffered_messages += 1
        self._buffered_bytes += size

    def _unbuffer(self, size: int) -> None:
        self._buffered_messages = max(0, self._buffered_messages - 1)
        self._buffered_bytes = max(0, self._buffered_bytes - size)

    async def _wait_for_delayed(self) -> None:
        if not self._delayed:
            return
        pending = list(self._delayed)
        done, still_pending = await asyncio.wait(pending, timeout=_DELAY_GRACE_SECONDS)
        for task in still_pending:
            task.cancel()
        if still_pending:
            self.recorder.event(
                "DELAY_BUFFER_DISCARDED",
                link=self.link.name,
                details={"messages": len(still_pending), "reason": "run finished"},
            )


class _NullLock:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc_info):
        return False


_NULL_LOCK = _NullLock()


def _decode(line: bytes) -> dict[str, Any] | None:
    try:
        value = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _decode_error(line: bytes) -> str:
    try:
        value = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return f"{type(exc).__name__}: {exc}"
    return f"JSON value is {type(value).__name__}, expected an object"


def _peer(peer: Any) -> str:
    if isinstance(peer, tuple) and len(peer) >= 2:
        return f"{peer[0]}:{peer[1]}"
    return str(peer)
