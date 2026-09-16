"""The event trace: everything EdgeFaultLab saw, in order, on one timeline.

Every run writes ``runs/<run_id>/events.jsonl`` - one JSON object per line::

    {"time": 5.214, "event": "MESSAGE_DROPPED", "link": "a_to_b",
     "direction": "forward", "message_id": "msg-abc", "details": {}}

``time`` is seconds since the run started.  The recorder also keeps the same
events in memory, because the assertion engine and the report both need to look
at the whole run at once.

The recorder is the only place that decides how much of a message is worth
keeping: message bodies are truncated at 16 KB and flagged, so that a scenario
forwarding video-sized frames does not fill the disk.
"""

from __future__ import annotations

import json
import time
from collections import Counter
from collections.abc import Callable, Mapping
from typing import Any

__all__ = [
    "MAX_RECORDED_MESSAGE_BYTES",
    "SUMMARY_COUNTERS",
    "EventRecorder",
    "encode_message",
]

#: Message bodies larger than this are truncated in the event trace.
MAX_RECORDED_MESSAGE_BYTES = 16 * 1024

_FLUSH_EVERY = 64

#: Fields copied out of a message so the trace stays greppable without
#: re-reading the (possibly truncated) body.
_METADATA_FIELDS = ("type", "source", "target", "message_id")
_PAYLOAD_FIELDS = ("command_id", "status", "reason")

#: Event name -> key used in ``summary.json``.
SUMMARY_COUNTERS: dict[str, str] = {
    "messages_received": "MESSAGE_RECEIVED",
    "messages_forwarded": "MESSAGE_FORWARDED",
    "messages_dropped": "MESSAGE_DROPPED",
    "messages_delayed": "MESSAGE_DELAYED",
    "messages_duplicated": "MESSAGE_DUPLICATED",
    "messages_reordered": "MESSAGE_REORDERED",
    "messages_mutated": "MESSAGE_MUTATED",
    "malformed_messages": "MALFORMED_MESSAGE",
    "messages_too_large": "MESSAGE_TOO_LARGE",
    "connections_opened": "CONNECTION_OPEN",
    "connections_closed": "CONNECTION_CLOSE",
    "faults_activated": "FAULT_ACTIVATED",
    "faults_completed": "FAULT_COMPLETED",
    "messages_matched": "MESSAGE_MATCHED",
    "process_starts": "PROCESS_START",
    "process_exits": "PROCESS_EXIT",
    "process_kills": "PROCESS_KILL",
    "process_restarts": "PROCESS_RESTART",
}


def encode_message(message: Any, limit: int = MAX_RECORDED_MESSAGE_BYTES) -> tuple[str, bool]:
    """Serialise ``message``, truncated to ``limit`` bytes.

    Returns ``(text, truncated)``.  Truncation happens on a byte boundary and is
    always reported, never silent.
    """
    try:
        raw = json.dumps(message, ensure_ascii=False, default=repr)
    except (TypeError, ValueError):  # pragma: no cover - json handles the rest
        raw = repr(message)
    encoded = raw.encode("utf-8")
    if len(encoded) <= limit:
        return raw, False
    return encoded[:limit].decode("utf-8", "ignore"), True


def message_metadata(message: Any) -> dict[str, Any]:
    """Small, greppable summary of a decoded message (schema-independent)."""
    if not isinstance(message, Mapping):
        return {}
    meta: dict[str, Any] = {}
    for key in _METADATA_FIELDS:
        if key in message and isinstance(message[key], (str, int, float, bool)):
            meta[key] = message[key]
    payload = message.get("payload")
    if isinstance(payload, Mapping):
        for key in _PAYLOAD_FIELDS:
            if key in payload and isinstance(payload[key], (str, int, float, bool)):
                meta[f"payload.{key}"] = payload[key]
    return meta


class EventRecorder:
    """Append-only event log plus the counters the summary is built from."""

    def __init__(
        self,
        path: str | None = None,
        echo: Callable[[dict], None] | None = None,
        start_time: float | None = None,
    ):
        self.path = path
        self.echo = echo
        self.start = time.monotonic() if start_time is None else start_time
        self.events: list[dict[str, Any]] = []
        #: Messages as the peer actually received them, used by assertions.
        self.observations: list[dict[str, Any]] = []
        self.counters: Counter[str] = Counter()
        self._file = open(path, "w", encoding="utf-8") if path else None
        self._since_flush = 0

    # -- timeline ---------------------------------------------------------

    def now(self) -> float:
        """Seconds since this run started."""
        return time.monotonic() - self.start

    def event(
        self,
        event: str,
        *,
        link: str | None = None,
        direction: str | None = None,
        message_id: str | None = None,
        message: Any = None,
        details: Mapping[str, Any] | None = None,
        at: float | None = None,
    ) -> dict[str, Any]:
        """Record one event and return the written record."""
        record: dict[str, Any] = {"time": round(self.now() if at is None else at, 6), "event": event}
        if link is not None:
            record["link"] = link
        if direction is not None:
            record["direction"] = direction
        if message is not None:
            meta = message_metadata(message)
            if message_id is None:
                message_id = meta.get("message_id")
            body, truncated = encode_message(message)
            merged = dict(meta)
            merged["message"] = body
            if truncated:
                merged["truncated"] = True
            merged.update(details or {})
            details = merged
        if message_id is not None:
            record["message_id"] = message_id
        if details:
            record["details"] = dict(details)

        self.counters[event] += 1
        self.events.append(record)
        if self._file is not None:
            self._file.write(json.dumps(record, ensure_ascii=False, default=repr) + "\n")
            self._since_flush += 1
            if self._since_flush >= _FLUSH_EVERY:
                self._file.flush()
                self._since_flush = 0
        if self.echo is not None:
            self.echo(record)
        return record

    def observe(
        self,
        message: Any,
        *,
        link: str | None = None,
        direction: str | None = None,
        at: float | None = None,
    ) -> dict[str, Any]:
        """Note that ``message`` reached the far side of a link.

        Assertions only ever see observations: a message that the proxy dropped
        did not arrive, so it must not count as "delivered".  Delayed messages
        are observed when they actually arrive, not when they were written.
        """
        observation = {
            "time": round(self.now() if at is None else at, 6),
            "message": message,
            "link": link,
            "direction": direction,
        }
        self.observations.append(observation)
        return observation

    # -- queries ----------------------------------------------------------

    def of_type(self, event: str) -> list[dict[str, Any]]:
        return [record for record in self.events if record["event"] == event]

    def first_time(self, event: str) -> float | None:
        for record in self.events:
            if record["event"] == event:
                return record["time"]
        return None

    def count(self, event: str) -> int:
        return self.counters.get(event, 0)

    def counters_summary(self) -> dict[str, int]:
        return {key: self.count(event) for key, event in SUMMARY_COUNTERS.items()}

    @property
    def duration(self) -> float:
        return self.events[-1]["time"] if self.events else 0.0

    def close(self) -> None:
        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None
