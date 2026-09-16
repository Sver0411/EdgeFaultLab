"""Tiny JSON field matcher.

EdgeFaultLab never hard codes a message schema. A scenario says which JSON
field has to equal which value, and this module answers that one question::

    {"type": "CONTROL_COMMAND", "payload.status": "EXECUTED"}

Supported syntax, and nothing more:

* a top level key: ``"type"``
* a dotted path: ``"payload.command_id"``
* a nested object, which is matched as a subset: ``{"payload": {"status": "EXECUTED"}}``
* equality of the JSON value against the expected value

Deliberately *not* supported: JSONPath, JMESPath, regular expressions, SQL-like
filters, wildcards. Those all start small and end up as a second language that
the scenario author has to learn.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

__all__ = ["MISSING", "get_field", "matches", "describe"]

_MISSING = object()

#: Sentinel returned by :func:`get_field` when a path does not exist.  It is
#: distinct from ``None`` so that ``{"payload.reason": null}`` can be matched.
MISSING = _MISSING


def get_field(message: Any, path: str) -> Any:
    """Return the value of ``path`` inside ``message``, or :data:`MISSING`.

    ``path`` is split on dots, so ``"payload.command_id"`` walks
    ``message["payload"]["command_id"]``.  Numeric segments address lists.
    """
    current = message
    for part in path.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                return MISSING
            current = current[part]
        elif isinstance(current, (list, tuple)):
            try:
                index = int(part)
            except ValueError:
                return MISSING
            if index < 0 or index >= len(current):
                return MISSING
            current = current[index]
        else:
            return MISSING
    return current


def matches(message: Any, expected: Mapping[str, Any] | None) -> bool:
    """Return ``True`` when every field of ``expected`` is present in ``message``.

    An empty (or missing) filter matches everything, which is how a fault that
    should hit "the next message on this link" is written.
    """
    if not expected:
        return True
    if not isinstance(message, Mapping):
        return False
    for key, want in expected.items():
        got = get_field(message, key)
        if got is MISSING:
            return False
        if isinstance(want, Mapping):
            if not matches(got, want):
                return False
        elif got != want:
            return False
    return True


def describe(expected: Mapping[str, Any] | None) -> str:
    """Render a filter for humans (`type='CONTROL_COMMAND'`)."""
    if not expected:
        return "<any message>"
    return ", ".join(f"{key}={value!r}" for key, value in expected.items())
