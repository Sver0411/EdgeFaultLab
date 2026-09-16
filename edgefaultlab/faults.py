"""Faults and the engine that decides when they apply.

EdgeFaultLab injects faults at the *semantic* level. A fault says "the next
``CONTROL_COMMAND`` on this link is dropped" or "every ``CONTROL_RESULT`` is
30 seconds late" - not "3% of the packets on this socket are lost". That is the
whole difference between this project and a generic TCP chaos proxy.

Every fault walks the same lifecycle, and every step of it is recorded::

    scheduled -> activated -> matched -> applied -> completed

Determinism: nothing here touches the global ``random`` state. Each fault gets
its own :class:`random.Random` seeded from ``(seed, fault id)``, so the same
scenario, the same seed and the same message order produce the same decisions.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

from .matcher import describe, get_field, matches
from .recorder import EventRecorder
from .scenario import MESSAGE_ACTIONS, TIMED_ACTIONS, FaultSpec

__all__ = ["Fault", "FaultEngine", "MessagePlan", "ReorderedBatch"]


@dataclass
class MessagePlan:
    """What the proxy should do with one received message.

    ``FaultEngine.plan()`` only *decides*.  Actions that take effect later -
    delaying, duplicating, buffering for reorder - are marked applied by the
    proxy at the moment they really happen (``FaultEngine.note_applied``), so a
    fault that never got to act never claims that it did.

    The fault that caused each action is named explicitly instead of being
    guessed from the end of a list: with several faults matching one message,
    the last entry says nothing about which one caused which action.
    """

    #: Every fault whose ``match`` matched this message, in scenario order.
    fault_ids: list[str] = field(default_factory=list)

    dropped: bool = False
    drop_fault_id: str | None = None

    delay_ms: float = 0.0
    delay_fault_id: str | None = None
    delay_fault_ids: list[str] = field(default_factory=list)

    copies: int = 0
    duplicate_fault_id: str | None = None
    duplicate_fault_ids: list[str] = field(default_factory=list)

    mutated: bool = False
    timestamp_fault_ids: list[str] = field(default_factory=list)

    reorder: Fault | None = None
    reorder_window: int = 0

    @property
    def touched(self) -> bool:
        return bool(self.fault_ids)


@dataclass
class ReorderedBatch:
    """Messages released from a reorder buffer, in the order they should go out."""

    fault_id: str
    key: tuple[str, str]
    items: list[tuple[Any, bytes]]


class Fault:
    """One fault from the scenario, with its own state and RNG."""

    def __init__(self, spec: FaultSpec, recorder: EventRecorder, seed: int):
        self.spec = spec
        self.recorder = recorder
        # Faults built by hand (tests, library users) may leave the id empty;
        # the validator fills in the same default for scenario files.
        self.id = spec.fault_id or f"{spec.action}#{spec.index + 1}"
        self.state = "scheduled"
        self.matched = 0
        self.applied = 0
        self.activated_at: float | None = None
        self.completed_at: float | None = None
        # A private stream per fault: the global random state is never touched.
        self.rng = random.Random(f"{seed}:{spec.index}:{spec.fault_id}")
        self._buffers: dict[tuple[str, str], list[tuple[Any, bytes]]] = {}

    # -- lifecycle --------------------------------------------------------

    def schedule(self) -> None:
        self.recorder.event(
            "FAULT_SCHEDULED",
            link=self.spec.link,
            details={
                "fault_id": self.id,
                "action": self.spec.action,
                "at": self.spec.at,
                "match": describe(self.spec.match),
                "count": self.spec.count,
                "probability": self.spec.probability,
            },
        )

    def activate(self) -> None:
        if self.state != "scheduled":
            return
        self.state = "active"
        self.activated_at = self.recorder.now()
        self.recorder.event(
            "FAULT_ACTIVATED",
            link=self.spec.link,
            details={
                "fault_id": self.id,
                "action": self.spec.action,
                "process": self.spec.process,
                "disruptive": self.spec.is_disruptive,
                "message": describe(self.spec.match) if self.spec.is_message_action else None,
            },
        )

    def complete(self, reason: str = "") -> None:
        if self.state == "completed":
            return
        self.state = "completed"
        self.completed_at = self.recorder.now()
        self.recorder.event(
            "FAULT_COMPLETED",
            link=self.spec.link,
            details={
                "fault_id": self.id,
                "action": self.spec.action,
                "matched": self.matched,
                "applied": self.applied,
                "reason": reason,
            },
        )

    @property
    def is_active(self) -> bool:
        return self.state == "active"

    @property
    def is_message_action(self) -> bool:
        return self.spec.action in MESSAGE_ACTIONS

    @property
    def is_timed_action(self) -> bool:
        return self.spec.action in TIMED_ACTIONS

    # -- message faults ---------------------------------------------------

    def applies_to(self, message: Any, link: str | None, direction: str | None) -> bool:
        if self.spec.link is not None and self.spec.link != link:
            return False
        return matches(message, self.spec.match)

    def roll(self) -> bool:
        """Increment the probability gate for this message (per-fault RNG)."""
        if self.spec.probability is None:
            return True
        return self.rng.random() < self.spec.probability

    def _count_exhausted(self) -> bool:
        if self.spec.action == "reorder":
            return False  # for reorder, 'count' is the buffer window, not a cap
        return self.spec.count is not None and self.applied >= self.spec.count

    def note_match(self, message: Any, link: str | None, direction: str | None) -> None:
        self.matched += 1
        message_id = message.get("message_id") if isinstance(message, dict) else None
        self.recorder.event(
            "MESSAGE_MATCHED",
            link=link,
            direction=direction,
            message_id=message_id,
            details={
                "fault_id": self.id,
                "action": self.spec.action,
                "match": describe(self.spec.match),
                "occurrence": self.matched,
            },
        )

    def note_applied(self, link: str | None, reason: str = "") -> None:
        self.applied += 1
        if self._count_exhausted():
            self.complete("count reached")

    def mutate(self, message: dict[str, Any]) -> tuple[bool, float | None, float | None]:
        """Apply ``timestamp_offset`` to an existing ``timestamp`` field only.

        A message without ``timestamp`` is left alone: EdgeFaultLab must not
        invent a field the system under test never had.
        """
        current = get_field(message, "timestamp")
        if current is None or isinstance(current, bool) or not isinstance(current, (int, float)):
            return False, None, None
        offset = self.spec.offset_ms / 1000.0
        message["timestamp"] = current + offset
        return True, current, current + offset

    def buffer(self, key: tuple[str, str], item: tuple[Any, bytes]) -> ReorderedBatch | None:
        """Push a message into the reorder window, release it when full."""
        window = self.spec.reorder_window
        buffer = self._buffers.setdefault(key, [])
        buffer.append(item)
        if len(buffer) < window:
            return None
        items = list(reversed(buffer))
        buffer.clear()
        return ReorderedBatch(fault_id=self.id, key=key, items=items)

    def flush(self, link_name: str | None = None) -> list[ReorderedBatch]:
        """Release whatever is still buffered (called when a link stops).

        With ``link_name``, only the buffers of that link are released; every
        other link keeps its residue untouched, because a link that is shutting
        down has no business writing another link's messages.
        """
        batches = []
        for key, items in self._buffers.items():
            if link_name is not None and key[0] != link_name:
                continue
            if items:
                batches.append(ReorderedBatch(fault_id=self.id, key=key, items=list(items)))
            items.clear()
        return batches

    @property
    def buffered_messages(self) -> int:
        return sum(len(items) for items in self._buffers.values())


class FaultEngine:
    """Holds every fault of a run and answers 'what should happen to this message?'"""

    def __init__(self, faults: tuple[FaultSpec, ...], recorder: EventRecorder, seed: int):
        self.recorder = recorder
        self.faults = [Fault(spec, recorder, seed) for spec in faults]
        self._by_id = {fault.id: fault for fault in self.faults}

    def start(self) -> None:
        for fault in self.faults:
            fault.schedule()

    def timed_faults(self) -> list[Fault]:
        return [fault for fault in self.faults if fault.is_timed_action]

    def message_faults(self) -> list[Fault]:
        return [fault for fault in self.faults if fault.is_message_action]

    def note_applied(self, fault_id: str | None, *, link: str | None = None, reason: str = "") -> None:
        """Record that an action really took effect on a message.

        Called by the proxy once the delivery, the duplicate copy or the reorder
        buffering actually happened - never while a plan is still only a plan.
        """
        if fault_id is None:
            return
        fault = self._by_id.get(fault_id)
        if fault is not None:
            fault.note_applied(link, reason)

    def activate_due(self, now: float) -> None:
        """Activate faults whose ``at`` has passed but whose timer has not fired."""
        for fault in self.faults:
            if fault.state == "scheduled" and now >= fault.spec.at:
                fault.activate()

    def plan(
        self,
        message: dict[str, Any],
        *,
        link: str | None,
        direction: str | None,
    ) -> MessagePlan:
        """Decide what happens to one decoded message on one link.

        Composition rule for v0.1, deliberately simple: **drop has terminal
        priority**.  A message that is dropped reaches nothing, so no other
        action is applied to it and none of the other faults burns a ``count``.
        """
        self.activate_due(self.recorder.now())
        plan = MessagePlan()
        best_copies = 0
        for fault in self.message_faults():
            if not fault.is_active:
                continue
            if not fault.applies_to(message, link, direction):
                continue
            if not fault.roll():
                self.recorder.event(
                    "FAULT_SKIPPED",
                    link=link,
                    direction=direction,
                    message_id=message.get("message_id"),
                    details={"fault_id": fault.id, "reason": "probability"},
                )
                continue

            action = fault.spec.action
            fault.note_match(message, link, direction)
            plan.fault_ids.append(fault.id)

            if action == "drop":
                plan.dropped = True
                plan.drop_fault_id = fault.id
                # Decided here and now: this message will not be forwarded.
                fault.note_applied(link)
                break  # a dropped message reaches nothing else
            if action == "delay":
                plan.delay_fault_ids.append(fault.id)
                if fault.spec.delay_ms > plan.delay_ms:
                    plan.delay_ms = fault.spec.delay_ms
                    plan.delay_fault_id = fault.id  # the fault that sets the final delay
            elif action == "duplicate":
                plan.copies += fault.spec.copies
                plan.duplicate_fault_ids.append(fault.id)
                if fault.spec.copies > best_copies:
                    best_copies = fault.spec.copies
                    plan.duplicate_fault_id = fault.id  # the largest contributor
            elif action == "reorder":
                if plan.reorder is None:
                    # One window per message: a second reorder fault cannot
                    # buffer something that is already in another window, and
                    # is therefore matched but never applied.
                    plan.reorder = fault
                    plan.reorder_window = fault.spec.reorder_window
            elif action == "timestamp_offset":
                ok, before, after = fault.mutate(message)
                if ok:
                    plan.mutated = True
                    plan.timestamp_fault_ids.append(fault.id)
                    self.recorder.event(
                        "MESSAGE_MUTATED",
                        link=link,
                        direction=direction,
                        message=message,
                        details={
                            "fault_id": fault.id,
                            "field": "timestamp",
                            "before": before,
                            "after": after,
                            "offset_ms": fault.spec.offset_ms,
                        },
                    )
                    # The message itself was changed, so this one really happened.
                    fault.note_applied(link)
                else:
                    self.recorder.event(
                        "FAULT_SKIPPED",
                        link=link,
                        direction=direction,
                        message_id=message.get("message_id"),
                        details={
                            "fault_id": fault.id,
                            "reason": "message has no numeric timestamp field",
                        },
                    )
        return plan

    def complete_all(self, reason: str = "run finished") -> None:
        for fault in self.faults:
            fault.complete(reason)

    def flush(self, link_name: str | None = None) -> list[ReorderedBatch]:
        """Release reorder buffers, optionally only those of one link."""
        batches: list[ReorderedBatch] = []
        for fault in self.message_faults():
            batches.extend(fault.flush(link_name))
        return batches

    @property
    def buffered_messages(self) -> int:
        return sum(fault.buffered_messages for fault in self.faults)
