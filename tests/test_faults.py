"""Fault behaviour: what each action does, and when it stops applying."""

from __future__ import annotations

from edgefaultlab.faults import FaultEngine
from edgefaultlab.recorder import EventRecorder
from tests.conftest import make_fault, silent_recorder


def engine(faults, recorder: EventRecorder, seed: int = 42) -> FaultEngine:
    fault_engine = FaultEngine(tuple(faults), recorder, seed)
    fault_engine.start()
    return fault_engine


def test_drop_applies_to_the_matching_message_and_then_completes():
    recorder = silent_recorder()
    fault_engine = engine(
        [make_fault(0, "drop", link="l", match={"type": "CMD"}, count=1)], recorder
    )
    plan = fault_engine.plan({"type": "CMD", "message_id": "m1"}, link="l", direction="forward")
    assert plan.dropped and plan.dropped_by == "drop#1"

    second = fault_engine.plan({"type": "CMD", "message_id": "m2"}, link="l", direction="forward")
    assert not second.dropped, "count=1 must stop after the first match"
    assert fault_engine.faults[0].state == "completed"
    assert not fault_engine.plan({"type": "OTHER"}, link="l", direction="forward").touched
    recorder.close()


def test_duplicate_plan_keeps_the_original_message_untouched():
    recorder = silent_recorder()
    message = {"type": "CMD", "message_id": "m1", "payload": {"command_id": "c1"}}
    original = dict(message)
    fault_engine = engine(
        [make_fault(0, "duplicate", link="l", match={"type": "CMD"}, copies=2)], recorder
    )
    plan = fault_engine.plan(message, link="l", direction="forward")
    assert plan.copies == 2
    assert message == original, "duplication must not rewrite the message"
    recorder.close()


def test_delay_uses_the_largest_matching_delay():
    recorder = silent_recorder()
    fault_engine = engine(
        [
            make_fault(0, "delay", link="l", match={"type": "CMD"}, delay_ms=100),
            make_fault(1, "delay", link="l", match={"type": "CMD"}, delay_ms=1500),
        ],
        recorder,
    )
    plan = fault_engine.plan({"type": "CMD"}, link="l", direction="forward")
    assert plan.delay_ms == 1500
    recorder.close()


def test_timestamp_offset_mutates_only_an_existing_timestamp():
    recorder = silent_recorder()
    fault_engine = engine(
        [
            make_fault(
                0,
                "timestamp_offset",
                link="l",
                match={"type": "CMD"},
                offset_ms=-30000,
            )
        ],
        recorder,
    )
    stamped = {"type": "CMD", "timestamp": 1000.0}
    plan = fault_engine.plan(stamped, link="l", direction="forward")
    assert plan.mutated and stamped["timestamp"] == 970.0

    unstamped = {"type": "CMD", "message_id": "m2"}
    plan = fault_engine.plan(unstamped, link="l", direction="forward")
    assert not plan.mutated
    assert "timestamp" not in unstamped, "a missing timestamp must not be invented"
    assert any(event["event"] == "FAULT_SKIPPED" for event in recorder.events)
    recorder.close()


def test_reorder_releases_the_window_in_reverse_order():
    recorder = silent_recorder()
    fault_engine = engine([make_fault(0, "reorder", link="l", match={"type": "CMD"})], recorder)
    fault = fault_engine.faults[0]
    assert fault.buffer(("l", "forward"), ({"message_id": "a"}, b"a")) is None
    batch = fault.buffer(("l", "forward"), ({"message_id": "b"}, b"b"))
    assert batch is not None
    assert [item[0]["message_id"] for item in batch.items] == ["b", "a"]
    recorder.close()


def test_probability_uses_the_seed_and_not_the_global_random_state():
    def drops_for_seed(seed: int) -> list[bool]:
        recorder = EventRecorder()
        fault_engine = FaultEngine(
            (make_fault(0, "drop", link="l", match={"type": "CMD"}, probability=0.5),),
            recorder,
            seed,
        )
        fault_engine.start()
        decisions = [
            fault_engine.plan({"type": "CMD", "n": index}, link="l", direction="forward").dropped
            for index in range(20)
        ]
        recorder.close()
        return decisions

    first, second, other = drops_for_seed(1), drops_for_seed(1), drops_for_seed(2)
    assert first == second
    assert first != other
    assert 0 < sum(first) < 20, "a 50% fault should drop some, not all, messages"
