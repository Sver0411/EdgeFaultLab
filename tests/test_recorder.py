"""The event trace: JSONL on disk, counters in memory, bounded message bodies."""

from __future__ import annotations

from edgefaultlab.recorder import MAX_RECORDED_MESSAGE_BYTES, EventRecorder
from tests.conftest import read_events


def test_events_are_written_as_one_json_object_per_line(tmp_path):
    path = tmp_path / "events.jsonl"
    recorder = EventRecorder(path)
    recorder.event("MESSAGE_RECEIVED", link="l", message={"type": "A", "message_id": "m1"})
    recorder.event("MESSAGE_DROPPED", link="l", message_id="m1", details={"fault_id": "drop#1"})
    recorder.close()

    events = read_events(path)
    assert [event["event"] for event in events] == ["MESSAGE_RECEIVED", "MESSAGE_DROPPED"]
    assert events[0]["message_id"] == "m1"
    assert events[0]["details"]["message"].startswith("{")
    assert events[0]["time"] <= events[1]["time"]
    assert events[1]["details"] == {"fault_id": "drop#1"}

    recorder = EventRecorder()
    recorder.event("MESSAGE_RECEIVED", message={"type": "A"}, at=0.1)
    recorder.event("MESSAGE_FORWARDED", message={"type": "A"}, at=0.2)
    recorder.event("MESSAGE_DROPPED", message={"type": "A"}, at=0.3)
    recorder.observe({"type": "A"}, link="l", direction="forward", at=1.5)

    assert recorder.count("MESSAGE_DROPPED") == 1
    assert recorder.counters_summary()["messages_forwarded"] == 1
    assert recorder.observations[0]["time"] == 1.5
    recorder.event("RUN_FINISHED", at=9.0)
    assert recorder.duration == 9.0


def test_large_messages_are_truncated_and_flagged():
    recorder = EventRecorder()
    huge = {"type": "SENSOR_DATA", "payload": {"blob": "x" * (MAX_RECORDED_MESSAGE_BYTES * 2)}}
    record = recorder.event("MESSAGE_RECEIVED", message=huge)
    details = record["details"]
    assert details["truncated"] is True
    assert len(details["message"].encode("utf-8")) <= MAX_RECORDED_MESSAGE_BYTES
    assert details["type"] == "SENSOR_DATA"

    small = recorder.event("MESSAGE_RECEIVED", message={"type": "A"})
    assert "truncated" not in small["details"]
