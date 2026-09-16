"""The field matcher: the only query language EdgeFaultLab has."""

from __future__ import annotations

from edgefaultlab.matcher import MISSING, get_field, matches

MESSAGE = {
    "type": "CONTROL_RESULT",
    "source": "C1",
    "payload": {"command_id": "cmd-1", "status": "EXECUTED", "nested": {"a": 1}},
}


def test_top_level_and_dotted_paths_match():
    assert matches(MESSAGE, {"type": "CONTROL_RESULT"})
    assert matches(MESSAGE, {"payload.command_id": "cmd-1"})
    assert matches(MESSAGE, {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"})
    assert not matches(MESSAGE, {"type": "CONTROL_COMMAND"})
    assert not matches(MESSAGE, {"payload.status": "REJECTED"})


def test_missing_fields_never_match_and_are_distinguishable_from_null():
    assert get_field(MESSAGE, "payload.reason") is MISSING
    assert not matches(MESSAGE, {"payload.reason": None})
    assert not matches(MESSAGE, {"payload.command_id.extra": "x"})
    assert get_field(MESSAGE, "payload.nested.a") == 1


def test_empty_filter_matches_any_object_and_nested_objects_match_as_a_subset():
    assert matches(MESSAGE, {})
    assert matches(MESSAGE, None)
    assert not matches("not an object", {"type": "CONTROL_RESULT"})
    assert matches(MESSAGE, {"payload": {"status": "EXECUTED"}})
    assert matches(MESSAGE, {"payload": {"nested": {"a": 1}}})
    assert not matches(MESSAGE, {"payload": {"nested": {"a": 2}}})
