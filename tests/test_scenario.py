"""Scenario parsing and validation: bad input must fail loudly, and early."""

from __future__ import annotations

import json
import os

import pytest

from edgefaultlab.scenario import Scenario, ScenarioError, load_scenario
from tests.conftest import ROOT, write_scenario

VALID = {
    "name": "demo",
    "seed": 7,
    "duration": 5,
    "links": [{"name": "a_to_b", "listen": "127.0.0.1:9501", "upstream": "127.0.0.1:9601"}],
    "processes": [],
    "faults": [],
    "assertions": [],
}


def test_shipped_example_scenario_loads():
    scenario = load_scenario(ROOT / "scenarios" / "example.json")
    assert scenario.name == "lost control command"
    assert scenario.seed == 42
    assert scenario.link_names == ("gateway_to_controller",)
    assert scenario.faults[0].action == "drop"


def test_unknown_fault_targets_are_reported(tmp_path):
    data = json.loads(json.dumps(VALID))
    data["faults"] = [
        {"action": "drop", "link": "gateway_to_x", "match": {"type": "A"}, "count": 1},
        {"action": "process_kill", "at": 1, "process": "ghost"},
    ]
    with pytest.raises(ScenarioError) as excinfo:
        load_scenario(write_scenario(tmp_path, data))
    assert "unknown link: gateway_to_x" in str(excinfo.value)
    assert "unknown process: ghost" in str(excinfo.value)


def test_unknown_action_and_typoed_field_are_reported(tmp_path):
    data = json.loads(json.dumps(VALID))
    data["faults"] = [{"action": "dalay", "link": "a_to_b", "match": {"type": "A"}}]
    with pytest.raises(ScenarioError) as excinfo:
        load_scenario(write_scenario(tmp_path, data))
    assert "unknown action 'dalay'" in str(excinfo.value)

    data["faults"] = [
        {"action": "delay", "link": "a_to_b", "match": {"type": "A"}, "dalay_ms": 5}
    ]
    with pytest.raises(ScenarioError) as excinfo:
        load_scenario(write_scenario(tmp_path, data))
    assert "unknown field 'dalay_ms'" in str(excinfo.value)


def test_pid_and_external_binding_are_refused(tmp_path):
    data = json.loads(json.dumps(VALID))
    data["faults"] = [{"action": "process_kill", "at": 1, "pid": 1234}]
    with pytest.raises(ScenarioError) as excinfo:
        load_scenario(write_scenario(tmp_path, data))
    assert "'pid' is not allowed" in str(excinfo.value)

    data = json.loads(json.dumps(VALID))
    data["processes"] = [{"name": "node", "command": ["python3", "n.py"], "pid": 99}]
    with pytest.raises(ScenarioError) as excinfo:
        load_scenario(write_scenario(tmp_path, data))
    assert "only ever manages" in str(excinfo.value)

    data = json.loads(json.dumps(VALID))
    data["links"][0]["listen"] = "0.0.0.0:9501"
    with pytest.raises(ScenarioError) as excinfo:
        load_scenario(write_scenario(tmp_path, data))
    assert "only binds loopback addresses" in str(excinfo.value)


def test_assertion_and_duration_problems_are_reported(tmp_path):
    data = json.loads(json.dumps(VALID))
    data["duration"] = 4
    data["faults"] = [{"action": "drop", "at": 9, "link": "a_to_b", "match": {"t": 1}, "count": 1}]
    data["assertions"] = [
        {"assert": "eventually", "match": {"type": "A"}},
        {"assert": "message_count", "match": {"type": "A"}},
        {"assert": "unique", "match": {"type": "A"}},
    ]
    with pytest.raises(ScenarioError) as excinfo:
        load_scenario(write_scenario(tmp_path, data))
    message = str(excinfo.value)
    assert "would never fire" in message
    assert "'eventually' requires 'within'" in message
    assert "'message_count' needs at least one of" in message
    assert "'unique' requires a 'key'" in message


def test_relative_cwd_is_resolved_against_the_scenario_directory(tmp_path):
    data = json.loads(json.dumps(VALID))
    data["processes"] = [
        {"name": "node", "command": ["python3", "node.py"], "cwd": "../target"}
    ]
    scenario = load_scenario(write_scenario(tmp_path, data))
    assert scenario.processes[0].cwd == os.path.normpath(str(tmp_path / ".." / "target"))


def link(name: str, listen: str, upstream: str) -> dict:
    return {"name": name, "listen": listen, "upstream": upstream}


def test_non_finite_numbers_are_rejected(tmp_path):
    """NaN and Infinity are not JSON, and no timing field may carry one."""
    drop_fault = {"action": "drop", "link": "a_to_b", "match": {"type": "A"}, "count": 1}
    eventually = {"assert": "eventually", "match": {"type": "A"}}
    cases = [
        ("duration = NaN", {"duration": float("nan")}, "finite"),
        ("duration = Infinity", {"duration": float("inf")}, "finite"),
        ("at = NaN", {"faults": [dict(drop_fault, at=float("nan"))]}, "finite"),
        ("at = -Infinity", {"faults": [dict(drop_fault, at=float("-inf"))]}, "finite"),
        (
            "delay_ms = Infinity",
            {"faults": [{"action": "delay", "link": "a_to_b", "delay_ms": float("inf"),
                         "match": {"type": "A"}}]},
            "finite",
        ),
        (
            "offset_ms = -Infinity",
            {"faults": [{"action": "timestamp_offset", "link": "a_to_b",
                         "offset_ms": float("-inf"), "match": {"type": "A"}}]},
            "finite",
        ),
        (
            "link_down duration = NaN",
            {"faults": [{"action": "link_down", "at": 1, "link": "a_to_b",
                         "duration": float("nan")}]},
            "finite",
        ),
        ("within = NaN", {"assertions": [dict(eventually, within=float("nan"))]}, "finite"),
    ]
    for label, override, needle in cases:
        data = json.loads(json.dumps(VALID))
        data.update(override)
        raw = write_scenario(tmp_path, data)
        # json.dumps writes the bare NaN / Infinity literals, which is exactly
        # the input the validator has to refuse.
        assert "NaN" in raw.read_text() or "Infinity" in raw.read_text()
        with pytest.raises(ScenarioError) as excinfo:
            load_scenario(raw)
        assert needle in str(excinfo.value).lower(), f"{label}: {excinfo.value}"

    # The same rule holds for scenarios built in Python instead of read from disk.
    with pytest.raises(ScenarioError):
        Scenario.from_dict(dict(VALID, duration=float("nan")))
    with pytest.raises(ScenarioError):
        Scenario.from_dict(dict(VALID, faults=[dict(drop_fault, at=float("inf"))]))


def test_link_topology_is_validated(tmp_path):
    """Duplicate binds, self loops and two-link cycles are caught before running."""
    cases = [
        (
            "duplicate listen",
            [
                link("a", "127.0.0.1:9000", "127.0.0.1:9601"),
                link("b", "127.0.0.1:9000", "127.0.0.1:9602"),
            ],
            "cannot listen on the same address",
        ),
        (
            "localhost is the same host as 127.0.0.1",
            [
                link("a", "localhost:9000", "127.0.0.1:9601"),
                link("b", "127.0.0.1:9000", "127.0.0.1:9602"),
            ],
            "cannot listen on the same address",
        ),
        (
            "self loop",
            [link("a", "127.0.0.1:9000", "localhost:9000")],
            "forwards to itself",
        ),
        (
            "direct two-link cycle",
            [
                link("a", "127.0.0.1:9000", "127.0.0.1:9001"),
                link("b", "127.0.0.1:9001", "127.0.0.1:9000"),
            ],
            "forward to each other",
        ),
    ]
    for label, links, needle in cases:
        data = json.loads(json.dumps(VALID))
        data["links"] = links
        with pytest.raises(ScenarioError) as excinfo:
            load_scenario(write_scenario(tmp_path, data))
        assert needle in str(excinfo.value), f"{label}: {excinfo.value}"

    # A chain that is not a loop still validates.
    data = json.loads(json.dumps(VALID))
    data["links"] = [
        link("a", "127.0.0.1:9000", "127.0.0.1:9601"),
        link("b", "127.0.0.1:9001", "127.0.0.1:9000"),
    ]
    assert len(load_scenario(write_scenario(tmp_path, data)).links) == 2


def test_duplicate_fault_ids_are_rejected(tmp_path):
    """The fault id is the key events, recovery and reports are joined on."""
    cases = [
        (
            "same id, same action",
            [
                {"id": "break-a", "action": "drop", "link": "a_to_b",
                 "match": {"type": "A"}, "count": 1},
                {"id": "break-a", "action": "drop", "link": "a_to_b",
                 "match": {"type": "B"}, "count": 1},
            ],
        ),
        (
            "same id, different action",
            [
                {"id": "break-a", "action": "drop", "link": "a_to_b",
                 "match": {"type": "A"}, "count": 1},
                {"id": "break-a", "action": "link_down", "at": 1, "link": "a_to_b",
                 "duration": 1},
            ],
        ),
        (
            "explicit id clashing with a generated default",
            [
                {"action": "drop", "link": "a_to_b", "match": {"type": "A"}, "count": 1},
                {"id": "drop#1", "action": "link_down", "at": 1, "link": "a_to_b",
                 "duration": 1},
            ],
        ),
    ]
    for label, faults in cases:
        data = json.loads(json.dumps(VALID))
        data["faults"] = faults
        with pytest.raises(ScenarioError) as excinfo:
            load_scenario(write_scenario(tmp_path, data))
        assert "duplicate fault id 'break-a'" in str(excinfo.value) or (
            "duplicate fault id 'drop#1'" in str(excinfo.value)
        ), f"{label}: {excinfo.value}"

    # Distinct ids still validate.
    data = json.loads(json.dumps(VALID))
    data["faults"] = [
        {"id": "break-a", "action": "drop", "link": "a_to_b", "match": {"type": "A"},
         "count": 1},
        {"id": "break-b", "action": "link_down", "at": 1, "link": "a_to_b", "duration": 1},
    ]
    assert len(load_scenario(write_scenario(tmp_path, data)).faults) == 2


def test_message_count_bounds_must_be_possible(tmp_path):
    cases = [
        ("equals -1", {"equals": -1}, "must be >= 0"),
        ("min -1", {"min": -1}, "must be >= 0"),
        ("max -2", {"max": -2}, "must be >= 0"),
        ("min > max", {"min": 10, "max": 2}, "must not be greater than"),
        ("equals < min", {"equals": 5, "min": 10}, "contradicts 'min'"),
        ("equals > max", {"equals": 10, "max": 5}, "contradicts 'max'"),
    ]
    for label, bounds, needle in cases:
        assertion = {"assert": "message_count", "match": {"type": "A"}}
        assertion.update(bounds)
        data = json.loads(json.dumps(VALID))
        data["assertions"] = [assertion]
        with pytest.raises(ScenarioError) as excinfo:
            load_scenario(write_scenario(tmp_path, data))
        assert needle in str(excinfo.value), f"{label}: {excinfo.value}"

    # Redundant but consistent bounds are not an error.
    data = json.loads(json.dumps(VALID))
    data["assertions"] = [
        {
            "assert": "message_count",
            "match": {"type": "A"},
            "equals": 5,
            "min": 2,
            "max": 10,
        },
        {"assert": "message_count", "match": {"type": "A"}, "equals": 0},
    ]
    scenario = load_scenario(write_scenario(tmp_path, data))
    assert scenario.assertions[0].equals == 5
    assert scenario.assertions[1].equals == 0
