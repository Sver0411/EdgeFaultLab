"""Scenario parsing and validation: bad input must fail loudly, and early."""

from __future__ import annotations

import json
import os

import pytest

from edgefaultlab.scenario import ScenarioError, load_scenario
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
