"""Run artifacts: summary.json, report.md and the recovery metric."""

from __future__ import annotations

import json

from edgefaultlab.assertions import AssertionResult
from edgefaultlab.recorder import EventRecorder
from edgefaultlab.report import (
    build_report_markdown,
    build_summary,
    compute_recovery,
    render_console,
    write_summary,
)
from edgefaultlab.scenario import AssertionSpec, Scenario


def scenario() -> Scenario:
    return Scenario(name="unit", duration=10, seed=3, links=(), processes=(), faults=(), assertions=())


def recorder_with_fault() -> EventRecorder:
    recorder = EventRecorder()
    recorder.event("MESSAGE_RECEIVED", link="l", message={"type": "CMD", "message_id": "m1"})
    recorder.event(
        "FAULT_ACTIVATED",
        link="l",
        at=5.0,
        details={"fault_id": "kill#1", "action": "process_kill", "disruptive": True},
    )
    recorder.observe({"type": "HEARTBEAT"}, link="l", direction="forward", at=8.5)
    recorder.event("MESSAGE_FORWARDED", link="l", message={"type": "HEARTBEAT", "message_id": "m2"})
    return recorder


def test_summary_contains_the_documented_counters(tmp_path):
    recorder = recorder_with_fault()
    results = [
        AssertionResult(0, "eventually", "heartbeat returns", True, "ok"),
        AssertionResult(1, "never", "no unsafe execution", False, "seen"),
    ]
    summary = build_summary(
        scenario(),
        3,
        recorder,
        results,
        [],
        run_id="run-1",
        exit_code=1,
        failures=[],
        processes=[],
    )
    for key in (
        "messages_received",
        "messages_forwarded",
        "messages_dropped",
        "messages_delayed",
        "messages_duplicated",
        "process_kills",
        "process_restarts",
        "assertions_passed",
        "assertions_failed",
    ):
        assert key in summary
    assert summary["assertions_passed"] == 1
    assert summary["assertions_failed"] == 1
    assert summary["result"] == "FAIL" and summary["exit_code"] == 1

    path = tmp_path / "summary.json"
    write_summary(path, summary)
    assert json.loads(path.read_text())["scenario"] == "unit"


def test_recovery_time_is_measured_from_the_fault_to_the_promise():
    recorder = recorder_with_fault()
    spec = AssertionSpec(0, "eventually", match={"type": "HEARTBEAT"}, after=0, within=10)
    recovery = compute_recovery(recorder, [("kill#1", 5.0)], (spec,))
    assert recovery[0]["recovery_time"] == 3.5
    assert recovery[0]["recovered_at"] == 8.5

    assert compute_recovery(recorder, [], (spec,)) == []
    stuck = compute_recovery(recorder, [("kill#1", 9.0)], (spec,))
    assert stuck[0]["recovery_time"] is None


def test_report_and_console_are_short_and_state_the_result():
    recorder = recorder_with_fault()
    results = [AssertionResult(0, "message_count", "commands arrive", True, "observed 1 message")]
    summary = build_summary(
        scenario(),
        3,
        recorder,
        results,
        compute_recovery(recorder, [("kill#1", 5.0)], ()),
        run_id="run-1",
        exit_code=0,
        failures=[],
        processes=[{"process": "relay", "pid": 12, "running": False, "restarts": 1, "exit_code": 0}],
    )
    markdown = build_report_markdown(scenario(), 3, recorder, results, summary["recovery"], summary)
    assert "# EdgeFaultLab Run" in markdown
    assert "## Faults" in markdown and "## Recovery" in markdown and "## Assertions" in markdown
    assert "PASS commands arrive" in markdown
    assert markdown.rstrip().endswith("PASS")
    assert len(markdown.splitlines()) < 45, "the report stays short on purpose"

    console = render_console(scenario(), 3, recorder, results, summary)
    assert "PASS  commands arrive" in console
    assert "recovery time: 3.500s" in console
    assert console.rstrip().endswith("PASS")
