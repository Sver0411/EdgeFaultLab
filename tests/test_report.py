"""Run artifacts: summary.json, report.md and the recovery metric."""

from __future__ import annotations

import json

from edgefaultlab.assertions import AssertionResult
from edgefaultlab.recorder import EventRecorder
from edgefaultlab.report import (
    build_report_markdown,
    build_summary,
    compute_recovery,
    find_disruptions,
    render_console,
    write_summary,
)
from edgefaultlab.scenario import AssertionSpec, Scenario
from tests.conftest import make_fault


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
    recorder.event(
        "PROCESS_KILL",
        at=5.0,
        details={"process": "relay", "pid": 12, "signal": "terminate", "fault_id": "kill#1"},
    )
    recorder.observe({"type": "HEARTBEAT"}, link="l", direction="forward", at=8.5)
    recorder.event("MESSAGE_FORWARDED", link="l", message={"type": "HEARTBEAT", "message_id": "m2"})
    return recorder


def kill_fault():
    return make_fault(0, "process_kill", at=5.0, process="relay", fault_id="kill#1")


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
    disruptions = find_disruptions(recorder, (kill_fault(),))
    assert disruptions[0]["event"] == "PROCESS_KILL"
    assert disruptions[0]["at"] == 5.0, "the clock starts when the kill took effect"

    recovery = compute_recovery(recorder, disruptions, (spec,))
    assert recovery[0]["recovery_time"] == 3.5
    assert recovery[0]["recovered_at"] == 8.5

    assert compute_recovery(recorder, [], (spec,)) == []
    stuck = compute_recovery(recorder, [dict(disruptions[0], at=9.0)], (spec,))
    assert stuck[0]["recovery_time"] is None


def test_recovery_starts_when_the_fault_actually_bit():
    """An armed fault that has not done anything yet is not a disruption."""
    recorder = EventRecorder()
    recorder.event(
        "FAULT_ACTIVATED",
        link="l",
        at=5.0,
        details={"fault_id": "drop#1", "action": "drop", "disruptive": True},
    )
    recorder.event(
        "MESSAGE_DROPPED",
        link="l",
        at=8.0,
        message={"type": "CMD", "message_id": "m1"},
        details={"fault_id": "drop#1"},
    )
    recorder.observe({"type": "HEARTBEAT"}, link="l", direction="forward", at=10.0)

    fault = make_fault(0, "drop", at=5.0, link="l", match={"type": "CMD"}, fault_id="drop#1")
    disruptions = find_disruptions(recorder, (fault,))
    assert disruptions[0]["at"] == 8.0
    assert disruptions[0]["event"] == "MESSAGE_DROPPED"

    spec = AssertionSpec(0, "eventually", match={"type": "HEARTBEAT"}, after=0, within=30)
    recovery = compute_recovery(recorder, disruptions, (spec,))
    assert recovery[0]["recovery_time"] == 2.0, "not 5.0s, which would count armed time"
    assert recovery[0]["disrupted_at"] == 8.0

    # A fault that never took effect has no recovery time at all.
    recorder2 = EventRecorder()
    recorder2.event(
        "FAULT_ACTIVATED", link="l", at=5.0, details={"fault_id": "drop#1", "action": "drop"}
    )
    assert find_disruptions(recorder2, (fault,)) == []


def test_recovery_uses_the_same_selector_and_window_as_the_assertion():
    recorder = EventRecorder()
    recorder.event(
        "LINK_DOWN",
        link="link_a",
        at=5.0,
        details={"fault_id": "down#1", "action": "link_down", "reason": "fault down#1"},
    )
    # Same message type, wrong link, then wrong direction, then the real one.
    recorder.observe({"type": "HEARTBEAT"}, link="link_b", direction="forward", at=6.0)
    recorder.observe({"type": "HEARTBEAT"}, link="link_a", direction="reverse", at=7.0)
    recorder.observe({"type": "HEARTBEAT"}, link="link_a", direction="forward", at=9.0)

    fault = make_fault(1, "link_down", at=5.0, link="link_a", fault_id="down#1")
    disruptions = find_disruptions(recorder, (fault,))
    assert disruptions[0]["at"] == 5.0

    spec = AssertionSpec(
        0,
        "eventually",
        match={"type": "HEARTBEAT"},
        after=0,
        within=8,  # the recovery at 9.0s is outside this window
        link="link_a",
        direction="forward",
    )
    recovery = compute_recovery(recorder, disruptions, (spec,))
    assert recovery[0]["recovered_at"] is None, "the window applies to recovery too"
    assert recovery[0]["recovery_time"] is None

    wider = AssertionSpec(
        0,
        "eventually",
        match={"type": "HEARTBEAT"},
        after=0,
        within=20,
        link="link_a",
        direction="forward",
    )
    recovery = compute_recovery(recorder, disruptions, (wider,))
    assert recovery[0]["recovered_at"] == 9.0
    assert recovery[0]["recovery_time"] == 4.0
    assert "on link_a (forward)" in recovery[0]["recovered_by"]


def test_a_disconnect_that_broke_nothing_is_not_a_disruption():
    fault = make_fault(0, "disconnect", at=5.0, link="l", fault_id="cut")
    spec = AssertionSpec(0, "eventually", match={"type": "HEARTBEAT"}, after=0, within=10)

    def disconnected(recorder: EventRecorder, at: float, connections: int) -> None:
        recorder.event(
            "LINK_DISCONNECTED",
            link="l",
            at=at,
            details={"fault_id": "cut", "connections": connections, "reason": "fault cut"},
        )

    empty = EventRecorder()
    disconnected(empty, 5.0, 0)
    empty.observe({"type": "HEARTBEAT"}, link="l", direction="forward", at=8.0)
    assert find_disruptions(empty, (fault,)) == [], "nothing was disconnected"
    assert empty.count("LINK_DISCONNECTED") == 1, "the event is still recorded for audit"

    real = EventRecorder()
    disconnected(real, 5.0, 1)
    real.observe({"type": "HEARTBEAT"}, link="l", direction="forward", at=8.0)
    disruptions = find_disruptions(real, (fault,))
    assert disruptions[0]["at"] == 5.0 and disruptions[0]["event"] == "LINK_DISCONNECTED"
    assert compute_recovery(real, disruptions, (spec,))[0]["recovery_time"] == 3.0

    # A no-op disconnect must not hide a later one that really cut a connection.
    both = EventRecorder()
    disconnected(both, 5.0, 0)
    disconnected(both, 6.0, 2)
    assert find_disruptions(both, (fault,))[0]["at"] == 6.0


def test_report_and_console_are_short_and_state_the_result():
    recorder = recorder_with_fault()
    results = [AssertionResult(0, "message_count", "commands arrive", True, "observed 1 message")]
    summary = build_summary(
        scenario(),
        3,
        recorder,
        results,
        compute_recovery(recorder, find_disruptions(recorder, (kill_fault(),)), ()),
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
