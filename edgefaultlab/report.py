"""Run artifacts: the console block, ``summary.json`` and ``report.md``.

Three audiences, three shapes:

* the console answer for a developer who just wants to know if the run passed,
* ``summary.json`` for CI, which wants numbers and an exit code,
* ``report.md`` for the human who has to explain *why* it failed - kept short
  on purpose (fault timeline, recovery, assertions, result), because a
  twenty-page report is a report nobody reads.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .assertions import AssertionResult
from .matcher import matches
from .recorder import EventRecorder, SUMMARY_COUNTERS
from .scenario import AssertionSpec, Scenario

__all__ = [
    "build_report_markdown",
    "build_summary",
    "compute_recovery",
    "CONSOLE_EVENTS",
    "describe_event",
    "format_event",
    "render_console",
    "render_header",
    "write_report",
    "write_summary",
]

_LABELS = {
    "LINK_LISTENING": "proxy started",
    "LINK_DOWN": "link down",
    "LINK_UP": "link up",
    "LINK_DISCONNECTED": "link disconnected",
    "CONNECTION_OPEN": "connection open",
    "CONNECTION_CLOSE": "connection closed",
    "CONNECTION_REJECTED": "connection rejected",
    "CONNECTION_FAILED": "connection failed",
    "CONNECTION_RESET": "connection reset",
    "CONNECTION_ERROR": "connection error",
    "MESSAGE_RECEIVED": "message received",
    "MESSAGE_FORWARDED": "message forwarded",
    "MESSAGE_DROPPED": "message dropped",
    "MESSAGE_DELAYED": "message delayed",
    "MESSAGE_DUPLICATED": "message duplicated",
    "MESSAGE_REORDERED": "message reordered",
    "MESSAGE_REORDER_BUFFERED": "message buffered",
    "REORDER_BUFFER_FLUSHED": "reorder buffer flushed",
    "MESSAGE_MUTATED": "message mutated",
    "MESSAGE_FORWARD_FAILED": "forward failed",
    "MALFORMED_MESSAGE": "malformed message",
    "MESSAGE_TOO_LARGE": "message too large",
    "BUFFER_OVERFLOW": "buffer overflow",
    "DELAY_BUFFER_DISCARDED": "delay buffer discarded",
    "FAULT_SCHEDULED": "fault scheduled",
    "FAULT_ACTIVATED": "fault activated",
    "MESSAGE_MATCHED": "message matched",
    "FAULT_SKIPPED": "fault skipped",
    "FAULT_COMPLETED": "fault completed",
    "PROCESS_START": "process started",
    "PROCESS_EXIT": "process exited",
    "PROCESS_KILL": "process killed",
    "PROCESS_KILL_FORCED": "process killed (forced)",
    "PROCESS_RESTART": "process restarted",
    "PROCESS_STOP": "process stopped",
}

#: Events worth showing in the markdown timeline.
_TIMELINE_EVENTS = (
    "FAULT_ACTIVATED",
    "MESSAGE_DROPPED",
    "MESSAGE_DELAYED",
    "MESSAGE_DUPLICATED",
    "MESSAGE_REORDERED",
    "MESSAGE_MUTATED",
    "MESSAGE_FORWARD_FAILED",
    "MALFORMED_MESSAGE",
    "MESSAGE_TOO_LARGE",
    "LINK_DOWN",
    "LINK_UP",
    "LINK_DISCONNECTED",
    "PROCESS_KILL",
    "PROCESS_RESTART",
    "PROCESS_EXIT",
)

#: Events echoed live while a run is in progress.  Connection churn and fault
#: bookkeeping stay in the event trace, where they belong.
CONSOLE_EVENTS = frozenset(
    {
        "LINK_LISTENING",
        "LINK_DOWN",
        "LINK_UP",
        "LINK_DISCONNECTED",
        "CONNECTION_FAILED",
        "CONNECTION_RESET",
        "MESSAGE_RECEIVED",
        "MESSAGE_FORWARDED",
        "MESSAGE_DROPPED",
        "MESSAGE_DELAYED",
        "MESSAGE_DUPLICATED",
        "MESSAGE_REORDERED",
        "MESSAGE_MUTATED",
        "MESSAGE_FORWARD_FAILED",
        "MALFORMED_MESSAGE",
        "MESSAGE_TOO_LARGE",
        "BUFFER_OVERFLOW",
        "FAULT_ACTIVATED",
        "MESSAGE_MATCHED",
        "FAULT_COMPLETED",
        "PROCESS_START",
        "PROCESS_EXIT",
        "PROCESS_KILL",
        "PROCESS_RESTART",
    }
)


def _label(event: str) -> str:
    return _LABELS.get(event, event.lower().replace("_", " "))


def describe_event(record: dict[str, Any]) -> str:
    """Compact 'what happened' suffix for one event."""
    event = record["event"]
    details = record.get("details", {})
    bits: list[str] = []
    if record.get("link"):
        bits.append(str(record["link"]))
    if record.get("message_id"):
        bits.append(str(record["message_id"]))
    if details.get("fault_id"):
        bits.append(f"fault={details['fault_id']}")
    if event in ("PROCESS_START", "PROCESS_EXIT", "PROCESS_KILL", "PROCESS_RESTART"):
        process = details.get("process")
        if process:
            bits = [str(process)] + [b for b in bits if not b.startswith("fault=")]
        if details.get("pid") is not None:
            bits.append(f"pid={details['pid']}")
        if details.get("old_pid") is not None:
            bits.append(f"{details['old_pid']} -> {details.get('new_pid')}")
        if details.get("exit_code") is not None:
            bits.append(f"exit={details['exit_code']}")
    if "delay_ms" in details:
        bits.append(f"delay={details['delay_ms']}ms")
    if "copies" in details:
        bits.append(f"copies={details['copies']}")
    if "before" in details and "after" in details:
        bits.append(f"timestamp {details['before']} -> {details['after']}")
    if event == "MALFORMED_MESSAGE" and details.get("error"):
        bits.append(str(details["error"]).splitlines()[0][:60])
    if event == "MESSAGE_TOO_LARGE":
        bits.append(f"size={details.get('size')}")
    return " ".join(bits)


def format_event(record: dict[str, Any]) -> str:
    """One console line for one event, e.g. ``00.812  message dropped  msg-001``."""
    detail = describe_event(record)
    return f"{record['time']:07.3f}  {_label(record['event']):<20} {detail}".rstrip()


def render_header(scenario: Scenario, seed: int, version: str) -> str:
    """The banner printed before a run starts."""
    return "\n".join(
        [
            f"EdgeFaultLab {version}",
            "",
            f"Scenario : {scenario.name}",
            f"Seed     : {seed}",
            f"Duration : {scenario.duration:g}s",
            "",
        ]
    )


def compute_recovery(
    recorder: EventRecorder,
    disruptions: list[tuple[str, float]],
    assertions: tuple[AssertionSpec, ...],
) -> list[dict[str, Any]]:
    """Time from each disruptive fault to the system's first sign of life.

    Recovery is measured against the ``eventually`` assertions of the scenario:
    "the fault fired at 10.0s, and the promised message showed up at 18.2s, so
    recovery took 8.2s".  A scenario without ``eventually`` assertions falls back
    to the first message delivered after the fault.
    """
    eventually = [spec for spec in assertions if spec.kind == "eventually"]
    report = []
    for fault_id, activated_at in disruptions:
        recovered_at: float | None = None
        recovered_by: str | None = None
        for observation in recorder.observations:
            if observation["time"] <= activated_at:
                continue
            if eventually:
                for spec in eventually:
                    if matches(observation.get("message"), spec.match):
                        recovered_at = observation["time"]
                        recovered_by = spec.description or f"assertions[{spec.index}]"
                        break
            else:
                recovered_at = observation["time"]
                recovered_by = "first message delivered after the fault"
            if recovered_at is not None:
                break
        report.append(
            {
                "fault": fault_id,
                "activated_at": round(activated_at, 6),
                "recovered_at": None if recovered_at is None else round(recovered_at, 6),
                "recovery_time": (
                    None if recovered_at is None else round(recovered_at - activated_at, 6)
                ),
                "recovered_by": recovered_by,
            }
        )
    return report


def build_summary(
    scenario: Scenario,
    seed: int,
    recorder: EventRecorder,
    results: list[AssertionResult],
    recovery: list[dict[str, Any]],
    *,
    run_id: str,
    exit_code: int,
    failures: list[str],
    processes: list[dict[str, Any]],
) -> dict[str, Any]:
    """The machine readable outcome of a run."""
    counters = recorder.counters_summary()
    first_recovery = next(
        (entry["recovery_time"] for entry in recovery if entry["recovery_time"] is not None),
        None,
    )
    summary: dict[str, Any] = {
        "run_id": run_id,
        "scenario": scenario.name,
        "seed": seed,
        "duration": scenario.duration,
        "observed_duration": round(recorder.duration, 6),
        "messages_received": counters["messages_received"],
        "messages_forwarded": counters["messages_forwarded"],
        "messages_dropped": counters["messages_dropped"],
        "messages_delayed": counters["messages_delayed"],
        "messages_duplicated": counters["messages_duplicated"],
        "messages_reordered": counters["messages_reordered"],
        "messages_mutated": counters["messages_mutated"],
        "malformed_messages": counters["malformed_messages"],
        "messages_too_large": counters["messages_too_large"],
        "connections_opened": counters["connections_opened"],
        "connections_closed": counters["connections_closed"],
        "faults_activated": counters["faults_activated"],
        "faults_completed": counters["faults_completed"],
        "process_kills": counters["process_kills"],
        "process_restarts": counters["process_restarts"],
        "assertions_passed": sum(1 for result in results if result.passed),
        "assertions_failed": sum(1 for result in results if not result.passed),
        "recovery_time": first_recovery,
        "recovery": recovery,
        "assertions": [
            {
                "assert": result.kind,
                "description": result.description,
                "result": result.status,
                "detail": result.detail,
            }
            for result in results
        ],
        "processes": processes,
        "failures": failures,
        "result": "PASS" if exit_code == 0 else "FAIL",
        "exit_code": exit_code,
    }
    return summary


def write_summary(path: str | Path, summary: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def build_report_markdown(
    scenario: Scenario,
    seed: int,
    recorder: EventRecorder,
    results: list[AssertionResult],
    recovery: list[dict[str, Any]],
    summary: dict[str, Any],
) -> str:
    """Short, human readable run report."""
    lines: list[str] = [
        "# EdgeFaultLab Run",
        "",
        f"Scenario: {scenario.name}",
        f"Seed: {seed}",
        f"Duration: {scenario.duration:g}s",
        f"Result: {summary['result']}",
        "",
        "## Faults",
        "",
    ]
    timeline = [
        record for record in recorder.events if record["event"] in _TIMELINE_EVENTS
    ]
    if timeline:
        for record in timeline:
            detail = describe_event(record)
            lines.append(
                f"- {record['time']:.3f}s `{record['event']}`"
                + (f" {detail}" if detail else "")
            )
    else:
        lines.append("- no fault fired during this run")

    lines += ["", "## Messages", ""]
    for key, event in SUMMARY_COUNTERS.items():
        if not key.startswith("messages_") and key != "malformed_messages":
            continue
        lines.append(f"- {key.replace('_', ' ')}: {recorder.count(event)}")

    if summary["processes"]:
        lines += ["", "## Processes", ""]
        for process in summary["processes"]:
            lines.append(
                f"- {process['process']}: pid={process['pid']} "
                f"running={process['running']} restarts={process['restarts']}"
            )

    lines += ["", "## Recovery", ""]
    entries = [entry for entry in recovery if entry["recovery_time"] is not None]
    if entries:
        for entry in entries:
            lines.append(
                f"- {entry['fault']}: {entry['recovery_time']:.3f}s "
                f"(fault at {entry['activated_at']:.3f}s, recovered at "
                f"{entry['recovered_at']:.3f}s via {entry['recovered_by']})"
            )
        lines.append("")
        lines.append(f"Recovery time: {entries[0]['recovery_time']:.3f} s")
    else:
        lines.append("Recovery time: n/a (no disruptive fault, or nothing recovered)")

    lines += ["", "## Assertions", ""]
    if results:
        for result in results:
            lines.append(f"{result.status} {result.description} - {result.detail}")
    else:
        lines.append("(the scenario declares no assertions)")

    if summary["failures"]:
        lines += ["", "## Errors", ""]
        lines += [f"- {failure}" for failure in summary["failures"]]

    lines += ["", "## Result", "", summary["result"], ""]
    return "\n".join(lines)


def write_report(path: str | Path, text: str) -> None:
    Path(path).write_text(text, encoding="utf-8")


def render_console(
    scenario: Scenario,
    seed: int,
    recorder: EventRecorder,
    results: list[AssertionResult],
    summary: dict[str, Any],
) -> str:
    """The block printed at the end of a run."""
    counters = recorder.counters_summary()
    lines = ["", "Assertions"]
    if results:
        for result in results:
            lines.append(f"{result.status}  {result.description}")
    else:
        lines.append("(none declared)")

    lines += ["", "Messages"]
    for key in (
        "messages_received",
        "messages_forwarded",
        "messages_dropped",
        "messages_delayed",
        "messages_duplicated",
        "messages_reordered",
    ):
        lines.append(f"{key.replace('messages_', ''):<12}: {counters[key]}")
    if counters["malformed_messages"]:
        lines.append(f"{'malformed':<12}: {counters['malformed_messages']}")

    lines += ["", "Faults", f"{'activated':<12}: {counters['faults_activated']}",
              f"{'completed':<12}: {counters['faults_completed']}"]

    if summary["processes"]:
        lines += ["", "Processes"]
        for process in summary["processes"]:
            lines.append(
                f"{process['process']:<12}: pid={process['pid']} "
                f"restarts={process['restarts']}"
            )

    if summary["recovery_time"] is not None:
        lines += ["", "Recovery", f"recovery time: {summary['recovery_time']:.3f}s"]

    lines += ["", "Result", summary["result"]]
    return "\n".join(lines)
