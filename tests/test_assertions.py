"""The five assertions, and the link selector that makes them unambiguous."""

from __future__ import annotations

from edgefaultlab.assertions import evaluate_assertions
from edgefaultlab.scenario import AssertionSpec


def observation(time: float, message_type: str, link: str = "l", **payload):
    return {
        "time": time,
        "message": {"type": message_type, "payload": payload},
        "link": link,
        "direction": "reverse",
    }


def run(spec: AssertionSpec, observations: list[dict]) -> tuple[bool, str]:
    result = evaluate_assertions((spec,), observations)[0]
    return result.passed, result.detail


def test_message_count_supports_equals_min_and_max():
    observations = [observation(1.0, "RESULT", status="EXECUTED"), observation(2.0, "RESULT")]
    passed, detail = run(
        AssertionSpec(0, "message_count", match={"type": "RESULT"}, equals=2), observations
    )
    assert passed and "observed 2" in detail

    passed, detail = run(
        AssertionSpec(0, "message_count", match={"payload.status": "EXECUTED"}, equals=2),
        observations,
    )
    assert not passed and "expected exactly 2" in detail

    passed, _ = run(
        AssertionSpec(0, "message_count", match={"type": "RESULT"}, minimum=1, maximum=1),
        observations,
    )
    assert not passed, "both bounds are enforced"


def test_never_fails_and_reports_the_first_offending_message():
    observations = [observation(1.0, "RESULT", reason="UNSAFE_EXECUTION")]
    passed, detail = run(
        AssertionSpec(0, "never", match={"payload.reason": "UNSAFE_EXECUTION"}), observations
    )
    assert not passed and "first at" in detail

    passed, detail = run(AssertionSpec(0, "never", match={"type": "ALERT"}), observations)
    assert passed and "was ever delivered" in detail


def test_eventually_only_looks_inside_its_window():
    observations = [observation(2.0, "HEARTBEAT"), observation(12.0, "HEARTBEAT")]
    passed, detail = run(
        AssertionSpec(
            0, "eventually", match={"type": "HEARTBEAT"}, after=10, within=8
        ),
        observations,
    )
    assert passed and "12.000s" in detail

    passed, detail = run(
        AssertionSpec(0, "eventually", match={"type": "HEARTBEAT"}, after=13, within=5),
        observations,
    )
    assert not passed and "nothing matching" in detail


def test_unique_catches_a_command_that_was_executed_twice():
    observations = [
        observation(1.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-1"),
        observation(2.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-2"),
        observation(3.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-1"),
    ]
    spec = AssertionSpec(
        0,
        "unique",
        match={"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
        key="payload.command_id",
    )
    passed, detail = run(spec, observations)
    assert not passed and "observed twice" in detail

    passed, detail = run(spec, observations[:2])
    assert passed and "2 distinct" in detail


def test_unique_passes_only_when_every_match_carries_a_distinct_key():
    spec = AssertionSpec(
        0,
        "unique",
        match={"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
        key="payload.command_id",
    )
    passed, detail = run(spec, [])
    assert passed and "0 distinct" in detail, "nothing to check is not a failure"

    passed, _ = run(
        spec, [observation(1.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-1")]
    )
    assert passed

    passed, detail = run(
        spec,
        [
            observation(1.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-1"),
            observation(2.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-2"),
            observation(3.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-3"),
        ],
    )
    assert passed and "3 distinct" in detail


def test_unique_fails_when_a_matching_message_has_no_key():
    """Not being able to check a command_id is not the same as checking it."""
    spec = AssertionSpec(
        0,
        "unique",
        match={"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
        key="payload.command_id",
    )
    passed, detail = run(spec, [observation(1.0, "CONTROL_RESULT", status="EXECUTED")])
    assert not passed
    assert "1 matching message(s) missing key payload.command_id" in detail

    passed, detail = run(
        spec,
        [
            observation(1.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-1"),
            observation(2.0, "CONTROL_RESULT", status="EXECUTED"),
            observation(3.0, "CONTROL_RESULT", status="EXECUTED"),
        ],
    )
    assert not passed and "2 matching message(s) missing key" in detail

    # Messages that do not match the filter are none of this assertion's business.
    passed, _ = run(
        spec,
        [
            observation(1.0, "CONTROL_RESULT", status="EXECUTED", command_id="cmd-1"),
            observation(2.0, "CONTROL_RESULT", status="REJECTED"),
        ],
    )
    assert passed


def test_sequence_needs_the_steps_in_order():
    spec = AssertionSpec(
        0, "sequence", steps=({"type": "CONTROL_COMMAND"}, {"type": "ACK"})
    )
    passed, detail = run(spec, [observation(1.0, "ACK"), observation(2.0, "CONTROL_COMMAND")])
    assert not passed and "never saw" in detail

    passed, detail = run(spec, [observation(1.0, "CONTROL_COMMAND"), observation(2.0, "ACK")])
    assert passed and "in order" in detail


def test_link_selector_ignores_deliveries_on_other_links():
    observations = [
        observation(1.0, "CONTROL_RESULT", link="consumer_side"),
        observation(1.1, "CONTROL_RESULT", link="producer_side"),
    ]
    spec = AssertionSpec(
        0, "message_count", match={"type": "CONTROL_RESULT"}, link="producer_side", equals=1
    )
    passed, _ = run(spec, observations)
    assert passed

    unselected = AssertionSpec(0, "message_count", match={"type": "CONTROL_RESULT"}, equals=1)
    passed, detail = run(unselected, observations)
    assert not passed, "without a selector every delivery counts"
