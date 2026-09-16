"""Post-run checks: the part that makes this more than a proxy.

Injecting a fault is easy.  The interesting question is whether the system
under test still keeps its promises afterwards, and that question is answered
here::

    message_count   how many matching messages arrived
    never           a matching message must not appear at all
    eventually      a matching message must arrive inside a time window
    unique          a key (``payload.command_id``) must not appear twice
    sequence        filters must be satisfied in order

``unique`` is deliberately unforgiving: a message that matches the filter but
does not carry the key fails the assertion.  "I could not check" is not the same
answer as "I checked, and it is fine".

Assertions run over :attr:`EventRecorder.observations` - the messages that
actually reached the far side of a link.  A dropped message was never observed,
so it cannot satisfy an assertion, and a delayed message counts when it arrives.

A scenario with several links sees the same message more than once, so an
assertion can say where it wants to look::

    {"assert": "message_count", "link": "producer_to_relay", "direction": "reverse",
     "match": {"type": "CONTROL_RESULT"}, "min": 1}

Without a selector an assertion counts every delivery, on every link.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .matcher import MISSING, describe, get_field, matches
from .scenario import AssertionSpec, default_description

__all__ = [
    "AssertionResult",
    "describe_observation",
    "evaluate_assertions",
    "observation_in_window",
    "observation_matches",
    "observation_selected",
    "select_observations",
]


@dataclass
class AssertionResult:
    """Outcome of one assertion, ready to print in the console and the report."""

    index: int
    kind: str
    description: str
    passed: bool
    detail: str

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


def describe_observation(observation: dict[str, Any]) -> str:
    """One-line description of an observed message, for failure messages."""
    message = observation.get("message")
    if not isinstance(message, dict):
        return f"t={observation.get('time', 0):.3f}s <unparsed>"
    parts = [f"t={observation.get('time', 0):.3f}s"]
    for key in ("type", "source", "target", "message_id"):
        if key in message:
            parts.append(f"{key}={message[key]}")
    return " ".join(parts)


def evaluate_assertions(
    assertions: tuple[AssertionSpec, ...], observations: list[dict[str, Any]]
) -> list[AssertionResult]:
    """Run every assertion against the observations of one finished run."""
    results = []
    for spec in assertions:
        passed, detail = _CHECKERS[spec.kind](spec, observations)
        results.append(
            AssertionResult(
                index=spec.index,
                kind=spec.kind,
                description=spec.description or default_description(spec),
                passed=passed,
                detail=detail,
            )
        )
    return results


# ---------------------------------------------------------------------------
# Individual assertions
# ---------------------------------------------------------------------------


def select_observations(
    spec: AssertionSpec, observations: list[dict[str, Any]]
) -> list[dict]:
    """Keep only the deliveries an assertion asked about (link and direction).

    Public because the recovery metric uses the same selection: an assertion and
    the recovery time derived from it must never drift apart.
    """
    return [obs for obs in observations if observation_selected(spec, obs)]


def observation_selected(spec: AssertionSpec, observation: dict[str, Any]) -> bool:
    """Does this delivery sit on the link and direction the assertion watches?"""
    if spec.link is not None and observation.get("link") != spec.link:
        return False
    if spec.direction is not None and observation.get("direction") != spec.direction:
        return False
    return True


def observation_matches(spec: AssertionSpec, observation: dict[str, Any]) -> bool:
    """Does one observed delivery satisfy the assertion's ``match`` filter?"""
    return matches(observation.get("message"), spec.match)


def observation_in_window(spec: AssertionSpec, time: float) -> bool:
    """Is ``time`` inside the window an ``eventually`` assertion watches?"""
    if spec.within is None:
        return True
    return spec.after <= time <= spec.after + spec.within


def _matching(spec: AssertionSpec, observations: list[dict[str, Any]]) -> list[dict]:
    return [obs for obs in select_observations(spec, observations) if observation_matches(spec, obs)]


def _check_message_count(
    spec: AssertionSpec, observations: list[dict[str, Any]]
) -> tuple[bool, str]:
    matched = _matching(spec, observations)
    count = len(matched)
    problems = []
    if spec.equals is not None and count != spec.equals:
        problems.append(f"expected exactly {spec.equals}")
    if spec.minimum is not None and count < spec.minimum:
        problems.append(f"expected at least {spec.minimum}")
    if spec.maximum is not None and count > spec.maximum:
        problems.append(f"expected at most {spec.maximum}")
    detail = f"observed {count} message(s) matching [{describe(spec.match)}]"
    if problems:
        return False, f"{detail} but {' and '.join(problems)}"
    return True, detail


def _check_never(spec: AssertionSpec, observations: list[dict[str, Any]]) -> tuple[bool, str]:
    matched = _matching(spec, observations)
    if matched:
        first = matched[0]
        return False, (
            f"{len(matched)} forbidden message(s); first at "
            f"{describe_observation(first)}"
        )
    return True, f"no message matching [{describe(spec.match)}] was ever delivered"


def _check_eventually(
    spec: AssertionSpec, observations: list[dict[str, Any]]
) -> tuple[bool, str]:
    assert spec.within is not None
    deadline = spec.after + spec.within
    window = [
        obs
        for obs in select_observations(spec, observations)
        if observation_in_window(spec, obs["time"])
    ]
    for observation in window:
        if observation_matches(spec, observation):
            return True, (
                f"matched [{describe(spec.match)}] at {observation['time']:.3f}s "
                f"(window {spec.after:.3f}s..{deadline:.3f}s)"
            )
    return False, (
        f"nothing matching [{describe(spec.match)}] arrived between "
        f"{spec.after:.3f}s and {deadline:.3f}s "
        f"({len(window)} message(s) were delivered in that window)"
    )


def _check_unique(spec: AssertionSpec, observations: list[dict[str, Any]]) -> tuple[bool, str]:
    assert spec.key is not None
    seen: dict[Any, dict] = {}
    matched = _matching(spec, observations)
    missing = [obs for obs in matched if get_field(obs.get("message"), spec.key) is MISSING]
    if missing:
        # A message without the key proves nothing about idempotency, so the
        # assertion cannot pass: silence is not evidence.
        return False, (
            f"{len(missing)} matching message(s) missing key {spec.key}; first at "
            f"{describe_observation(missing[0])}"
        )
    for observation in matched:
        value = get_field(observation.get("message"), spec.key)
        key = value if isinstance(value, (str, int, float, bool)) or value is None else repr(value)
        if key in seen:
            return False, (
                f"{spec.key}={value!r} observed twice: "
                f"{describe_observation(seen[key])} and {describe_observation(observation)}"
            )
        seen[key] = observation
    return True, f"{len(seen)} distinct {spec.key} value(s) in {len(matched)} matching message(s)"


def _check_sequence(spec: AssertionSpec, observations: list[dict[str, Any]]) -> tuple[bool, str]:
    step_index = 0
    for observation in select_observations(spec, observations):
        if step_index >= len(spec.steps):
            break
        if matches(observation.get("message"), spec.steps[step_index]):
            step_index += 1
    if step_index == len(spec.steps):
        return True, f"all {len(spec.steps)} steps occurred in order"
    pending = describe(spec.steps[step_index])
    return False, (
        f"stopped after {step_index}/{len(spec.steps)} steps; never saw [{pending}]"
    )


_CHECKERS = {
    "message_count": _check_message_count,
    "never": _check_never,
    "eventually": _check_eventually,
    "unique": _check_unique,
    "sequence": _check_sequence,
}
