"""Scenario files: the complete configuration surface of EdgeFaultLab.

v0.1 reads JSON only - no YAML, no extra dependency, and a file that any
language can generate.  Parsing and validation live together on purpose: an
invalid scenario must fail loudly *before* a single socket is opened, with a
message that names the offending field.

    {
      "name": "lost control command",
      "seed": 42,
      "duration": 20,
      "links": [{"name": "a_to_b", "listen": "127.0.0.1:9501",
                 "upstream": "127.0.0.1:9601"}],
      "processes": [],
      "faults": [{"at": 5.0, "action": "drop", "link": "a_to_b",
                  "match": {"type": "CONTROL_COMMAND"}, "count": 1}],
      "assertions": []
    }
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .matcher import describe

__all__ = [
    "MESSAGE_ACTIONS",
    "TIMED_ACTIONS",
    "ACTION_NAMES",
    "ASSERTION_NAMES",
    "AssertionSpec",
    "FaultSpec",
    "Link",
    "ProcessSpec",
    "Scenario",
    "ScenarioError",
    "load_scenario",
    "parse_address",
]


class ScenarioError(Exception):
    """Raised when a scenario file is missing fields or makes no sense.

    Carries *every* problem found, so ``edgefaultlab validate`` can print a
    complete list instead of making the user fix one typo per run.
    """

    def __init__(self, errors: list[str] | str, path: str | None = None):
        if isinstance(errors, str):
            errors = [errors]
        self.errors = list(errors)
        self.path = path
        prefix = f"{path}: " if path else ""
        head = (
            "scenario is invalid"
            if len(self.errors) == 1
            else f"scenario is invalid ({len(self.errors)} problems)"
        )
        super().__init__(prefix + head + "\n  - " + "\n  - ".join(self.errors))


# ---------------------------------------------------------------------------
# Action vocabulary
# ---------------------------------------------------------------------------

#: Faults that act on a message as it crosses a link.
MESSAGE_ACTIONS = (
    "drop",
    "delay",
    "duplicate",
    "reorder",
    "timestamp_offset",
)

#: Faults that act on a link or a process at a point in time.
TIMED_ACTIONS = (
    "disconnect",
    "link_down",
    "process_kill",
    "process_restart",
)

ACTION_NAMES = MESSAGE_ACTIONS + TIMED_ACTIONS

ASSERTION_NAMES = ("message_count", "never", "eventually", "unique", "sequence")

_LOOPBACK_HOSTS = ("127.0.0.1", "::1", "localhost")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Link:
    """One TCP link EdgeFaultLab proxies: it listens, and dials ``upstream``."""

    name: str
    listen_host: str
    listen_port: int
    upstream_host: str
    upstream_port: int

    @property
    def listen(self) -> str:
        return f"{self.listen_host}:{self.listen_port}"

    @property
    def upstream(self) -> str:
        return f"{self.upstream_host}:{self.upstream_port}"


@dataclass(frozen=True)
class ProcessSpec:
    """A process EdgeFaultLab starts, and is therefore allowed to kill.

    There is deliberately no way to name a PID that EdgeFaultLab did not start.
    """

    name: str
    command: tuple[str, ...]
    cwd: str | None = None
    env: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class FaultSpec:
    """One fault, exactly as written in the scenario."""

    index: int
    action: str
    at: float = 0.0
    fault_id: str = ""
    link: str | None = None
    process: str | None = None
    match: dict[str, Any] = field(default_factory=dict)
    count: int | None = None
    probability: float | None = None
    delay_ms: float = 0.0
    copies: int = 0
    offset_ms: float = 0.0
    link_down_seconds: float = 0.0

    @property
    def is_message_action(self) -> bool:
        return self.action in MESSAGE_ACTIONS

    @property
    def is_disruptive(self) -> bool:
        """Does this fault make the system lose something?

        Used to decide which fault a recovery time is measured from.
        """
        return self.action in ("drop", "disconnect", "link_down", "process_kill")

    @property
    def reorder_window(self) -> int:
        """How many matched messages a ``reorder`` fault buffers (default 2)."""
        return self.count or 2


@dataclass(frozen=True)
class AssertionSpec:
    """One post-run check over the messages EdgeFaultLab actually delivered."""

    index: int
    kind: str
    match: dict[str, Any] = field(default_factory=dict)
    link: str | None = None
    direction: str | None = None
    key: str | None = None
    equals: int | None = None
    minimum: int | None = None
    maximum: int | None = None
    after: float = 0.0
    within: float | None = None
    steps: tuple[dict[str, Any], ...] = ()
    description: str = ""


@dataclass(frozen=True)
class Scenario:
    """A validated scenario, ready to run."""

    name: str
    duration: float
    seed: int
    links: tuple[Link, ...]
    processes: tuple[ProcessSpec, ...]
    faults: tuple[FaultSpec, ...]
    assertions: tuple[AssertionSpec, ...]
    path: str | None = None

    def link(self, name: str) -> Link:
        for candidate in self.links:
            if candidate.name == name:
                return candidate
        raise KeyError(name)

    @property
    def link_names(self) -> tuple[str, ...]:
        return tuple(link.name for link in self.links)

    @classmethod
    def from_dict(cls, data: Any, path: str | None = None, base_dir: str | None = None):
        return _parse_scenario(data, path, base_dir)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def load_scenario(path: str | os.PathLike[str]) -> Scenario:
    """Read, parse and validate a scenario file."""
    scenario_path = Path(path)
    try:
        raw = scenario_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ScenarioError(f"cannot read scenario file: {exc.strerror or exc}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ScenarioError(
            f"not valid JSON: {exc.msg} (line {exc.lineno}, column {exc.colno})"
        ) from exc
    return _parse_scenario(data, str(scenario_path), str(scenario_path.resolve().parent))


def parse_address(value: Any, label: str, errors: list[str]) -> tuple[str, int] | None:
    """Parse ``"127.0.0.1:9501"`` (also ``"[::1]:9501"``)."""
    if not isinstance(value, str):
        errors.append(f"{label} must be a 'host:port' string")
        return None
    host, sep, port_text = value.rpartition(":")
    if not sep or not host or not port_text:
        errors.append(f"{label} must be a 'host:port' string, got {value!r}")
        return None
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        port = int(port_text)
    except ValueError:
        errors.append(f"{label} port must be a number, got {port_text!r}")
        return None
    if not 1 <= port <= 65535:
        errors.append(f"{label} port must be between 1 and 65535, got {port}")
        return None
    return host, port


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_unknown_keys(where: str, data: dict, allowed: set[str], errors: list[str]) -> None:
    for key in data:
        # Keys starting with an underscore are comments ("_note"), which keeps
        # the JSON self-documenting without a schema escape hatch.
        if key not in allowed and not key.startswith("_"):
            errors.append(
                f"{where}: unknown field {key!r} (allowed: {', '.join(sorted(allowed))})"
            )


def _check_match(where: str, data: dict, errors: list[str], required: bool) -> dict:
    match = data.get("match", {})
    if match is None:
        match = {}
    if not isinstance(match, dict):
        errors.append(f"{where}: 'match' must be a JSON object")
        return {}
    if required and not match:
        errors.append(f"{where}: 'match' must not be empty")
    for key, value in match.items():
        if not isinstance(key, str):
            errors.append(f"{where}: 'match' keys must be strings")
        elif isinstance(value, list):
            errors.append(
                f"{where}: match value for {key!r} must be a JSON scalar or object, not a list"
            )
    return match


def _parse_links(data: list[Any], errors: list[str]) -> tuple[Link, ...]:
    links: list[Link] = []
    seen: set[str] = set()
    for index, entry in enumerate(data):
        where = f"links[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{where}: must be a JSON object")
            continue
        _check_unknown_keys(where, entry, {"name", "listen", "upstream"}, errors)
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{where}: 'name' must be a non-empty string")
            name = None
        elif name in seen:
            errors.append(f"{where}: duplicate link name {name!r}")
        else:
            seen.add(name)
        listen = parse_address(entry.get("listen"), f"{where}.listen", errors)
        upstream = parse_address(entry.get("upstream"), f"{where}.upstream", errors)
        if listen and listen[0] not in _LOOPBACK_HOSTS:
            errors.append(
                f"{where}.listen: v0.1 only binds loopback addresses "
                f"(127.0.0.1, ::1, localhost), got {listen[0]!r}"
            )
        if name and listen and upstream:
            links.append(
                Link(
                    name=name,
                    listen_host=listen[0],
                    listen_port=listen[1],
                    upstream_host=upstream[0],
                    upstream_port=upstream[1],
                )
            )
    return tuple(links)


def _parse_processes(
    data: list[Any], errors: list[str], base_dir: str | None
) -> tuple[ProcessSpec, ...]:
    processes: list[ProcessSpec] = []
    seen: set[str] = set()
    for index, entry in enumerate(data):
        where = f"processes[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{where}: must be a JSON object")
            continue
        if "pid" in entry:
            errors.append(
                f"{where}: 'pid' is not allowed - EdgeFaultLab only ever manages "
                "the child processes it started itself"
            )
        _check_unknown_keys(where, entry, {"name", "command", "cwd", "env"}, errors)
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{where}: 'name' must be a non-empty string")
            name = None
        elif name in seen:
            errors.append(f"{where}: duplicate process name {name!r}")
        else:
            seen.add(name)

        command = entry.get("command")
        argv: tuple[str, ...] | None = None
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(part, str) and part for part in command)
        ):
            errors.append(f"{where}: 'command' must be a non-empty list of strings")
        else:
            argv = tuple(command)

        cwd = entry.get("cwd")
        if cwd is not None and (not isinstance(cwd, str) or not cwd):
            errors.append(f"{where}: 'cwd' must be a non-empty string")
            cwd = None
        elif cwd and base_dir and not os.path.isabs(cwd):
            cwd = os.path.normpath(os.path.join(base_dir, cwd))

        env = entry.get("env", {})
        if not isinstance(env, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in env.items()
        ):
            errors.append(f"{where}: 'env' must be a JSON object of strings")
            env = {}

        if name and argv:
            processes.append(ProcessSpec(name=name, command=argv, cwd=cwd, env=dict(env)))
    return tuple(processes)


_FAULT_FIELDS = {
    "id",
    "at",
    "action",
    "link",
    "process",
    "match",
    "count",
    "probability",
    "delay_ms",
    "copies",
    "offset_ms",
    "duration",
}


def _parse_faults(
    data: list[Any],
    errors: list[str],
    link_names: set[str],
    process_names: set[str],
) -> tuple[FaultSpec, ...]:
    faults: list[FaultSpec] = []
    for index, entry in enumerate(data):
        where = f"faults[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{where}: must be a JSON object")
            continue
        if "pid" in entry:
            errors.append(
                f"{where}: 'pid' is not allowed - use 'process' with the name of a "
                "process declared in this scenario"
            )
        _check_unknown_keys(where, entry, _FAULT_FIELDS, errors)

        action = entry.get("action")
        if action not in ACTION_NAMES:
            errors.append(
                f"{where}: unknown action {action!r} (known: {', '.join(ACTION_NAMES)})"
            )
            continue

        fault_id = entry.get("id", f"{action}#{index + 1}")
        if not isinstance(fault_id, str) or not fault_id:
            errors.append(f"{where}: 'id' must be a non-empty string")
            fault_id = f"{action}#{index + 1}"

        at = entry.get("at", 0.0)
        if not _is_number(at) or at < 0:
            errors.append(f"{where}: 'at' must be a number >= 0 (seconds)")
            at = 0.0

        link = entry.get("link")
        if link is not None:
            if not isinstance(link, str):
                errors.append(f"{where}: 'link' must be a string")
                link = None
            elif link not in link_names:
                errors.append(f"{where}: unknown link: {link}")
                link = None
        process = entry.get("process")
        if process is not None:
            if not isinstance(process, str):
                errors.append(f"{where}: 'process' must be a string")
                process = None
            elif process not in process_names:
                errors.append(f"{where}: unknown process: {process}")
                process = None

        count = entry.get("count")
        if count is not None and (not isinstance(count, int) or isinstance(count, bool) or count < 1):
            errors.append(f"{where}: 'count' must be an integer >= 1")
            count = None

        probability = entry.get("probability")
        if probability is not None and (
            not _is_number(probability) or not 0.0 <= probability <= 1.0
        ):
            errors.append(f"{where}: 'probability' must be a number between 0 and 1")
            probability = None

        match = _check_match(where, entry, errors, required=False)

        if action in TIMED_ACTIONS:
            if match:
                errors.append(f"{where}: '{action}' is a timed fault and cannot have a 'match'")
            if action in ("process_kill", "process_restart"):
                if entry.get("process") is None:
                    errors.append(f"{where}: '{action}' requires a 'process'")
                if entry.get("link") is not None:
                    errors.append(f"{where}: '{action}' must not declare a 'link'")
                if count is not None or probability is not None:
                    errors.append(f"{where}: '{action}' does not support 'count' or 'probability'")
            else:
                if entry.get("link") is None:
                    errors.append(f"{where}: '{action}' requires a 'link'")
                if entry.get("process") is not None:
                    errors.append(f"{where}: '{action}' must not declare a 'process'")
            if action == "link_down":
                duration = entry.get("duration")
                if not _is_number(duration) or duration <= 0:
                    errors.append(f"{where}: 'link_down' requires a 'duration' > 0 (seconds)")
                else:
                    faults.append(
                        FaultSpec(
                            index=index,
                            action=action,
                            at=float(at),
                            fault_id=fault_id,
                            link=link,
                            link_down_seconds=float(duration),
                        )
                    )
                continue
            faults.append(
                FaultSpec(
                    index=index,
                    action=action,
                    at=float(at),
                    fault_id=fault_id,
                    link=link,
                    process=process,
                )
            )
            continue

        # --- message actions -------------------------------------------------
        delay_ms = entry.get("delay_ms", 0.0)
        if action == "delay":
            if not _is_number(delay_ms) or delay_ms <= 0:
                errors.append(f"{where}: 'delay' requires a 'delay_ms' > 0")
            elif not match:
                errors.append(f"{where}: 'delay' requires a 'match' filter")
        elif "delay_ms" in entry:
            errors.append(f"{where}: 'delay_ms' is only valid for the 'delay' action")

        copies = entry.get("copies", 0)
        if action == "duplicate":
            if not isinstance(copies, int) or isinstance(copies, bool) or copies < 1:
                errors.append(f"{where}: 'duplicate' requires a 'copies' integer >= 1")
            elif not match:
                errors.append(f"{where}: 'duplicate' requires a 'match' filter")
        elif "copies" in entry:
            errors.append(f"{where}: 'copies' is only valid for the 'duplicate' action")

        offset_ms = entry.get("offset_ms", 0.0)
        if action == "timestamp_offset":
            if not _is_number(offset_ms) or offset_ms == 0:
                errors.append(f"{where}: 'timestamp_offset' requires a non-zero 'offset_ms'")
            elif not match:
                errors.append(f"{where}: 'timestamp_offset' requires a 'match' filter")
        elif "offset_ms" in entry:
            errors.append(f"{where}: 'offset_ms' is only valid for the 'timestamp_offset' action")

        if action == "reorder":
            if not match:
                errors.append(f"{where}: 'reorder' requires a 'match' filter")
            if count is not None and count < 2:
                errors.append(f"{where}: 'reorder' needs a window of at least 2 messages")

        if "duration" in entry:
            errors.append(f"{where}: 'duration' is only valid for the 'link_down' action")

        faults.append(
            FaultSpec(
                index=index,
                action=action,
                at=float(at),
                fault_id=fault_id,
                link=link,
                match=match,
                count=count,
                probability=float(probability) if probability is not None else None,
                delay_ms=float(delay_ms),
                copies=copies,
                offset_ms=float(offset_ms),
            )
        )
    return tuple(faults)


_ASSERTION_FIELDS = {
    "assert",
    "name",
    "description",
    "match",
    "link",
    "direction",
    "key",
    "equals",
    "min",
    "max",
    "after",
    "within",
    "steps",
}


def _parse_assertions(
    data: list[Any], errors: list[str], link_names: set[str]
) -> tuple[AssertionSpec, ...]:
    assertions: list[AssertionSpec] = []
    for index, entry in enumerate(data):
        where = f"assertions[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{where}: must be a JSON object")
            continue
        _check_unknown_keys(where, entry, _ASSERTION_FIELDS, errors)
        kind = entry.get("assert")
        if kind not in ASSERTION_NAMES:
            errors.append(
                f"{where}: unknown assertion {kind!r} (known: {', '.join(ASSERTION_NAMES)})"
            )
            continue

        match = _check_match(where, entry, errors, required=kind in ("never", "eventually"))
        description = entry.get("description") or entry.get("name")
        if description is not None and not isinstance(description, str):
            errors.append(f"{where}: 'description' must be a string")
            description = None

        link = entry.get("link")
        if link is not None:
            if not isinstance(link, str):
                errors.append(f"{where}: 'link' must be a string")
                link = None
            elif link not in link_names:
                errors.append(f"{where}: unknown link: {link}")

        direction = entry.get("direction")
        if direction is not None and direction not in ("forward", "reverse"):
            errors.append(
                f"{where}: 'direction' must be 'forward' (client -> upstream) or "
                "'reverse' (upstream -> client)"
            )
            direction = None

        def number(name: str, minimum: float | None = None) -> float | None:
            value = entry.get(name)
            if value is None:
                return None
            if not _is_number(value) or (minimum is not None and value < minimum):
                bound = "" if minimum is None else f" >= {minimum}"
                errors.append(f"{where}: '{name}' must be a number{bound}")
                return None
            return float(value)

        equals = entry.get("equals")
        if equals is not None and (not isinstance(equals, int) or isinstance(equals, bool)):
            errors.append(f"{where}: 'equals' must be an integer")
            equals = None
        minimum = entry.get("min")
        if minimum is not None and (not isinstance(minimum, int) or isinstance(minimum, bool)):
            errors.append(f"{where}: 'min' must be an integer")
            minimum = None
        maximum = entry.get("max")
        if maximum is not None and (not isinstance(maximum, int) or isinstance(maximum, bool)):
            errors.append(f"{where}: 'max' must be an integer")
            maximum = None

        key = entry.get("key")
        if kind == "unique":
            if not isinstance(key, str) or not key:
                errors.append(f"{where}: 'unique' requires a 'key' field path")
                key = None
        elif key is not None:
            errors.append(f"{where}: 'key' is only valid for the 'unique' assertion")

        steps: tuple[dict[str, Any], ...] = ()
        if kind == "sequence":
            raw_steps = entry.get("steps")
            if not isinstance(raw_steps, list) or len(raw_steps) < 2:
                errors.append(f"{where}: 'sequence' requires a 'steps' list of at least 2 filters")
            elif not all(isinstance(step, dict) and step for step in raw_steps):
                errors.append(f"{where}: every 'steps' entry must be a non-empty JSON object")
            else:
                steps = tuple(raw_steps)
        elif "steps" in entry:
            errors.append(f"{where}: 'steps' is only valid for the 'sequence' assertion")

        within: float | None = None
        if kind == "eventually":
            within = number("within", minimum=0)
            if within is None or within == 0:
                errors.append(f"{where}: 'eventually' requires 'within' > 0 (seconds)")
        elif "within" in entry:
            errors.append(f"{where}: 'within' is only valid for the 'eventually' assertion")

        after = 0.0
        if kind == "eventually":
            after = number("after", minimum=0) or 0.0
        elif "after" in entry:
            errors.append(f"{where}: 'after' is only valid for the 'eventually' assertion")

        if kind == "message_count" and equals is None and minimum is None and maximum is None:
            errors.append(f"{where}: 'message_count' needs at least one of 'equals', 'min', 'max'")

        assertions.append(
            AssertionSpec(
                index=index,
                kind=kind,
                match=match,
                link=link,
                direction=direction,
                key=key,
                equals=equals,
                minimum=minimum,
                maximum=maximum,
                after=after,
                within=within,
                steps=steps,
                description=description or "",
            )
        )
    return tuple(assertions)


def default_description(spec: AssertionSpec) -> str:
    """Human sentence used in the console and the markdown report."""
    where = "" if spec.link is None else f" on {spec.link}"
    if spec.direction is not None:
        where += f" ({spec.direction})"
    if spec.kind == "message_count":
        limits = []
        if spec.equals is not None:
            limits.append(f"== {spec.equals}")
        if spec.minimum is not None:
            limits.append(f">= {spec.minimum}")
        if spec.maximum is not None:
            limits.append(f"<= {spec.maximum}")
        return f"count of [{describe(spec.match)}]{where} {' and '.join(limits)}"
    if spec.kind == "never":
        return f"[{describe(spec.match)}]{where} never observed"
    if spec.kind == "eventually":
        return (
            f"[{describe(spec.match)}]{where} within {spec.within}s after {spec.after}s"
        )
    if spec.kind == "unique":
        return (
            f"every {spec.key}{where} appears at most once in [{describe(spec.match)}]"
        )
    steps = " -> ".join(describe(step) for step in spec.steps)
    return f"sequence{where} {steps}"


def _parse_scenario(data: Any, path: str | None, base_dir: str | None) -> Scenario:
    errors: list[str] = []
    if not isinstance(data, dict):
        raise ScenarioError("scenario must be a JSON object", path)
    _check_unknown_keys(
        "scenario",
        data,
        {"name", "duration", "seed", "links", "processes", "faults", "assertions"},
        errors,
    )

    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        errors.append("'name' must be a non-empty string")
        name = "unnamed"

    duration = data.get("duration")
    if not _is_number(duration) or duration <= 0:
        errors.append("'duration' must be a number > 0 (seconds)")
        duration = 0.0

    seed = data.get("seed", 0)
    if not isinstance(seed, int) or isinstance(seed, bool):
        errors.append("'seed' must be an integer")
        seed = 0

    raw_links = data.get("links")
    if not isinstance(raw_links, list) or not raw_links:
        errors.append("'links' must be a non-empty list")
        raw_links = []

    raw_processes = data.get("processes", [])
    if not isinstance(raw_processes, list):
        errors.append("'processes' must be a list")
        raw_processes = []

    raw_faults = data.get("faults", [])
    if not isinstance(raw_faults, list):
        errors.append("'faults' must be a list")
        raw_faults = []

    raw_assertions = data.get("assertions", [])
    if not isinstance(raw_assertions, list):
        errors.append("'assertions' must be a list")
        raw_assertions = []

    links = _parse_links(raw_links, errors)
    processes = _parse_processes(raw_processes, errors, base_dir)
    faults = _parse_faults(
        raw_faults,
        errors,
        link_names={link.name for link in links},
        process_names={proc.name for proc in processes},
    )
    assertions = _parse_assertions(
        raw_assertions, errors, link_names={link.name for link in links}
    )

    if duration and any(fault.at > duration for fault in faults):
        for fault in faults:
            if fault.at > duration:
                errors.append(
                    f"faults[{fault.index}]: 'at' ({fault.at}) is after the scenario "
                    f"duration ({duration}) so it would never fire"
                )

    if errors:
        raise ScenarioError(errors, path)

    return Scenario(
        name=name,
        duration=float(duration),
        seed=int(seed),
        links=links,
        processes=processes,
        faults=faults,
        assertions=assertions,
        path=path,
    )
