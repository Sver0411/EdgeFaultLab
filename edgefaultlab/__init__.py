"""EdgeFaultLab - deterministic fault injection and resilience testing.

EdgeFaultLab sits between the nodes of a distributed Edge / IoT system, injects
message and process faults into a *specific* part of the conversation and then
checks whether the system still keeps the invariants that a scenario declares.

    Scenario -> Inject -> Observe -> Assert -> Report

The whole point of the project is the last two steps: a generic TCP chaos proxy
can drop bytes, EdgeFaultLab asks "was this ``command_id`` executed twice?" and
fails the run when the answer is yes.

The public surface is intentionally small::

    from edgefaultlab import load_scenario, ScenarioRunner

    scenario = load_scenario("scenarios/example.json")
    exit_code = ScenarioRunner(scenario).run()
"""

from __future__ import annotations

from .assertions import AssertionResult, evaluate_assertions
from .matcher import MISSING, get_field, matches
from .processes import ProcessManager, ProcessError
from .proxy import LinkProxy, MessageTooLarge, read_lines
from .recorder import EventRecorder
from .runner import ScenarioRunner
from .scenario import Scenario, ScenarioError, load_scenario

__version__ = "0.1.0"

__all__ = [
    "MISSING",
    "AssertionResult",
    "EventRecorder",
    "LinkProxy",
    "MessageTooLarge",
    "ProcessError",
    "ProcessManager",
    "Scenario",
    "ScenarioError",
    "ScenarioRunner",
    "__version__",
    "evaluate_assertions",
    "get_field",
    "load_scenario",
    "matches",
    "read_lines",
]
