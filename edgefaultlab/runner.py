"""The scenario runner: one run, start to finish.

    processes start -> proxies listen -> faults fire on schedule
                    -> the scenario duration elapses
                    -> everything shuts down, assertions are evaluated
                    -> runs/<run_id>/{events.jsonl,summary.json,report.md}

Exit codes, so a scenario can gate a CI job:

* ``0`` every assertion passed,
* ``1`` at least one assertion failed,
* ``2`` the run itself could not be carried out (bad port, process that will
  not start, an assertion that could not be evaluated, buffer overflow).
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from pathlib import Path

from .assertions import AssertionResult, evaluate_assertions
from .faults import Fault, FaultEngine
from .processes import ProcessError, ProcessManager
from .proxy import LinkProxy, ProxyError
from .recorder import EventRecorder
from .report import (
    build_report_markdown,
    build_summary,
    compute_recovery,
    find_disruptions,
    render_console,
    write_report,
    write_summary,
)
from .scenario import Scenario

__all__ = ["EXIT_OK", "EXIT_ASSERTION_FAILED", "EXIT_RUN_FAILED", "ScenarioRunner"]

EXIT_OK = 0
EXIT_ASSERTION_FAILED = 1
EXIT_RUN_FAILED = 2

DEFAULT_RUNS_DIR = "runs"


class ScenarioRunner:
    """Runs one scenario and produces one run directory."""

    def __init__(
        self,
        scenario: Scenario,
        *,
        output: str | Path | None = None,
        runs_dir: str | Path = DEFAULT_RUNS_DIR,
        seed: int | None = None,
        echo: Callable[[dict], None] | None = None,
        log: Callable[[str], None] | None = None,
    ):
        self.scenario = scenario
        self.output = Path(output) if output is not None else None
        self.runs_dir = Path(runs_dir)
        self.seed = scenario.seed if seed is None else seed
        self.echo = echo
        self.log = log or (lambda message: None)

        self.recorder: EventRecorder | None = None
        self.engine: FaultEngine | None = None
        self.processes: ProcessManager | None = None
        self.proxies: dict[str, LinkProxy] = {}
        self.results: list[AssertionResult] = []
        self.summary: dict | None = None
        self.run_dir: Path | None = None
        self.failures: list[str] = []
        self.stop_event = asyncio.Event()
        self._start_monotonic = 0.0

    # -- public API --------------------------------------------------------

    async def run(self) -> int:
        """Run the scenario and return the process exit code."""
        run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
        self.run_dir = self.output if self.output is not None else self.runs_dir / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self._start_monotonic = time.monotonic()
        self.recorder = EventRecorder(
            self.run_dir / "events.jsonl", echo=self.echo, start_time=self._start_monotonic
        )
        self.engine = FaultEngine(self.scenario.faults, self.recorder, self.seed)
        self.processes = ProcessManager(
            self.scenario.processes, self.recorder, self.run_dir / "logs"
        )
        self.proxies = {
            link.name: LinkProxy(link, self.engine, self.recorder, on_error=self._on_error)
            for link in self.scenario.links
        }

        started_ok = False
        timers: list[asyncio.Task] = []
        self.recorder.event(
            "RUN_START",
            details={
                "scenario": self.scenario.name,
                "seed": self.seed,
                "duration": self.scenario.duration,
                "run_id": run_id,
                "links": list(self.proxies),
            },
        )
        try:
            self.engine.start()
            # Links listen first: a node that connects the instant it starts
            # must find EdgeFaultLab already there, or the test tool itself
            # becomes the fault. Only a real client connection needs the
            # upstream, and that happens later.
            for proxy in self.proxies.values():
                await proxy.start()
            await self.processes.start_all()
            started_ok = True
            timers = [
                asyncio.create_task(self._run_timed_fault(fault))
                for fault in self.engine.timed_faults()
            ]
            await self._run_for(self.scenario.duration)
        except (ProcessError, ProxyError, OSError) as exc:
            self.failures.append(f"{type(exc).__name__}: {exc}")
            self.recorder.event("RUN_FAILED", details={"error": str(exc)})
            self.log(f"error: {exc}")
        finally:
            for timer in timers:
                timer.cancel()
            await self._shutdown(started_ok)

        results = evaluate_assertions(self.scenario.assertions, self.recorder.observations)
        self.results = results
        for result in results:
            self.recorder.event(
                "ASSERTION_PASS" if result.passed else "ASSERTION_FAIL",
                details={
                    "assert": result.kind,
                    "description": result.description,
                    "detail": result.detail,
                },
            )
        self.recorder.event("RUN_FINISHED", details={"failures": len(self.failures)})
        disruptions = find_disruptions(self.recorder, self.scenario.faults)
        recovery = compute_recovery(self.recorder, disruptions, self.scenario.assertions)
        exit_code = self._exit_code(results)
        summary = build_summary(
            self.scenario,
            self.seed,
            self.recorder,
            results,
            recovery,
            run_id=run_id,
            exit_code=exit_code,
            failures=list(self.failures),
            processes=self._process_states(),
        )
        self.summary = summary
        write_summary(self.run_dir / "summary.json", summary)
        write_report(
            self.run_dir / "report.md",
            build_report_markdown(
                self.scenario, self.seed, self.recorder, results, recovery, summary
            ),
        )
        self.console = render_console(self.scenario, self.seed, self.recorder, results, summary)
        self.recorder.close()
        return exit_code

    # -- internals ---------------------------------------------------------

    def _exit_code(self, results: list[AssertionResult]) -> int:
        if self.failures:
            return EXIT_RUN_FAILED
        if any(not result.passed for result in results):
            return EXIT_ASSERTION_FAILED
        return EXIT_OK

    def _process_states(self) -> list[dict]:
        if self.processes is None:
            return []
        return [managed.state() for managed in self.processes.processes.values()]

    def _on_error(self, exc: Exception) -> None:
        """Called from inside the proxy when a fault could not be applied."""
        self.failures.append(f"{type(exc).__name__}: {exc}")
        self.stop_event.set()

    async def _run_for(self, duration: float) -> None:
        """Wait for the scenario duration, or until something forces a stop."""
        deadline = self._start_monotonic + duration
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=remaining)
                return  # a failure asked us to stop early
            except asyncio.TimeoutError:
                continue

    async def _sleep_until(self, at: float) -> None:
        await asyncio.sleep(max(0.0, self._start_monotonic + at - time.monotonic()))

    async def _run_timed_fault(self, fault: Fault) -> None:
        """Timer driven faults: disconnect, link_down, process_kill, process_restart."""
        spec = fault.spec
        try:
            await self._sleep_until(spec.at)
            if self.stop_event.is_set():
                return
            fault.activate()
            if spec.action == "disconnect":
                proxy = self.proxies[spec.link]
                await proxy.disconnect(f"fault {fault.id}", fault_id=fault.id)
                fault.complete("connections dropped")
            elif spec.action == "link_down":
                proxy = self.proxies[spec.link]
                await proxy.set_down(True, f"fault {fault.id}", fault_id=fault.id)
                await self._sleep_until(spec.at + spec.link_down_seconds)
                if not self.stop_event.is_set():
                    await proxy.set_down(False, f"fault {fault.id}")
                    fault.complete("link restored")
            elif spec.action == "process_kill":
                await self.processes.kill(spec.process, fault_id=fault.id)
                fault.complete("process terminated")
            elif spec.action == "process_restart":
                await self.processes.restart(spec.process)
                fault.complete("process restarted")
        except asyncio.CancelledError:
            raise
        except (ProcessError, ProxyError) as exc:
            self.failures.append(f"{type(exc).__name__}: {exc}")
            self.recorder.event(
                "FAULT_FAILED",
                link=spec.link,
                details={"fault_id": fault.id, "error": str(exc)},
            )
            self.log(f"error: fault {fault.id}: {exc}")
            self.stop_event.set()

    async def _shutdown(self, started_ok: bool) -> None:
        """Close links, flush buffers, stop child processes - always in that order."""
        if self.engine is not None:
            self.engine.complete_all(
                "run finished" if started_ok else "run aborted during startup"
            )
        for proxy in self.proxies.values():
            try:
                await proxy.stop()
            except Exception as exc:  # pragma: no cover - shutdown must not raise
                self.failures.append(f"shutdown: {type(exc).__name__}: {exc}")
        if self.processes is not None:
            try:
                await self.processes.stop_all()
            except ProcessError as exc:
                self.failures.append(str(exc))
