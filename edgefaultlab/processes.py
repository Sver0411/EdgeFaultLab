"""Process faults: kill and restart the processes EdgeFaultLab started itself.

This module is where the safety boundary lives.  EdgeFaultLab is allowed to
terminate exactly one class of process: a child process it spawned from a
scenario's ``processes`` block.  There is no ``pid`` field anywhere in the
scenario format, and the validator rejects one, because "kill PID 1234" is a
foot-gun with no legitimate use in a test tool that runs on a laptop.

Every start, exit, kill and restart is recorded with the PID it belongs to, so
the report can say "A1 died as PID 4711 and came back as PID 4712".
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path

from .recorder import EventRecorder
from .scenario import ProcessSpec

__all__ = ["ManagedProcess", "ProcessError", "ProcessManager"]

#: How long a process gets to exit after SIGTERM / CTRL_BREAK before SIGKILL.
TERMINATE_GRACE_SECONDS = 5.0

_IS_WINDOWS = sys.platform.startswith("win")


class ProcessError(Exception):
    """A process fault that could not be carried out (never swallowed)."""


class ManagedProcess:
    """One scenario-declared process plus its current OS state."""

    def __init__(self, spec: ProcessSpec, recorder: EventRecorder, log_path: Path):
        self.spec = spec
        self.recorder = recorder
        self.log_path = log_path
        self.proc: asyncio.subprocess.Process | None = None
        self.pid: int | None = None
        self.restarts = 0
        self.exit_code: int | None = None
        self.expected_stop = False
        self._log = None
        self._watcher: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def is_running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    def state(self) -> dict:
        return {
            "process": self.name,
            "pid": self.pid,
            "running": self.is_running,
            "restarts": self.restarts,
            "exit_code": self.exit_code,
        }


class ProcessManager:
    """Owns every child process of one run."""

    def __init__(
        self, specs: tuple[ProcessSpec, ...], recorder: EventRecorder, log_dir: str | Path
    ):
        self.recorder = recorder
        self.log_dir = Path(log_dir)
        self.processes: dict[str, ManagedProcess] = {
            spec.name: ManagedProcess(spec, recorder, self.log_dir / f"{spec.name}.log")
            for spec in specs
        }

    def __contains__(self, name: object) -> bool:
        return name in self.processes

    def __len__(self) -> int:
        return len(self.processes)

    def names(self) -> list[str]:
        return list(self.processes)

    def get(self, name: str) -> ManagedProcess:
        try:
            return self.processes[name]
        except KeyError:
            raise ProcessError(
                f"unknown process: {name!r} (declared: {', '.join(self.processes) or 'none'})"
            ) from None

    # -- lifecycle --------------------------------------------------------

    async def start_all(self) -> None:
        for name in self.processes:
            await self.start(name)

    async def start(self, name: str) -> ManagedProcess:
        managed = self.get(name)
        if managed.is_running:
            raise ProcessError(f"process {name!r} is already running (pid {managed.pid})")

        env = dict(os.environ)
        env.update(managed.spec.env)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        managed._log = managed.log_path.open("ab")

        kwargs: dict = {}
        if _IS_WINDOWS:  # pragma: no cover - platform dependent
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True

        try:
            managed.proc = await asyncio.create_subprocess_exec(
                *managed.spec.command,
                cwd=managed.spec.cwd,
                env=env,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=managed._log,
                stderr=asyncio.subprocess.STDOUT,
                **kwargs,
            )
        except OSError as exc:
            managed._log.close()
            managed._log = None
            raise ProcessError(
                f"process {name!r} failed to start: {exc.strerror or exc} "
                f"(command {managed.spec.command[0]!r}, cwd {managed.spec.cwd!r})"
            ) from exc

        managed.pid = managed.proc.pid
        managed.expected_stop = False
        managed.exit_code = None
        managed._watcher = asyncio.create_task(self._watch(managed))
        self.recorder.event(
            "PROCESS_START",
            details={
                "process": name,
                "pid": managed.pid,
                "command": list(managed.spec.command),
                "cwd": managed.spec.cwd,
            },
        )
        return managed

    async def _watch(self, managed: ManagedProcess) -> None:
        proc = managed.proc
        if proc is None:  # pragma: no cover - defensive
            return
        returncode = await proc.wait()
        managed.exit_code = returncode
        self.recorder.event(
            "PROCESS_EXIT",
            details={
                "process": managed.name,
                "pid": managed.pid,
                "exit_code": returncode,
                "expected": managed.expected_stop,
            },
        )
        if managed._log is not None:
            managed._log.close()
            managed._log = None

    async def kill(self, name: str) -> None:
        """Terminate a managed process because a scenario asked for it."""
        managed = self.get(name)
        if not managed.is_running:
            raise ProcessError(
                f"cannot kill process {name!r}: it is not running"
                + (
                    f" (last exit code {managed.exit_code})"
                    if managed.exit_code is not None
                    else ""
                )
            )
        self.recorder.event(
            "PROCESS_KILL",
            details={"process": name, "pid": managed.pid, "signal": "terminate"},
        )
        await self._terminate(managed)

    async def restart(self, name: str) -> None:
        """Stop (if needed) and start a process again, recording both PIDs."""
        managed = self.get(name)
        old_pid = managed.pid
        if managed.is_running:
            managed.expected_stop = True
            await self._terminate(managed)
        await self.start(name)
        managed.restarts += 1
        self.recorder.event(
            "PROCESS_RESTART",
            details={
                "process": name,
                "old_pid": old_pid,
                "new_pid": managed.pid,
                "restart": managed.restarts,
            },
        )

    async def stop_all(self) -> None:
        """Clean shutdown of everything this manager started."""
        for managed in self.processes.values():
            if not managed.is_running:
                continue
            managed.expected_stop = True
            self.recorder.event(
                "PROCESS_STOP",
                details={"process": managed.name, "pid": managed.pid, "reason": "run finished"},
            )
            await self._terminate(managed)

    async def _terminate(self, managed: ManagedProcess) -> None:
        proc = managed.proc
        if proc is None or proc.returncode is not None:
            return
        self._signal(proc, managed)
        try:
            await asyncio.wait_for(proc.wait(), timeout=TERMINATE_GRACE_SECONDS)
        except asyncio.TimeoutError:
            self.recorder.event(
                "PROCESS_KILL_FORCED",
                details={"process": managed.name, "pid": managed.pid, "signal": "kill"},
            )
            try:
                proc.kill()
            except ProcessLookupError:  # pragma: no cover - already gone
                pass
            await proc.wait()

    def _signal(self, proc: asyncio.subprocess.Process, managed: ManagedProcess) -> None:
        """Ask a process (and, on POSIX, its session) to stop."""
        if _IS_WINDOWS:  # pragma: no cover - platform dependent
            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT)
            except (ValueError, OSError, AttributeError):
                proc.terminate()
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            # No process group (or already gone): fall back to the process itself.
            proc.terminate()
