"""Process faults, and the safety boundary around them."""

from __future__ import annotations

import asyncio
import sys

import pytest

from edgefaultlab.processes import ProcessError, ProcessManager
from edgefaultlab.recorder import EventRecorder
from edgefaultlab.scenario import ProcessSpec
from tests.conftest import wait_until


def sleeper(name: str = "sleeper") -> ProcessSpec:
    return ProcessSpec(
        name=name, command=(sys.executable, "-c", "import time; time.sleep(30)")
    )


def test_start_and_stop_record_the_process_lifecycle(tmp_path):
    recorder = EventRecorder()

    async def scenario():
        manager = ProcessManager((sleeper(),), recorder, tmp_path)
        await manager.start_all()
        managed = manager.get("sleeper")
        assert managed.is_running and managed.pid and managed.pid > 0
        await manager.stop_all()
        await asyncio.sleep(0.1)
        assert not managed.is_running
        assert managed.exit_code is not None

    asyncio.run(scenario())
    events = [event["event"] for event in recorder.events]
    assert events.count("PROCESS_START") == 1
    assert events.count("PROCESS_EXIT") == 1
    start = recorder.of_type("PROCESS_START")[0]
    assert start["details"]["cwd"] is None


def test_kill_terminates_a_managed_process_and_records_its_pid(tmp_path):
    recorder = EventRecorder()

    async def scenario():
        manager = ProcessManager((sleeper(),), recorder, tmp_path)
        await manager.start_all()
        pid = manager.get("sleeper").pid
        await manager.kill("sleeper")
        await asyncio.sleep(0.1)
        assert not manager.get("sleeper").is_running
        assert recorder.of_type("PROCESS_KILL")[0]["details"] == {
            "process": "sleeper",
            "pid": pid,
            "signal": "terminate",
            "fault_id": None,
        }
        with pytest.raises(ProcessError, match="not running"):
            await manager.kill("sleeper")

    asyncio.run(scenario())


def test_restart_records_the_old_and_new_pid(tmp_path):
    recorder = EventRecorder()

    async def scenario():
        manager = ProcessManager((sleeper(),), recorder, tmp_path)
        await manager.start_all()
        old_pid = manager.get("sleeper").pid
        await manager.restart("sleeper")
        managed = manager.get("sleeper")
        assert managed.restarts == 1
        assert managed.is_running
        assert managed.pid not in (None, old_pid)
        await manager.stop_all()

    asyncio.run(scenario())
    restart = recorder.of_type("PROCESS_RESTART")[0]
    assert restart["details"]["old_pid"] != restart["details"]["new_pid"]
    assert restart["details"]["restart"] == 1


def test_unknown_processes_and_broken_commands_are_explicit_errors(tmp_path):
    recorder = EventRecorder()

    async def scenario():
        manager = ProcessManager((), recorder, tmp_path)
        with pytest.raises(ProcessError, match="unknown process"):
            await manager.kill("ghost")

        broken = ProcessSpec(name="broken", command=("/definitely/not/a/binary",))
        other = ProcessManager((broken,), recorder, tmp_path)
        with pytest.raises(ProcessError, match="failed to start"):
            await other.start_all()

    asyncio.run(scenario())
    assert recorder.count("PROCESS_START") == 0


CHATTY = (
    "import sys, time\n"
    "print('incarnation running', flush=True)\n"
    "sys.stdout.flush()\n"
    "time.sleep(60)\n"
)


def test_restart_does_not_let_the_old_watcher_touch_the_new_incarnation(tmp_path):
    """The old process exit must be reported with the old PID, and only then."""
    recorder = EventRecorder()
    logs = tmp_path / "logs"

    async def scenario():
        spec = ProcessSpec(name="chatty", command=(sys.executable, "-u", "-c", CHATTY))
        manager = ProcessManager((spec,), recorder, logs)
        await manager.start_all()
        managed = manager.get("chatty")
        first_pid = managed.pid

        log = logs / "chatty.log"
        assert await wait_until(
            lambda: log.exists() and "incarnation running" in log.read_text(), timeout=10
        ), "the first incarnation never got as far as writing"

        await manager.restart("chatty")
        second_pid = managed.pid
        assert second_pid != first_pid
        assert managed.is_running, "the new incarnation is running"
        assert managed.current.log is not None
        assert not managed.current.log.closed, "the old watcher must not close the new log"

        # Wait for the second incarnation to actually get as far as writing,
        # rather than guessing how long interpreter startup takes.
        assert await wait_until(
            lambda: log.exists() and log.read_text().count("incarnation running") >= 2,
            timeout=10,
        ), "the new incarnation could not write to its own log"
        await manager.stop_all()

    asyncio.run(scenario())

    events = recorder.events
    kinds = [event["event"] for event in events]
    starts = [event["details"]["pid"] for event in recorder.of_type("PROCESS_START")]
    exits = [event["details"]["pid"] for event in recorder.of_type("PROCESS_EXIT")]
    restart = recorder.of_type("PROCESS_RESTART")[0]["details"]

    assert len(starts) == 2 and starts[0] != starts[1], "two incarnations, two PIDs"
    assert restart["old_pid"] == starts[0] and restart["new_pid"] == starts[1]

    # The exit of the first incarnation is reported before the second one starts,
    # so it cannot have been attributed to the new PID.
    assert exits == [starts[0], starts[1]], exits
    first_exit = kinds.index("PROCESS_EXIT")
    second_start = kinds.index("PROCESS_START", kinds.index("PROCESS_START") + 1)
    assert first_exit < second_start

    # Both incarnations really ran: neither log handle was closed underneath it.
    content = (logs / "chatty.log").read_text()
    assert content.count("incarnation running") == 2
