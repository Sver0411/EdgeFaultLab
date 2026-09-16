"""Process faults, and the safety boundary around them."""

from __future__ import annotations

import asyncio
import sys

import pytest

from edgefaultlab.processes import ProcessError, ProcessManager
from edgefaultlab.recorder import EventRecorder
from edgefaultlab.scenario import ProcessSpec


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
