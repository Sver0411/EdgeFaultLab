"""End to end: a real three node system, real sockets, real faults, real report.

Nothing here is mocked.  The producer, relay and consumer are started by the
runner itself, they speak newline JSON over TCP, and the assertions are checked
against what the proxy actually delivered.
"""

from __future__ import annotations

import asyncio
import json
import sys

from edgefaultlab.runner import ScenarioRunner
from edgefaultlab.scenario import load_scenario
from tests.conftest import (
    Collector,
    DEMO_SYSTEM,
    free_port,
    free_ports,
    read_events,
    run_scenario_file,
    write_scenario,
)


def demo_scenario(duration: float, faults: list[dict], assertions: list[dict]) -> dict:
    proxy_producer, relay_port, proxy_consumer, consumer_port = free_ports(4)
    return {
        "name": "demo end to end",
        "seed": 42,
        "duration": duration,
        "links": [
            {
                "name": "producer_to_relay",
                "listen": f"127.0.0.1:{proxy_producer}",
                "upstream": f"127.0.0.1:{relay_port}",
            },
            {
                "name": "relay_to_consumer",
                "listen": f"127.0.0.1:{proxy_consumer}",
                "upstream": f"127.0.0.1:{consumer_port}",
            },
        ],
        "processes": [
            {
                "name": "consumer",
                "command": [sys.executable, "consumer.py", "--port", str(consumer_port)],
                "cwd": str(DEMO_SYSTEM),
            },
            {
                "name": "relay",
                "command": [
                    sys.executable,
                    "relay.py",
                    "--listen",
                    f"127.0.0.1:{relay_port}",
                    "--upstream",
                    f"127.0.0.1:{proxy_consumer}",
                ],
                "cwd": str(DEMO_SYSTEM),
            },
            {
                "name": "producer",
                "command": [
                    sys.executable,
                    "producer.py",
                    "--connect",
                    f"127.0.0.1:{proxy_producer}",
                    "--interval",
                    "1.5",
                ],
                "cwd": str(DEMO_SYSTEM),
            },
        ],
        "faults": faults,
        "assertions": assertions,
    }


def executed_assertions(after: float = 0, within: float = 12) -> list[dict]:
    return [
        {
            "assert": "eventually",
            "after": after,
            "within": within,
            "link": "producer_to_relay",
            "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
        },
        {
            "assert": "unique",
            "link": "producer_to_relay",
            "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
            "key": "payload.command_id",
        },
    ]


def test_drop_scenario_passes_and_writes_its_artifacts(tmp_path):
    data = demo_scenario(
        duration=9,
        faults=[
            {
                "id": "drop-first",
                "at": 0,
                "action": "drop",
                "link": "relay_to_consumer",
                "match": {"type": "CONTROL_COMMAND"},
                "count": 1,
            }
        ],
        assertions=executed_assertions()
        + [
            {
                "assert": "message_count",
                "link": "producer_to_relay",
                "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
                "min": 2,
            }
        ],
    )
    path = write_scenario(tmp_path, data)
    output = tmp_path / "run"
    exit_code, runner = run_scenario_file(path, output)

    assert exit_code == 0
    assert all(result.passed for result in runner.results)
    assert (output / "events.jsonl").exists()
    assert (output / "report.md").exists()
    summary = json.loads((output / "summary.json").read_text())
    assert summary["messages_dropped"] == 1
    assert summary["assertions_failed"] == 0
    assert summary["result"] == "PASS"
    assert summary["recovery_time"] is not None
    assert summary["processes"] and all(not p["running"] for p in summary["processes"])

    events = read_events(output / "events.jsonl")
    assert [event["event"] for event in events][0] == "RUN_START"
    assert [event["event"] for event in events][-1] == "RUN_FINISHED"
    assert any(event["event"] == "MESSAGE_DROPPED" for event in events)
    assert (output / "logs" / "relay.log").exists()


def test_a_failing_assertion_exits_nonzero_so_ci_notices(tmp_path):
    data = demo_scenario(
        duration=6,
        faults=[],
        assertions=[
            {
                "assert": "never",
                "link": "producer_to_relay",
                "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
                "description": "commands must never be executed in this scenario",
            }
        ],
    )
    path = write_scenario(tmp_path, data)
    output = tmp_path / "run"
    exit_code, runner = run_scenario_file(path, output)

    assert exit_code == 1
    assert not runner.results[0].passed
    assert runner.summary["assertions_failed"] == 1
    assert runner.summary["result"] == "FAIL"
    assert "FAIL" in (output / "report.md").read_text()


def test_link_down_is_survived_and_the_fault_is_recorded(tmp_path):
    data = demo_scenario(
        duration=12,
        faults=[
            {
                "id": "link-down",
                "at": 2,
                "action": "link_down",
                "link": "relay_to_consumer",
                "duration": 2,
            }
        ],
        assertions=executed_assertions(after=5, within=6),
    )
    path = write_scenario(tmp_path, data)
    output = tmp_path / "run"
    exit_code, runner = run_scenario_file(path, output)

    assert exit_code == 0, [result.detail for result in runner.results]
    events = read_events(output / "events.jsonl")
    kinds = [event["event"] for event in events]
    assert kinds.count("LINK_DOWN") == 1 and kinds.count("LINK_UP") == 1
    assert kinds.count("FAULT_ACTIVATED") == 1 and kinds.count("FAULT_COMPLETED") == 1
    summary = json.loads((output / "summary.json").read_text())
    assert summary["messages_dropped"] == 0
    assert summary["recovery_time"] is not None


#: Connects to the proxy port the instant the process starts - no retry, no
#: sleep.  If the proxy is not listening yet, this raises and the run fails.
CONNECT_IMMEDIATELY = (
    "import json, socket, sys\n"
    "sock = socket.create_connection(('127.0.0.1', int(sys.argv[1])), timeout=3)\n"
    "sock.sendall(json.dumps({'type': 'HELLO', 'message_id': 'startup'}).encode() + b'\\n')\n"
    "sock.close()\n"
)


def test_proxies_listen_before_child_processes_start(tmp_path):
    async def scenario():
        listen, upstream = free_port(), free_port()
        collector = Collector()
        await collector.start(upstream)
        data = {
            "name": "startup order",
            "seed": 1,
            "duration": 3,
            "links": [
                {
                    "name": "l",
                    "listen": f"127.0.0.1:{listen}",
                    "upstream": f"127.0.0.1:{upstream}",
                }
            ],
            "processes": [
                {
                    "name": "connector",
                    "command": [sys.executable, "-c", CONNECT_IMMEDIATELY, str(listen)],
                }
            ],
            "faults": [],
            "assertions": [],
        }
        runner = ScenarioRunner(
            load_scenario(write_scenario(tmp_path, data)), output=tmp_path / "run"
        )
        exit_code = await runner.run()
        await collector.stop()
        return exit_code, runner, collector

    exit_code, runner, collector = asyncio.run(scenario())

    assert exit_code == 0
    assert [message["message_id"] for message in collector.received] == ["startup"]
    assert runner.summary["processes"][0]["exit_code"] == 0, (
        "the child connected without having to retry - EdgeFaultLab did not make it wait"
    )
    assert runner.summary["processes"][0]["pid"] is not None
