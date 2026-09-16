# EdgeFaultLab

**English** | [简体中文](README.zh-CN.md)

**Deterministic fault injection and resilience testing for distributed Edge / IoT systems.**

EdgeFaultLab sits between the nodes of your system and injects *semantic* faults
- a lost `CONTROL_COMMAND`, a duplicated command, a stale timestamp, a gateway
that dies - and then checks whether the system kept its promises. It exits
non-zero when it did not, so the run can gate a CI job.

```text
Scenario -> Inject -> Observe -> Assert -> Report
```

```bash
pip install -e .
edgefaultlab validate scenarios/drop_message.json
edgefaultlab run scenarios/drop_message.json
```

Python 3.10+, no runtime dependencies, no root, no kernel network tricks.

---

## What is EdgeFaultLab?

A proxy that speaks your protocol, plus an assertion engine that judges the
result. You describe a link between two nodes, a fault that hits part of the
traffic on it, and the invariants the system must keep. EdgeFaultLab runs the
system, injects the fault, records everything it saw, and tells you which
invariants survived.

```text
       Node A                          Node B
   (gateway A1)                    (controller C1)
         |                                ^
         v                                |
   +-------------------------------------------+
   |              EdgeFaultLab                 |
   |  Scenario Runner -> Fault Engine          |
   |  TCP proxy -> Event trace -> Assertions   |
   +-------------------------------------------+
```

## Why it exists

Any project can show that the happy path works. The questions that actually
decide whether an Edge / IoT deployment survives the field are harder:

* what happens when a message is **lost**?
* what happens when a message is **duplicated**?
* what happens when a message **arrives late**?
* what happens when a gateway **dies mid-flight**?
* can the system keep working while the **server is offline**?
* after recovery, is there **split brain** or a **stale owner**?
* is the same actuator command **executed twice**?
* after an execution result is lost, does the system **start the actuator again**?

EdgeFaultLab exists to ask those questions mechanically - to make "we handle
duplicates" a test result instead of a claim in a design document.

## How it differs from a generic network fault proxy

Tools like Toxiproxy, `tc`/`netem` and packet-level chaos engines are very good
at what they do: latency, bandwidth, connection resets, generic packet loss.
EdgeFaultLab is not a replacement for them and does not try to be. It works one
level up, where the system's own vocabulary lives:

| Generic network fault proxy | EdgeFaultLab |
| --- | --- |
| drops *N % of packets* | drops *the next `CONTROL_COMMAND` from `A1` to `C1`* |
| duplicates *bytes* | duplicates *a command, keeping `message_id` and `payload`* |
| adds latency to *a socket* | delays *every `CONTROL_RESULT`* |
| reports *throughput and errors* | reports *assertions: PASS / FAIL and an exit code* |
| asks "did the bytes arrive?" | asks "was `cmd-001` executed exactly once?" |

The last row is the whole point. A proxy tells you what the network did;
EdgeFaultLab tells you whether the system still keeps its invariants.

## Architecture

```text
             EdgeFaultLab

      +---------------------+
      |   Scenario Runner   |
      +----------+----------+
                 |
       +---------+---------+
       |         |         |
       v         v         v
    Process    Fault     Assertion
    Manager    Engine     Engine
       |         |         |
       +----+----+----+----+
            |         |
            v         v
        Event Trace   Report
```

| Module | Responsibility |
| --- | --- |
| `scenario.py` | parse and validate the scenario JSON; the only place that decides what a scenario may say |
| `runner.py` | one run: start processes, open links, fire faults on schedule, shut down, evaluate, report |
| `proxy.py` | newline JSON over TCP; forwards, and applies whatever the fault engine decides |
| `faults.py` | fault lifecycle and per-fault decisions, seeded and reproducible |
| `processes.py` | starts / kills / restarts only the child processes it started itself |
| `assertions.py` | the five post-run checks over what was actually delivered |
| `matcher.py` | the entire query language: `field == value`, plus dotted paths |
| `recorder.py` | the event trace (`events.jsonl`) and the run counters |
| `report.py` | console block, `summary.json`, `report.md`, recovery time |
| `cli.py` | `edgefaultlab validate` / `edgefaultlab run` |

## Fault types

| Action | What it does | Typical question |
| --- | --- | --- |
| `drop` | discards a matching message | does the sender retry? |
| `delay` | holds a matching message, then sends it | does anything time out? |
| `duplicate` | sends the same message again (`message_id` and payload untouched) | is the consumer idempotent? |
| `reorder` | buffers the next matched messages and releases them reversed | does order matter? |
| `timestamp_offset` | adds an offset to an existing `timestamp` field | are expired commands refused? |
| `disconnect` | drops the TCP connections currently open on a link | how fast does the node reconnect? |
| `link_down` | makes a link unusable for N seconds | does the system survive an outage? |
| `process_kill` | terminates a process EdgeFaultLab started | who takes over? |
| `process_restart` | starts it again with the same command, cwd and env | does it recover? |

```json
{"at": 5.0, "action": "drop", "link": "gateway_to_controller",
 "match": {"type": "CONTROL_COMMAND"}, "count": 1}
```

Faults are matched per message and may carry a `count` (apply to the first N
matches) or a `probability` (apply to a fraction of them). A probability is
always rolled on the fault's own seeded random generator, never on global state.

## Scenario format

JSON, because it costs no dependency and every language can emit it. Keys
starting with `_` are treated as comments. The `cwd` of a process is resolved
relative to the scenario file.

```json
{
  "_note": "the first CONTROL_COMMAND never reaches the controller",
  "name": "lost control command",
  "seed": 42,
  "duration": 20,

  "links": [
    {"name": "gateway_to_controller",
     "listen": "127.0.0.1:9501",
     "upstream": "127.0.0.1:9601"}
  ],

  "processes": [
    {"name": "gateway_a1",
     "command": ["python", "gateway.py", "--id", "A1"],
     "cwd": "../target-project"}
  ],

  "faults": [
    {"at": 5.0, "action": "drop", "link": "gateway_to_controller",
     "match": {"type": "CONTROL_COMMAND"}, "count": 1}
  ],

  "assertions": [
    {"assert": "eventually", "after": 5, "within": 15,
     "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"}},
    {"assert": "unique",
     "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
     "key": "payload.command_id"}
  ]
}
```

`edgefaultlab validate` checks all of it up front - unknown links, unknown
processes, misspelled actions, misspelled fields, missing required fields,
faults scheduled after the run ends, numbers that are not finite - and reports
every problem at once. EdgeFaultLab never hard codes a message schema: it
decodes a JSON object and matches the fields *you* name.

The link layout is checked statically too, so a mistake shows up in `validate`
instead of as an `Address already in use` half way through a run:

* two links may not listen on the same address (`localhost` and `127.0.0.1` count
  as the same host; `::1` is its own),
* a link may not forward to its own listen address,
* two links may not forward into each other (a direct two-link cycle).

## Assertions

Assertions run over the messages that actually reached the far side of a link.
A dropped message was never delivered, so it cannot satisfy an assertion; a
delayed message counts when it arrives.

| `assert` | Meaning | Fields |
| --- | --- | --- |
| `message_count` | how many matching messages arrived | `equals` / `min` / `max` |
| `never` | a matching message must not appear at all | `match` |
| `eventually` | a matching message must arrive inside a window | `after`, `within` |
| `unique` | a key must not appear twice | `key` (`payload.command_id`) |
| `sequence` | filters must be satisfied in order | `steps` |

When a scenario has several links, the same message crosses more than one of
them. An assertion can say where to look:

```json
{"assert": "unique", "link": "producer_to_relay", "direction": "reverse",
 "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
 "key": "payload.command_id"}
```

Without a selector, every delivery on every link counts.

`unique` is unforgiving on purpose: if a message matches the filter but does not
carry the key, the assertion **fails**. "I could not check the `command_id`" is
not the same answer as "I checked it, and it is unique", and only the second one
proves idempotency.

## Quick start

```bash
git clone https://github.com/Sver0411/EdgeFaultLab && cd EdgeFaultLab
python3 -m venv .venv && . .venv/bin/activate
pip install -e .

edgefaultlab validate scenarios/drop_message.json   # check a scenario
edgefaultlab run scenarios/drop_message.json        # run it
edgefaultlab run scenarios/drop_message.json --output runs/test-001
edgefaultlab run scenarios/drop_message.json --seed 123
```

Exit codes: `0` every assertion passed, `1` at least one assertion failed,
`2` the run itself could not be carried out (port already in use, a process that
will not start, a buffer overflow).

## Demo

The repository ships a three process system in `examples/demo_system/`, used as
a miniature Edge / IoT deployment - no other project needed:

```text
producer (B1)  ->  relay (A1)  ->  consumer (C1)

producer: sends CONTROL_COMMAND, retries until it is ACKed
relay:    forwards newline JSON, reconnects to its upstream
consumer: executes once, rejects duplicate command_ids and expired commands
```

```text
$ edgefaultlab run scenarios/drop_message.json

EdgeFaultLab 0.1.0

Scenario : drop message
Seed     : 42
Duration : 20s

000.015  process started      consumer pid=9967
000.024  proxy started        producer_to_relay
001.613  message received     producer_to_relay msg-0001
001.614  fault activated      relay_to_consumer fault=drop-first-command
001.616  message dropped      relay_to_consumer msg-0001 fault=drop-first-command
003.114  message received     producer_to_relay msg-0001     (the retry)
003.518  message forwarded    producer_to_relay control_result-cmd-0001

Assertions
PASS  dropped command is eventually delivered and executed
PASS  no command_id is executed twice
PASS  the system keeps working after the drop

Messages
received    : 38
forwarded   : 37
dropped     : 1

Result
PASS
```

Four demo scenarios ship with it:

| Scenario | Fault | Checked |
| --- | --- | --- |
| `scenarios/drop_message.json` | first `CONTROL_COMMAND` dropped | retry, eventual execution, no double execution |
| `scenarios/duplicate_command.json` | first `CONTROL_COMMAND` duplicated | the same `command_id` is executed once |
| `scenarios/delay_result.json` | first `CONTROL_RESULT` delayed 2.5s | late but delivered; `COMMAND -> ACK -> RESULT` order holds |
| `scenarios/process_crash.json` | relay killed at 6s, restarted at 14s | the pipeline recovers, nothing runs twice |

## Smart Agriculture example

`examples/smart_agriculture/` contains five scenarios for a real multi-node
system (server, gateways A1 / A2, sensors, controllers): gateway failover,
server outage, duplicated commands, stale commands and lost commands. They start
that repository's own entry points and copy none of its source; you only have to
edit `cwd` so it points at your checkout. See
[examples/smart_agriculture/README.md](examples/smart_agriculture/README.md) for
the port layout that lets a proxy sit in front of nodes with hard-coded ports.

These files are checked into EdgeFaultLab CI with `edgefaultlab validate`, which
proves they are well formed scenarios. Running them against the other repository
is **not** part of EdgeFaultLab's CI: that would mean cloning and starting a
second project on every push, and the cross-repo result belongs to that project,
not to this one.

## Output and reports

```text
runs/<run_id>/
  events.jsonl     every event, one JSON object per line, with relative times
  summary.json     counters, assertion results, PIDs, recovery time - for CI
  report.md        fault timeline, recovery, assertions, result - for humans
  logs/<name>.log  stdout/stderr of every process the run started
```

Recorded events include `PROCESS_START/EXIT/KILL/RESTART`,
`CONNECTION_OPEN/CLOSE`, `MESSAGE_RECEIVED/FORWARDED/DROPPED/DELAYED/DUPLICATED/
REORDERED/MUTATED`, `MALFORMED_MESSAGE`, `MESSAGE_TOO_LARGE`, the fault
lifecycle `FAULT_SCHEDULED/ACTIVATED/COMPLETED`, every delivery a fault matched,
and one `ASSERTION_PASS` or `ASSERTION_FAIL` per assertion at the end.
Message bodies are truncated at 16 KB and flagged `truncated: true`, so a run
cannot fill the disk.

**Recovery time** is the gap between the moment a disruptive fault *took
effect* - `MESSAGE_DROPPED`, `LINK_DISCONNECTED`, `LINK_DOWN` or `PROCESS_KILL`,
not merely "the fault was armed" - and the system's first sign of life
afterwards. That sign of life is the first delivery that satisfies one of the
scenario's `eventually` assertions, judged with the same link, direction, match
filter and time window as that assertion. A scenario with no `eventually`
assertion falls back to the first message delivered after the disruption.

## Determinism

```json
{"action": "drop", "probability": 0.3, "match": {"type": "CONTROL_COMMAND"}}
```

The same scenario, the same seed and the same message order produce the same
fault decisions. Every fault owns a `random.Random` seeded from `(seed, fault
id)`; the global random state is never touched, so nothing else in the process
can shift a decision.

What the seed does **not** promise: an identical run of the *whole system*.
Process scheduling, TCP timing and retransmissions are still the operating
system's business, and the system under test has a say too. EdgeFaultLab
guarantees that *its own* decisions are reproducible - not that every millisecond
of a run is. The event trace carries a timestamp on every line precisely because
the timing around those decisions still varies.

## Safety boundaries

* **Loopback only.** Links bind `127.0.0.1` / `::1` / `localhost`. Binding an
  external interface is not a v0.1 feature, and `validate` refuses it.
* **Only its own children.** `process_kill` and `process_restart` name a process
  declared in the scenario. There is no `pid` field, and the validator rejects
  one - EdgeFaultLab can only kill processes it started itself.
* **No system-level networking.** v0.1 never calls `tc`, `netem`, `iptables`,
  `pfctl`, a firewall, or a root network namespace.
* **No silent failures.** A scenario problem, an unreachable upstream, a port
  already in use, a process that will not start, an overflowing buffer: all of
  them are reported and produce a non-zero exit code. Nothing is swallowed.

## Limitations

EdgeFaultLab v0.1 tests the **software-visible failure semantics** of a system.
It is not a hardware, radio or electrical test bench. In particular it does not:

* inject real RF interference or emulate a LoRa PHY,
* corrupt real IP packets, or use `tc` / `netem` / packet capture,
* simulate CPU brownout, flash corruption or power loss,
* test electrical faults,
* replace hardware validation or field testing,
* reproduce an exact millisecond-level schedule of a real deployment.

It also needs the system to be reachable over TCP with newline-delimited JSON
messages, and it can only inject faults on links you route through it.

The proxy does not implement half-open HTTP-style upgrades, TLS termination or
protocol-aware framing: it copies newline-delimited lines. Process management is
exercised on macOS and Linux (CI runs `ubuntu-latest`); the Windows path uses
`CTRL_BREAK_EVENT` / `terminate()` and is best effort.

One message may be at most 1 MiB (`MAX_MESSAGE_SIZE`). The boundary is exact: a
line of exactly 1 MiB is delivered, 1 MiB + 1 byte is refused with
`MESSAGE_TOO_LARGE` and the connection is closed, whether or not the oversized
line was newline terminated.

## Not implemented

Deliberately out of scope for v0.1: a web dashboard, Grafana / Prometheus,
Docker, Kubernetes, Chaos Mesh, an MQTT broker, LoRa / CAN / BLE / serial
simulators, UDP, QUIC, gRPC, an HTTP proxy, packet capture and PCAP, kernel or
root network manipulation, distributed agents, a cloud service, authentication,
a database, a plugin marketplace, and anything involving an LLM or fault
prediction.

v0.1 is local, TCP, newline JSON, processes, faults, assertions, reports.

## Project layout and tests

```text
edgefaultlab/                10 source files, standard library only
scenarios/                   4 runnable demo scenarios + 1 template
examples/demo_system/        the three node demo system
examples/smart_agriculture/  5 integration scenarios for another repository
tests/                       49 tests, including real-TCP end-to-end runs
```

```bash
pip install -e ".[dev]"
pytest
```

## License

MIT.
