# Using EdgeFaultLab with Smart Agriculture Edge AI

These scenarios show how to point EdgeFaultLab at a *real* multi-node Edge
system: [Smart-Agriculture-Edge-AI](https://github.com/Sver0411/Smart-Agriculture-Edge-AI)
(server, gateways A1 / A2, sensor nodes B1 / B2, controllers C1 / C2).

**No source of that project is copied here.**  Every scenario is a JSON file
that starts the other repository's own entry points, so you keep testing the
code you actually ship.  Before running one, edit `cwd` in the `processes`
block so it points at your checkout:

```json
{"name": "gateway_a1", "command": ["python3", "-m", "gateway.gateway", "--id", "A1"],
 "cwd": "/path/to/Smart-Agriculture-Edge-AI"}
```

## Why the ports look odd

EdgeFaultLab can only inject faults into traffic that crosses it, and the nodes
of the agriculture system dial hard-coded ports (`gateway A1` is `9201`, the
server is `9100`).  The trick is to leave the *client* side of the topology
untouched and move the real process one port over, so the proxy can sit on the
port everybody already dials:

```text
B1 / C1 ------> 127.0.0.1:9201  EdgeFaultLab  ------> 127.0.0.1:9301  real A1
A1 / A2 ------> 127.0.0.1:9100  EdgeFaultLab  ------> 127.0.0.1:9400  real server
```

| Node | Started with | Why |
| --- | --- | --- |
| `server` | `--port 9400` | the proxy owns `9100`, which both gateways dial |
| `gateway_a1` | `--port 9301 --server-port 9100 --peer-port 9202` | its node-side port is proxied |
| `gateway_a2` | `--port 9202 --server-port 9100 --peer-port 9201` | talks to A1 through the proxy |

The `gateways_to_server` link therefore sees everything both gateways upload:
heartbeats, sensor data, alerts - which is what makes "is the cloud still being
served?" an observable assertion.

## The scenarios

| Scenario | Injected fault | What it checks |
| --- | --- | --- |
| `gateway_failover.json` | kill gateway A1 at 10s | the mesh keeps reporting, nothing is executed twice |
| `server_outage.json` | server down 10s..20s | the LAN keeps working while the cloud is gone, then catches up |
| `duplicate_control_command.json` | duplicate a `CONTROL_COMMAND` | one `command_id`, one execution |
| `stale_command.json` | `timestamp_offset -30000 ms` | the controller refuses an expired command |
| `lost_command.json` | drop the first `CONTROL_COMMAND` | ACK timeout, retry, eventual execution, one execution |

## Run one

```bash
edgefaultlab validate examples/smart_agriculture/gateway_failover.json
edgefaultlab run examples/smart_agriculture/gateway_failover.json --output runs/failover-001
```

Everything the run learned lands in `runs/failover-001/`: `events.jsonl` (the
full timeline), `summary.json` (counters, for CI) and `report.md` (for a human).
