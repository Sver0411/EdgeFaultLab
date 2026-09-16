# Demo system

A three process Edge / IoT style system, small enough to read in one sitting and
real enough to break in interesting ways.  It exists so that EdgeFaultLab has
something to test that is not "this repository's own unit tests".

```text
producer  ------>  relay  ------>  consumer
  (B1)             (A1)             (C1)
  sends           forwards          executes, idempotent
  CONTROL_COMMAND  messages         replies ACK + CONTROL_RESULT
  retries on ACK
  timeout
```

All three speak newline delimited JSON over TCP and use nothing but the Python
standard library.

## Why these three nodes

* the **producer** retries with the *same* `message_id` and `command_id` when it
  does not get an ACK, which is what makes idempotency observable,
* the **relay** is a dumb forwarder, so killing it hurts the whole path - and it
  is a process EdgeFaultLab started, so a scenario may kill it,
* the **consumer** refuses to execute the same `command_id` twice and rejects
  commands whose `timestamp` is older than its TTL, which is exactly what the
  `duplicate` and `timestamp_offset` faults are meant to poke at.

## Running it by hand

```bash
python examples/demo_system/consumer.py --port 9601
python examples/demo_system/relay.py --listen 127.0.0.1:9600 --upstream 127.0.0.1:9601
python examples/demo_system/producer.py --connect 127.0.0.1:9600
```

Point `--connect` at an EdgeFaultLab link instead of at the relay and the same
three processes become a resilience test - see `scenarios/*.json`.
