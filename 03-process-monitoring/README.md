# 03 - Process Monitoring

A minimal process monitoring example implemented using raw Elixir process primitives.

This example intentionally avoids `GenServer`, `Supervisor`, `Task`, and other OTP abstractions to demonstrate how one process can detect when another process terminates.

---

## Learning Objective

Understand how a process can:

* monitor another process
* detect normal process termination
* detect process crashes
* receive `:DOWN` messages
* identify monitored processes using monitor references
* monitor multiple workers concurrently

This example forms the foundation for understanding failure detection and supervision in OTP.

---

## Problem

Processes can terminate at any time.

A worker may finish normally:

```elixir
send(worker, :stop)
```
---


or crash unexpectedly:

```elixir
send(worker, :crash)
```

Another process may need to know when this happens.

Repeatedly checking:

```elixir
Process.alive?(worker)
```

would require polling.

Instead, the BEAM allows one process to monitor another process.

```elixir
ref = Process.monitor(worker)
```

When the worker terminates, the monitoring process receives a message describing the termination.

---

## Architecture

```text
                +-----------------------+
                |    Worker Process     |
                |-----------------------|
                | receive loop          |
                | mailbox               |
                +-----------------------+
                         ▲
                         │
                      monitor
                         │
                +-----------------------+
                |  Monitoring Process   |
                |-----------------------|
                | monitor reference     |
                | mailbox               |
                +-----------------------+
```

When the worker terminates:

```text
                +-----------------------+
                |    Worker Process     |
                |-----------------------|
                |      terminated       |
                +-----------------------+
                         │
                         │ detected by BEAM
                         ▼
                +-----------------------+
                |         BEAM          |
                +-----------------------+
                         │
                         │ {:DOWN, ...}
                         ▼
                +-----------------------+
                |  Monitoring Process   |
                +-----------------------+
```

---

## Protocol

### Worker Messages

```elixir
{:work, caller_pid, value}
:stop
:crash
```

### Worker Response

```elixir
{:done, name, value}
```

### Monitor Notification

When a monitored process terminates, the BEAM sends:

```elixir
{:DOWN, ref, :process, pid, reason}
```

---

## Message Flow

For a normal termination:

```text
Monitoring Process                 Worker

Process.monitor(worker)

send(worker, :stop)
--------------------------------->

                              receive

                              :stop

                              returns :ok

                              process exits

              BEAM detects termination

{:DOWN, ref, :process,
 worker, :normal}
<---------------------------------

receive

reason = :normal
```

The `:stop` message goes to the worker's mailbox.

The `:DOWN` message goes to the monitoring process's mailbox.

---

## Implementation

A worker is started using:

```elixir
spawn(fn -> loop(name) end)
```

The new process enters its receive loop:

```elixir
loop(name)
```

and waits for messages.

The monitoring process creates a monitor using:

```elixir
ref = Process.monitor(worker)
```

The returned reference uniquely identifies that monitor.

When the worker terminates, the monitoring process receives:

```elixir
receive do
  {:DOWN, ^ref, :process, ^worker, reason} ->
    ...
end
```

The worker and monitoring process remain separate processes.

Monitoring does not cause the monitoring process to terminate when the worker crashes.

---

## Running

```bash
elixir monitor.exs
```

The script runs three examples:

```elixir
MonitorDemo.normal_exit()
MonitorDemo.crash_exit()
MonitorDemo.multiple_workers()
```

Example output will include:

```text
worker-1 stopping normally
worker-1 terminated
reason: :normal
```

The crash example also produces an expected exception report because the worker intentionally crashes.

The multiple-worker example logs a `:DOWN` notification for every monitored worker.

The exact order of worker termination messages may vary because the workers execute concurrently.

---

## Key Concepts

### Monitor

A monitor creates one-way failure observation between processes.

```elixir
ref = Process.monitor(worker)
```

The process calling `Process.monitor/1` becomes the monitoring process.

---

### Monitor Reference

`Process.monitor/1` returns a unique reference.

```elixir
ref = Process.monitor(worker)
```

This reference identifies the particular monitor.

When the worker terminates, the same reference appears in:

```elixir
{:DOWN, ref, :process, worker, reason}
```

This becomes important when monitoring multiple processes.

---

### `:DOWN` Message

When a monitored process terminates, the BEAM sends:

```elixir
{:DOWN, ref, :process, pid, reason}
```

to the monitoring process.

For normal termination:

```elixir
reason == :normal
```

For a crash, `reason` contains information describing the failure.

---

### Normal Exit

The worker handles:

```elixir
:stop ->
  IO.puts("#{name} stopping normally")
  :ok
```

There is no recursive call to:

```elixir
loop(name)
```

so the worker function finishes and the process terminates normally.

The monitor receives a `:DOWN` message with:

```elixir
:normal
```

as the reason.

---

### Crash

The worker can intentionally crash:

```elixir
:crash ->
  raise "#{name} crashed"
```

The worker process terminates abnormally.

The monitoring process does not crash with it.

Instead, it receives a `:DOWN` message containing the failure reason.

---

### Monitoring Multiple Workers

Each worker gets its own monitor reference.

```elixir
ref1 = Process.monitor(worker1)
ref2 = Process.monitor(worker2)
ref3 = Process.monitor(worker3)
```

The references are stored in a map:

```elixir
%{
  ref1 => "worker-1",
  ref2 => "worker-2",
  ref3 => "worker-3"
}
```

This map represents the workers whose termination notifications are still expected.

---

### Waiting for Workers

`wait_for_workers/1` waits for any monitored worker to terminate.

```elixir
receive do
  {:DOWN, ref, :process, pid, reason} ->
    ...
end
```

When a worker terminates, its reference is removed:

```elixir
Map.pop(monitors, ref)
```

The function then waits again using recursion:

```elixir
wait_for_workers(remaining)
```

The process continues until the monitor map is empty.

```text
3 workers
    ↓
receive :DOWN
    ↓
2 workers
    ↓
receive :DOWN
    ↓
1 worker
    ↓
receive :DOWN
    ↓
0 workers
    ↓
finished
```

---

## Why not check `Process.alive?/1`?

A process could repeatedly check:

```elixir
Process.alive?(worker)
```

but this requires polling.

There is also a timing problem: knowing that a process was alive at one instant does not guarantee that it will still be alive immediately afterward.

Monitoring instead lets the BEAM notify us when termination occurs.

```text
Process.monitor(worker)

        ↓

worker runs

        ↓

worker terminates

        ↓

BEAM sends :DOWN
```

---

## Monitor vs Link

A monitor observes another process.

If the monitored worker crashes:

```text
Worker
   ↓
crashes
   ↓
Monitoring process receives :DOWN
   ↓
Monitoring process stays alive
```

A monitor therefore provides failure detection without automatically propagating the failure.

Process links behave differently and are introduced in the next example.

---

## Limitations

This implementation is intentionally simple.

It does **not** provide:

* automatic process restart
* supervision
* restart strategies
* process registration
* distributed monitoring across application nodes
* production worker management

These problems are solved progressively in later examples.

---

## Next Example

**04 - Process Linking**

The next example introduces `spawn_link/1` and process links to demonstrate how failures can propagate between connected processes.

---

## References

- Blog post: [Process monitoring in Elixir](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)

- Adjacent posts:
  - Previous: [Correlated request–reply in Elixir](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)
  - Next: [04 - Process Linking](../04-process-linking/README.md)

* Erlang Processes
* Erlang Process Monitoring
* Elixir `spawn/1`
* Elixir `send/2`
* Elixir `receive`
* Elixir `Process.monitor/1`
* Elixir `Process.alive?/1`
