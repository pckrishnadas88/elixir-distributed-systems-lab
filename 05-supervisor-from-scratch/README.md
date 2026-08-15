# 05 - Supervisor From Scratch

A minimal one-for-one supervisor implemented using raw Elixir process primitives.

This example intentionally avoids `Supervisor`, `GenServer`, `Task`, and other OTP abstractions to demonstrate how a process can observe linked workers and restart a failed child.

---

## Learning Objective

Understand how a process can:

- start and link multiple worker processes
- trap exit signals instead of crashing
- detect child termination through `:EXIT` messages
- restart only the worker that failed
- preserve healthy sibling processes
- distinguish abnormal failure from normal shutdown

This example explains the basic mechanism that OTP supervisors provide in a tested, configurable, production-ready form.

---

## Problem

A long-running worker can crash while processing a message:

```elixir
send(worker, :crash)
```

Without supervision, the worker disappears and no longer accepts work.

The parent could periodically check `Process.alive?/1`, but that requires polling and still leaves restart logic scattered through the application.

Instead, the parent links itself to each worker:

```elixir
pid = spawn_link(fn -> worker_loop() end)
```

and converts exit signals into ordinary mailbox messages:

```elixir
Process.flag(:trap_exit, true)
```

When one worker crashes, the parent receives:

```elixir
{:EXIT, pid, reason}
```

It can identify that worker and start a replacement.

---

## Architecture

```text
                 +------------------------+
                 |   Manual Supervisor    |
                 |------------------------|
                 | trap_exit = true       |
                 | child PID -> name map  |
                 | restart loop           |
                 +------------------------+
                    /                  \
              link /                    \ link
                  v                      v
        +------------------+    +------------------+
        |     worker_1     |    |     worker_2     |
        |------------------|    |------------------|
        | receive loop     |    | receive loop     |
        | private state    |    | private state    |
        +------------------+    +------------------+
```

If `worker_1` crashes, only that child is replaced:

```text
worker_1 crashes
       |
       | {:EXIT, worker_1_pid, reason}
       v
manual supervisor
       |
       | spawn_link replacement
       v
new worker_1

worker_2 keeps the same PID throughout.
```

This is the central idea of a one-for-one restart strategy.

---

## Protocol

### Worker Messages

```elixir
{:work, caller_pid, value}
:crash
:stop
```

### Worker Response

```elixir
{:done, worker_name, value, processed_count}
```

### Supervisor Messages

```elixir
{:children, caller_pid, reference}
{:EXIT, child_pid, reason}
:stop
```

### Supervisor Response

```elixir
{:children, reference, %{worker_name => child_pid}}
```

---

## Message Flow

For an abnormal worker exit:

```text
Demo                  Supervisor                 worker_1

send(worker_1, :crash)
----------------------------------------------->

                                               raises
                                               exits

                      {:EXIT, pid, reason}
                      <-------------------------- BEAM

                      remove old PID
                      spawn_link new worker_1

children(supervisor)
--------------------->

{:children, ref,
 %{worker_1: new_pid,
   worker_2: same_pid}}
<---------------------
```

The supervisor does not restart a child that exits with reason `:normal`. This prevents an intentional shutdown from being treated as a failure.

---

## Implementation

The supervisor first enables exit trapping:

```elixir
Process.flag(:trap_exit, true)
```

It starts each worker with a link and records its identity:

```elixir
pid = Worker.start(name)
children = Map.put(children, pid, name)
```

When a child fails, the supervisor removes the dead PID and starts the same logical worker again:

```elixir
{:EXIT, pid, reason} ->
  name = Map.fetch!(children, pid)
  children = Map.delete(children, pid)
  new_pid = start_child(name)
  loop(Map.put(children, new_pid, name))
```

The replacement receives a new PID and begins with fresh state. The other child is not restarted.

---

## Running

```bash
elixir supervisor.exs
```

Example output includes:

```text
supervisor: started worker_1 as #PID<...>
supervisor: started worker_2 as #PID<...>
supervisor: worker_1 exited with {%RuntimeError{message: "worker_1 crashed"}, ...}
supervisor: restarted worker_1 as #PID<...>
worker_1 restarted: true
worker_2 unchanged: true
reply: {:done, :worker_1, :job_after_restart, 1}
```

An exception report is expected because `worker_1` intentionally crashes.

PIDs and stack-trace details vary between runs.

---

## Key Concepts

### Manual Restart Loop

The supervisor stays alive in a recursive receive loop. Each child exit becomes a message that the loop can inspect and handle.

### One-for-One Restart Strategy

Only the failed child is restarted. Healthy siblings keep running with the same PIDs and state.

### Links and Exit Trapping

Links propagate failures. `trap_exit` changes incoming exit signals into `:EXIT` messages so the supervisor can respond instead of terminating.

### Fresh Worker State

Restarting creates a new process. Any state stored only inside the crashed worker is lost unless it can be reconstructed from another source.

### Why OTP Supervisors Exist

The example implements only the smallest useful subset of supervision. Real systems also need:

- restart intensity limits to prevent infinite crash loops
- multiple restart strategies
- deterministic child start and shutdown order
- shutdown timeouts
- child specifications
- supervisor initialization guarantees
- structured error reports and introspection

OTP's `Supervisor` provides these behaviors consistently, so production applications should use it instead of maintaining a custom restart loop.

---

## Limitations

- A worker that repeatedly crashes will be restarted forever.
- Restarted workers lose their in-memory state.
- The supervisor has no nested supervision tree.
- There is no restart delay or backoff.
- There are no restart-frequency limits.
- The child registry is only a private map inside the supervisor process.

These limitations are intentional. They show why supervision is more than simply calling `spawn_link/1` again.

---

## Next Step

Project 06 introduces named processes so callers can find a long-running process without passing its PID directly.

---

## References

- Previous: Process Linking
- Next: Named Processes
- Elixir documentation: [`Supervisor`](https://hexdocs.pm/elixir/Supervisor.html)
- Elixir documentation: [`Process.flag/2`](https://hexdocs.pm/elixir/Process.html#flag/2)
