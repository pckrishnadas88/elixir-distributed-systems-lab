# 04 - Process Linking

A minimal process-linking example implemented using raw Elixir process primitives.

This example intentionally avoids `GenServer`, `Supervisor`, `Task`, and other OTP abstractions to demonstrate how failures propagate between linked processes.

---

## Learning Objective

Understand how processes can:

* create linked processes using `spawn_link/1`
* propagate abnormal exits
* terminate together when one linked process crashes
* convert exit signals into mailbox messages
* use `Process.flag(:trap_exit, true)`
* distinguish process links from process monitors

This example forms the foundation for understanding supervision and process restart strategies in OTP.

---

## Problem

Processes can depend on each other.

Suppose a parent process starts a worker:

```elixir
worker =
  spawn(fn ->
    perform_work()
  end)
```

If the worker crashes, the parent may need to know that an important part of its work has failed.

A process monitor can notify the parent:

```elixir
Process.monitor(worker)
```

but monitoring only observes the failure. It does not connect the lifecycles of the two processes.

Sometimes two processes should be treated as part of the same operation.

If one process fails, the other process should also terminate instead of continuing in an invalid state.

Elixir provides process links for this purpose:

```elixir
spawn_link(fn ->
  perform_work()
end)
```

When a linked process terminates abnormally, an exit signal propagates through the link.

---

## Architecture

```text
                +-----------------------+
                |    Parent Process     |
                |-----------------------|
                | starts worker         |
                | linked to worker      |
                +-----------------------+
                         ▲
                         │
                         │ bidirectional link
                         │
                         ▼
                +-----------------------+
                |    Worker Process     |
                |-----------------------|
                | performs work         |
                | may crash             |
                +-----------------------+
```

When the worker crashes without exit trapping:

```text
                +-----------------------+
                |    Worker Process     |
                |-----------------------|
                |       crashes         |
                +-----------------------+
                         │
                         │ exit signal
                         ▼
                +-----------------------+
                |    Parent Process     |
                |-----------------------|
                |      terminates       |
                +-----------------------+
```

When the parent traps exits:

```text
                +-----------------------+
                |    Worker Process     |
                |-----------------------|
                |       crashes         |
                +-----------------------+
                         │
                         │ exit signal
                         ▼
                +-----------------------+
                |    Parent Process     |
                |-----------------------|
                | receives {:EXIT, ...} |
                | remains alive         |
                +-----------------------+
```

---

## Protocol

The script supports two command-line modes:

```text
propagate
trap
```

### Propagate Mode

```bash
elixir process_linking.exs propagate
```

The worker crashes, and the abnormal exit propagates to the parent.

### Trap Mode

```bash
elixir process_linking.exs trap
```

The parent traps exit signals and receives:

```elixir
{:EXIT, worker_pid, reason}
```

as a regular mailbox message.

---

## Message Flow

### Without Exit Trapping

```text
Parent Process                    Worker Process

spawn_link(fn -> ... end)
--------------------------------->

                            worker starts

                            Process.sleep(500)

                            raise "Worker crashed"

                            worker terminates
                            abnormally

              exit signal propagates

parent receives exit signal
and terminates
```

The parent does not receive a regular Elixir message.

It receives an exit signal because the processes are linked.

Since the parent is not trapping exits, the signal terminates it.

---

### With Exit Trapping

```text
Parent Process                    Worker Process

Process.flag(
  :trap_exit,
  true
)

spawn_link(fn -> ... end)
--------------------------------->

                            worker starts

                            Process.sleep(500)

                            raise "Worker crashed"

                            worker terminates
                            abnormally

              exit signal propagates

exit signal converted into:

{:EXIT, worker, reason}

receive

parent remains alive
```

The worker still crashes.

Only the parent’s response to the exit signal changes.

---

## Implementation

The script contains two examples:

```elixir
ProcessLinking.run("propagate")
ProcessLinking.run("trap")
```

The selected example is determined using the first command-line argument:

```elixir
mode = List.first(System.argv())
```

The mode is passed to:

```elixir
ProcessLinking.run(mode)
```

---

## Code

Create `process_linking.exs`:

```elixir
defmodule ProcessLinking do
  def run("propagate") do
    IO.puts("Parent started: #{inspect(self())}")

    worker =
      spawn_link(fn ->
        IO.puts("Worker started: #{inspect(self())}")
        Process.sleep(500)

        raise "Worker crashed"
      end)

    IO.puts("Parent linked to worker: #{inspect(worker)}")
    IO.puts("The worker crash will terminate the parent.")

    Process.sleep(:infinity)
  end

  def run("trap") do
    Process.flag(:trap_exit, true)

    IO.puts("Parent started: #{inspect(self())}")
    IO.puts("Parent is trapping exits")

    worker =
      spawn_link(fn ->
        IO.puts("Worker started: #{inspect(self())}")
        Process.sleep(500)

        raise "Worker crashed"
      end)

    IO.puts("Parent linked to worker: #{inspect(worker)}")

    receive do
      {:EXIT, ^worker, reason} ->
        IO.puts("Parent received an EXIT message.")
        IO.puts("Worker: #{inspect(worker)}")
        IO.puts("Reason: #{inspect(reason)}")
        IO.puts("Parent is still alive.")
    end
  end

  def run(_) do
    IO.puts("""
    Usage:

      elixir process_linking.exs propagate
      elixir process_linking.exs trap
    """)
  end
end

mode = List.first(System.argv())

ProcessLinking.run(mode)
```

---

## Running

The script provides two separate examples.

### Exit Propagation

Run:

```bash
elixir process_linking.exs propagate
```

Example output:

```text
Parent started: #PID<0.95.0>
Parent linked to worker: #PID<0.102.0>
Worker started: #PID<0.102.0>
The worker crash will terminate the parent.
** (EXIT from #PID<0.95.0>) an exception was raised:
    ** (RuntimeError) Worker crashed
        process_linking.exs:9: anonymous fn/0 in ProcessLinking.run/1
```

The exact process identifiers may change each time the program runs.

The worker intentionally crashes, so the exception report is expected.

Because the worker is linked to the parent, the worker’s abnormal exit terminates the parent.

---

### Trapping Exits

Run:

```bash
elixir process_linking.exs trap
```

Example output:

```text
Parent started: #PID<0.95.0>
Parent is trapping exits
Parent linked to worker: #PID<0.102.0>
Worker started: #PID<0.102.0>
Parent received an EXIT message.
Worker: #PID<0.102.0>

11:40:13.501 [error] Process #PID<0.102.0> raised an exception
** (RuntimeError) Worker crashed
    process_linking.exs:27: anonymous fn/0 in ProcessLinking.run/1
Reason: {%RuntimeError{message: "Worker crashed"}, [{ProcessLinking, :"-run/1-fun-1-", 0, [file: ~c"process_linking.exs", line: 27, error_info: %{module: Exception}]}]}
Parent is still alive.
```

The exact process identifiers, timestamp, line numbers, and stack trace may vary.

The error report is expected because the worker still crashes.

The parent receives the exit information as a mailbox message and remains alive.

---

## Key Concepts

### Process Link

A link establishes a bidirectional relationship between two processes.

```elixir
spawn_link(fn ->
  perform_work()
end)
```

This operation:

1. creates a new process
2. links the new process to the caller
3. returns the new process PID

It combines:

```elixir
spawn/1
```

and:

```elixir
Process.link/1
```

into one atomic operation.

---

### `spawn_link/1`

The worker is created using:

```elixir
worker =
  spawn_link(fn ->
    raise "Worker crashed"
  end)
```

The calling process becomes linked to the new worker.

```text
Parent Process ↔ Worker Process
```

The relationship is bidirectional.

An abnormal exit from the worker can affect the parent, and an abnormal exit from the parent can affect the worker.

---

### Exit Propagation

The worker intentionally crashes:

```elixir
raise "Worker crashed"
```

This terminates the worker with an abnormal exit reason.

Because the worker is linked to the parent, an exit signal propagates to the parent.

```text
worker raises exception
        ↓
worker terminates abnormally
        ↓
exit signal travels through link
        ↓
parent terminates
```

This is why the first example ends with:

```text
** (EXIT from #PID<0.95.0>) an exception was raised
```

---

### Why the Parent Sleeps Forever

The propagation example ends with:

```elixir
Process.sleep(:infinity)
```

This keeps the parent process alive while waiting for the linked worker.

Without it, the parent function could finish before the worker crashes.

The sleep does not handle any messages. It only prevents the parent from exiting normally.

---

### Trapping Exits

The parent enables exit trapping using:

```elixir
Process.flag(:trap_exit, true)
```

This changes how the current process responds to incoming exit signals.

Without exit trapping:

```text
exit signal
    ↓
parent terminates
```

With exit trapping:

```text
exit signal
    ↓
converted into {:EXIT, pid, reason}
    ↓
placed in parent mailbox
```

Only the process calling `Process.flag/2` is affected.

It does not enable exit trapping globally.

---

### `{:EXIT, pid, reason}` Message

When exit trapping is enabled, a linked process’s exit signal becomes:

```elixir
{:EXIT, pid, reason}
```

The parent receives it using:

```elixir
receive do
  {:EXIT, ^worker, reason} ->
    ...
end
```

The message contains:

* `:EXIT` — identifies the message as a trapped exit
* `worker` — PID of the process that terminated
* `reason` — information explaining why it terminated

---

### Pinning the Worker PID

The receive pattern uses the pin operator:

```elixir
{:EXIT, ^worker, reason}
```

The pin operator ensures that the message belongs to the specific worker stored in the `worker` variable.

Without the pin operator:

```elixir
{:EXIT, worker, reason}
```

the pattern would bind `worker` to any PID contained in an exit message.

Using:

```elixir
^worker
```

means:

> Match only an exit message from this particular worker.

---

### Exit Reason

For an exception, the reason contains the exception and its stack trace:

```elixir
{%RuntimeError{message: "Worker crashed"}, stacktrace}
```

This information allows the parent to inspect why the worker terminated.

A normal process termination usually has:

```elixir
:normal
```

as its exit reason.

---

### The Worker Still Crashes

Exit trapping does not prevent the worker from crashing.

The worker executes:

```elixir
raise "Worker crashed"
```

and terminates.

This is why Elixir still logs:

```text
[error] Process #PID<0.102.0> raised an exception
```

Exit trapping only changes what happens to the parent.

```text
Worker crashes
      ↓
Worker terminates
      ↓
Parent receives exit message
      ↓
Parent remains alive
```

It does not recover or restart the failed worker.

---

### Exit Trapping vs `try/catch`

Exit trapping may appear similar to `try/catch`, but they handle different situations.

A `try` block handles a failure occurring inside the current process:

```elixir
try do
  raise "failure"
rescue
  error ->
    IO.inspect(error)
end
```

Exit trapping handles an exit signal arriving from another linked process:

```elixir
Process.flag(:trap_exit, true)

receive do
  {:EXIT, pid, reason} ->
    IO.inspect({pid, reason})
end
```

The worker’s exception is not caught by the parent.

The worker crashes, and the parent handles the resulting exit signal.

A useful mental model is:

> Notify this process when a linked process exits instead of automatically terminating this process.

---

### Links Are Bidirectional

Links work in both directions:

```text
Parent Process ↔ Worker Process
```

If the worker terminates abnormally, the exit signal reaches the parent.

If the parent terminates abnormally, the exit signal reaches the worker.

This allows a group of related processes to fail together.

---

## Monitor vs Link

A monitor creates one-way failure observation:

```text
Monitoring Process → Worker
```

When the worker crashes:

```text
Worker crashes
      ↓
Monitoring process receives :DOWN
      ↓
Monitoring process remains alive
```

A link creates a bidirectional failure relationship:

```text
Parent ↔ Worker
```

When the worker crashes:

```text
Worker crashes
      ↓
Exit signal propagates
      ↓
Parent normally terminates
```

The parent can remain alive only if it traps the exit signal.

| Feature                          | Monitor             | Link                               |
| -------------------------------- | ------------------- | ---------------------------------- |
| Relationship                     | One-way             | Bidirectional                      |
| Creation                         | `Process.monitor/1` | `spawn_link/1` or `Process.link/1` |
| Notification                     | `{:DOWN, ...}`      | Exit signal                        |
| Default observer behaviour       | Remains alive       | Terminates on abnormal exit        |
| Reference returned               | Yes                 | No                                 |
| Can receive failure as a message | Automatically       | Requires `trap_exit`               |

---

## Why Link Processes?

Links are useful when processes depend on each other.

Suppose a process coordinates a job using a worker.

If the worker crashes, allowing the coordinator to continue may leave the job incomplete or its state inconsistent.

A link allows the failure to propagate:

```text
Critical worker fails
        ↓
Dependent process also terminates
        ↓
Higher-level process can handle failure
```

OTP supervisors use process links and exit trapping to detect failed child processes and restart them.

---

## Limitations

This implementation is intentionally simple.

It does **not** provide:

* automatic worker restart
* supervision trees
* restart intensity limits
* one-for-one restart strategies
* one-for-all restart strategies
* process registration
* multiple linked-worker management
* production error handling

These problems are solved progressively in later examples.

---

## Next Example

**05 - Supervisor From Scratch**

The next example uses process links and exit trapping to build a basic supervisor that detects a failed worker and starts a replacement.

---

## References

* Blog post: [Process linking in Elixir](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-4-process-linking-okb)

* Adjacent posts:

  * Previous: [Process monitoring in Elixir](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)
  * Next: [Supervisor from scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh)

* Erlang Processes

* Erlang Process Links

* Erlang Errors and Error Handling

* Elixir `spawn_link/1`

* Elixir `Process.link/1`

* Elixir `Process.flag/2`

* Elixir `send/2`

* Elixir `receive`
