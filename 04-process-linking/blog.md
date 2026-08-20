# Building Distributed Systems in Elixir: Part 4 — Process Linking

In the previous part of this series, we used process monitors to observe when another process terminated.

A monitor sends a message like this:

```elixir
{:DOWN, ref, :process, pid, reason}
```

The monitoring process stays alive and decides what to do with that information.

But sometimes observation is not enough.

Two processes may be part of the same operation. If one of them fails, allowing the other to continue could leave the system in an invalid state.

For that relationship, the BEAM provides **process links**.

In this part, we'll build two small examples using:

```elixir
spawn_link/1
Process.flag(:trap_exit, true)
```

No `GenServer`.

No `Supervisor`.

The goal is to understand how failures propagate between processes before introducing OTP supervision.

---

## The Problem

Imagine a parent process starts a worker:

```elixir
worker =
  spawn(fn ->
    perform_work()
  end)
```

The two processes execute independently.

If the worker crashes, the parent is unaffected unless it explicitly monitors the worker or checks its state.

```text
Parent Process                 Worker Process

      alive                       working
        |                            |
        |                            X crash
        |
      alive
```

That independence is useful when the processes do unrelated work.

But suppose the worker is essential to the parent's operation. Continuing without it would not make sense.

What we want is a shared failure relationship:

```text
worker fails
     |
     v
parent is affected
```

This is what process links provide.

---

## Creating a Linked Process

We can create and link a process in one operation:

```elixir
worker =
  spawn_link(fn ->
    perform_work()
  end)
```

Conceptually, `spawn_link/1` combines:

```elixir
spawn/1
Process.link/1
```

The important difference is that spawning and linking happen atomically. There is no gap in which the child could terminate before the link is established.

After `spawn_link/1`, the processes have a bidirectional relationship:

```text
Parent Process <----------> Worker Process
                  link
```

The word **bidirectional** matters.

The link does not represent ownership or a one-way subscription. An abnormal exit from either process can affect the other.

---

## Exit Propagation

Let's start a linked worker that intentionally crashes:

```elixir
worker =
  spawn_link(fn ->
    IO.puts("Worker started: #{inspect(self())}")
    Process.sleep(500)

    raise "Worker crashed"
  end)
```

The worker raises an exception:

```elixir
raise "Worker crashed"
```

That causes the worker to terminate with an abnormal exit reason.

Because the worker is linked to the parent, the BEAM sends an exit signal across the link.

```text
Parent Process                    Worker Process

spawn_link(fn -> ... end)
--------------------------------->

                            worker starts

                            Process.sleep(500)

                            raise "Worker crashed"

                            worker terminates
                            abnormally
                                  X
                                  |
                     exit signal  |
                    <-------------+
        |
        X parent terminates
```

By default, an abnormal exit signal terminates the receiving process too.

The parent does not receive an ordinary mailbox message in this mode. The exit signal directly affects its lifecycle.

---

## A Propagation Example

Here is the first mode from our script:

```elixir
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
```

The parent sleeps forever here so that it remains alive while the worker runs:

```elixir
Process.sleep(:infinity)
```

Without that line, the parent function could finish normally before the worker reaches the intentional crash.

The sleep is not responsible for detecting the failure. It simply keeps the parent alive long enough for the linked exit signal to arrive.

Run the example with:

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
```

The exact PIDs, stack trace, and output order may vary between runs because the processes execute concurrently.

The important result is the same:

```text
worker crashes -> parent terminates
```

---

## Normal and Abnormal Exits

Links propagate exit signals, but the exit reason matters.

A process that finishes normally exits with:

```elixir
:normal
```

Normal exits do not ordinarily take down linked processes that are not trapping exits.

An exception produces an abnormal reason. In our example, that reason contains the `RuntimeError` and stack trace.

Other important reasons include:

```elixir
:shutdown
:killed
```

So the simplified rule for an ordinary process is:

```text
linked process exits normally    -> remain alive
linked process exits abnormally  -> terminate
```

There are special cases—particularly untrappable `:kill` signals—but this model is enough to understand the example and the motivation for supervisors.

---

## Trapping Exits

Sometimes a linked process needs to react to a failure instead of immediately terminating.

The current process can enable exit trapping:

```elixir
Process.flag(:trap_exit, true)
```

This changes an incoming link exit signal into a regular mailbox message:

```elixir
{:EXIT, from_pid, reason}
```

Now the process can handle the exit using `receive`.

```elixir
receive do
  {:EXIT, ^worker, reason} ->
    IO.puts("Parent received an EXIT message.")
    IO.puts("Reason: #{inspect(reason)}")
end
```

The worker still crashes.

Exit trapping changes the receiving parent's response to that crash.

```text
Without trap_exit              With trap_exit

exit signal                    exit signal
     |                              |
     v                              v
parent terminates              {:EXIT, pid, reason}
                                     |
                                     v
                               parent's mailbox
```

---

## A Trapped-Exit Example

Here is the second mode:

```elixir
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
```

The call to `Process.flag/2` affects the process that calls it:

```elixir
Process.flag(:trap_exit, true)
```

In this script, that is the parent process executing `run/1`.

It must be enabled before the linked worker crashes. Otherwise, the abnormal signal can terminate the parent before it enters `receive`.

---

## Understanding the `:EXIT` Message

The trapped exit message has this shape:

```elixir
{:EXIT, from_pid, reason}
```

Let's break it down.

### `:EXIT`

Identifies the value as an exit signal converted into a mailbox message.

### `from_pid`

Identifies the linked process that terminated.

### `reason`

Describes why that process terminated.

In our example, the reason contains the exception and stack trace because the worker terminates by raising:

```elixir
raise "Worker crashed"
```

---

## Why Use `^worker`?

The receive pattern contains:

```elixir
{:EXIT, ^worker, reason}
```

The variable was already bound when we created the process:

```elixir
worker = spawn_link(fn -> ... end)
```

The pin operator means:

> Match an exit message from this particular worker.

Without the pin:

```elixir
{:EXIT, worker, reason}
```

the pattern could bind `worker` to the PID found in any matching `:EXIT` message.

Pinning matters when a process has several links and its mailbox may contain exit messages from more than one process.

---

## Running the Trap Example

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

11:40:13.501 [error] Process #PID<0.102.0> raised an exception
** (RuntimeError) Worker crashed

Parent received an EXIT message.
Worker: #PID<0.102.0>
Reason: {%RuntimeError{message: "Worker crashed"}, [...]}
Parent is still alive.
```

The error report is expected. Trapping an exit does not prevent or hide the worker's exception.

It prevents that linked abnormal exit from automatically terminating the parent and makes the reason available as a message.

---

## The Complete Example

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

The script has two modes because an untrapped linked crash terminates the process running the example. Keeping the demonstrations separate makes each behavior clear.

---

## Links vs Monitors

Links and monitors both provide information about process termination, but they express different relationships.

```text
Monitor
-------
one-way observation
monitored process terminates
observer receives {:DOWN, ...}
observer stays alive

Link
----
bidirectional lifecycle relationship
linked process terminates abnormally
exit signal propagates
receiver normally terminates too
```

With exit trapping, a link can also produce a mailbox message, but it is still not the same as a monitor.

### Monitor notification

```elixir
{:DOWN, ref, :process, pid, reason}
```

### Trapped link exit

```elixir
{:EXIT, pid, reason}
```

A monitor includes a unique reference and is one-way.

A link is bidirectional and connects the failure behavior of both processes.

A useful question is:

```text
Do I need to observe this process?
    -> monitor

Should our failure lifecycles be connected?
    -> link
```

---

## Links Are Not Supervision

At this point, the trapped parent knows that its worker failed:

```elixir
{:EXIT, worker, reason}
```

But it still has to decide what happens next.

It might:

* log the failure
* stop itself
* start a replacement worker
* retry with a limit
* apply a restart policy

Our example does only the first step: it receives and prints the failure.

A production-quality restart system must answer more questions:

```text
Which processes should restart?

How many restart attempts are allowed?

What state should a replacement receive?

What if restarts repeatedly fail?

Should sibling processes restart too?
```

Those decisions are why OTP provides supervisors.

---

## What We Built

Without using `GenServer` or `Supervisor`, we now have:

```text
Process
   +
Mailbox
   +
Request / Reply
   +
Request References
   +
Process Monitoring
   +
Process Linking
   +
Trapped Exits
```

The important idea from this part is:

> **Links turn process failure into a relationship between process lifecycles.**

Without exit trapping, an abnormal exit propagates and normally terminates the linked process.

With exit trapping, that signal becomes an `:EXIT` message that the process can handle.

---

## What's Next?

We can now detect a worker failure and receive its reason.

The next step is to respond by starting a replacement.

In Part 5, we'll build a small **supervisor from scratch** and explore:

```text
worker crashes
      |
      v
supervisor receives exit
      |
      v
supervisor starts replacement
```

That will show why restart strategies, restart intensity limits, and OTP supervisors exist.

---

Source code link : https://github.com/pckrishnadas88/elixir-distributed-systems-lab/tree/main/04-process-linking

---

## Series

1. [Building a Stateful Process in Elixir Without GenServer](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)
2. [Correlated Request/Reply](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)
3. [Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)
4. **Process Linking**
5. [Supervisor From Scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh)

---

← Previous: [Part 3 — Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)

Next: [Part 5 — Supervisor From Scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh) →

---

## Content License

This article is licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/). You may share and adapt it with attribution for non-commercial purposes. The accompanying source code remains licensed under the MIT License.
