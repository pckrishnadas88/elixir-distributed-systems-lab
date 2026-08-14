# Building Distributed Systems in Elixir: Part 3 — Process Monitoring

In the previous parts of this series, we built a stateful process using a mailbox and then added correlated request–reply using references.

Now we have another problem.

**What happens when the process we are communicating with dies?**

A worker might finish normally, crash because of an exception, or terminate for some other reason.

Instead of repeatedly checking whether a process is alive, the BEAM provides **process monitoring**.

In this part, we'll build process monitoring directly using:

```elixir
Process.monitor/1
```

No `GenServer`.

No `Supervisor`.

The goal is to understand the primitive underneath OTP's failure-handling mechanisms.

---

## The Problem

Imagine we start a worker:

```elixir
worker = Worker.start("worker-1")
```

That worker is an independent BEAM process.

At some point it could stop:

```elixir
send(worker, :stop)
```

or crash:

```elixir
send(worker, :crash)
```

Another process may need to know that the worker is gone.

One option would be repeatedly checking:

```elixir
Process.alive?(worker)
```

But polling isn't what we want.

Instead, we can ask the BEAM to notify us when the process terminates.

---

## Monitoring a Process

Monitoring starts with one line:

```elixir
ref = Process.monitor(worker)
```

The important detail is:

> The process calling `Process.monitor/1` becomes the monitoring process.

`Process.monitor/1` returns a unique reference.

If the worker later terminates, the BEAM sends the monitoring process a message:

```elixir
{:DOWN, ref, :process, worker, reason}
```

This is just a message.

That means we can handle it using the same `receive` primitive we've already been using.

---

## A Simple Worker

Let's create a worker that understands three messages:

```elixir
defmodule Worker do
  def start(name) do
    spawn(fn -> loop(name) end)
  end

  defp loop(name) do
    receive do
      {:work, from, value} ->
        IO.puts("#{name} received #{value}")
        send(from, {:done, name, value})
        loop(name)

      :stop ->
        IO.puts("#{name} stopping normally")
        :ok

      :crash ->
        raise "#{name} crashed"
    end
  end
end
```

The process starts here:

```elixir
spawn(fn -> loop(name) end)
```

The newly spawned process executes `loop/1` and waits at:

```elixir
receive do
```

If it receives work:

```elixir
{:work, from, value}
```

it handles the message and calls:

```elixir
loop(name)
```

again.

That keeps the process alive.

But look at `:stop`:

```elixir
:stop ->
  IO.puts("#{name} stopping normally")
  :ok
```

There is no call to `loop/1`.

The function finishes, so the process terminates normally.

---

## Detecting a Normal Exit

Now let's monitor the worker:

```elixir
def normal_exit do
  worker = Worker.start("worker-1")

  ref = Process.monitor(worker)

  send(worker, :stop)

  receive do
    {:DOWN, ^ref, :process, ^worker, reason} ->
      IO.puts("worker-1 terminated")
      IO.puts("reason: #{inspect(reason)}")
  end
end
```

The execution flow looks like this:

```text
Monitoring Process                 Worker Process

       |                                |
       | -------- spawn ------------->  |
       |                                |
       |                             receive
       |                             waiting
       |
       | Process.monitor(worker)
       |
       | -------- :stop ------------->  |
       |                                |
       |                         receives :stop
       |                                |
       |                          returns :ok
       |                                |
       |                           terminates
       |                                X
       |
       |      BEAM detects termination
       |
       | <---- {:DOWN, ..., :normal}
       |
    receive
       |
       v
 reason = :normal
```

There are two separate messages here.

First:

```elixir
:stop
```

is sent to the **worker's mailbox**.

Later:

```elixir
{:DOWN, ref, :process, worker, :normal}
```

is sent by the BEAM to the **monitoring process's mailbox**.

This distinction is important.

---

## Understanding the `:DOWN` Message

The notification has this shape:

```elixir
{:DOWN, ref, :process, pid, reason}
```

Let's break it down.

### `:DOWN`

Identifies the message as a monitor notification.

### `ref`

The unique monitor reference returned by:

```elixir
Process.monitor(worker)
```

### `:process`

Indicates that the monitored entity was a process.

### `pid`

The PID of the process that terminated.

### `reason`

Describes why the process terminated.

For a normal exit:

```elixir
:normal
```

For a crash, it contains information about the failure.

---

## Why Use `^ref`?

Our receive pattern contains:

```elixir
{:DOWN, ^ref, :process, ^worker, reason}
```

The pin operator `^` tells Elixir to match against an existing value.

We already have:

```elixir
ref = Process.monitor(worker)
```

So:

```elixir
^ref
```

means:

> Match the `:DOWN` message belonging to this particular monitor.

Likewise:

```elixir
^worker
```

means:

> Match the PID of this particular worker.

This lets us precisely identify the termination notification we're waiting for.

---

## Detecting a Crash

Monitoring also works when a worker crashes.

```elixir
def crash_exit do
  worker = Worker.start("worker-2")

  ref = Process.monitor(worker)

  send(worker, :crash)

  receive do
    {:DOWN, ^ref, :process, ^worker, reason} ->
      IO.puts("worker-2 terminated")
      IO.puts("reason: #{inspect(reason)}")
  end
end
```

The worker receives:

```elixir
:crash
```

and executes:

```elixir
raise "#{name} crashed"
```

The worker dies.

But the monitoring process does **not** die with it.

Instead, the BEAM sends it a `:DOWN` message containing the failure reason.

This gives us an important property:

> Monitoring lets one process observe another process's failure without automatically propagating that failure.

---

## Monitoring Multiple Workers

Real systems usually have more than one worker.

Let's start three:

```elixir
worker1 = Worker.start("worker-1")
worker2 = Worker.start("worker-2")
worker3 = Worker.start("worker-3")
```

Then monitor each one:

```elixir
ref1 = Process.monitor(worker1)
ref2 = Process.monitor(worker2)
ref3 = Process.monitor(worker3)
```

Each monitor gets a different reference.

We can keep track of them using a map:

```elixir
%{
  ref1 => "worker-1",
  ref2 => "worker-2",
  ref3 => "worker-3"
}
```

Think of this map as:

```text
workers whose termination we are still waiting for

ref1 -> worker-1
ref2 -> worker-2
ref3 -> worker-3
```

Now send different termination messages:

```elixir
send(worker1, :stop)
send(worker2, :crash)
send(worker3, :stop)
```

One worker crashes while the other two stop normally.

---

## Waiting for All Workers

We can write a small recursive function to collect the monitor notifications:

```elixir
defp wait_for_workers(monitors) when map_size(monitors) == 0 do
  IO.puts("all workers terminated")
end

defp wait_for_workers(monitors) do
  receive do
    {:DOWN, ref, :process, pid, reason} ->
      case Map.pop(monitors, ref) do
        {nil, monitors} ->
          wait_for_workers(monitors)

        {name, remaining} ->
          IO.puts("#{name} terminated")
          IO.puts("pid: #{inspect(pid)}")
          IO.puts("reason: #{inspect(reason)}")
          IO.puts("")

          wait_for_workers(remaining)
      end
  end
end
```

The function has a simple purpose:

> Wait for each worker to terminate and log what happened.

Conceptually:

```text
3 workers
    |
    | worker terminates
    v
receive :DOWN
    |
    v
log termination
    |
    v
remove monitor
    |
    v
2 workers
    |
    | worker terminates
    v
receive :DOWN
    |
    v
1 worker
    |
    | worker terminates
    v
receive :DOWN
    |
    v
0 workers
    |
    v
finished
```

The order is not guaranteed.

Even though we send:

```elixir
send(worker1, :stop)
send(worker2, :crash)
send(worker3, :stop)
```

the workers execute concurrently.

Any of them may terminate first.

---

## The Complete Example

```elixir
defmodule Worker do
  def start(name) do
    spawn(fn -> loop(name) end)
  end

  defp loop(name) do
    receive do
      {:work, from, value} ->
        IO.puts("#{name} received #{value}")
        send(from, {:done, name, value})
        loop(name)

      :stop ->
        IO.puts("#{name} stopping normally")
        :ok

      :crash ->
        raise "#{name} crashed"
    end
  end
end

defmodule MonitorDemo do
  def normal_exit do
    worker = Worker.start("worker-1")
    ref = Process.monitor(worker)

    send(worker, :stop)

    receive do
      {:DOWN, ^ref, :process, ^worker, reason} ->
        IO.puts("worker-1 terminated")
        IO.puts("reason: #{inspect(reason)}")
    end
  end

  def crash_exit do
    worker = Worker.start("worker-2")
    ref = Process.monitor(worker)

    send(worker, :crash)

    receive do
      {:DOWN, ^ref, :process, ^worker, reason} ->
        IO.puts("worker-2 terminated")
        IO.puts("reason: #{inspect(reason)}")
    end
  end

  def multiple_workers do
    worker1 = Worker.start("worker-1")
    worker2 = Worker.start("worker-2")
    worker3 = Worker.start("worker-3")

    ref1 = Process.monitor(worker1)
    ref2 = Process.monitor(worker2)
    ref3 = Process.monitor(worker3)

    send(worker1, :stop)
    send(worker2, :crash)
    send(worker3, :stop)

    wait_for_workers(%{
      ref1 => "worker-1",
      ref2 => "worker-2",
      ref3 => "worker-3"
    })
  end

  defp wait_for_workers(monitors) when map_size(monitors) == 0 do
    IO.puts("all workers terminated")
  end

  defp wait_for_workers(monitors) do
    receive do
      {:DOWN, ref, :process, pid, reason} ->
        case Map.pop(monitors, ref) do
          {nil, monitors} ->
            wait_for_workers(monitors)

          {name, remaining} ->
            IO.puts("#{name} terminated")
            IO.puts("pid: #{inspect(pid)}")
            IO.puts("reason: #{inspect(reason)}")
            IO.puts("")

            wait_for_workers(remaining)
        end
    end
  end
end

MonitorDemo.normal_exit()
MonitorDemo.crash_exit()
MonitorDemo.multiple_workers()
```

Run it with:

```bash
elixir monitor.exs
```

---

## Monitor vs Polling

We could ask:

```elixir
Process.alive?(worker)
```

But that only tells us whether the process was alive at that particular moment.

The process could terminate immediately afterward.

With monitoring:

```elixir
Process.monitor(worker)
```

we instead ask the BEAM to notify us about termination.

```text
monitor worker
      |
      v
worker runs independently
      |
      v
worker terminates
      |
      v
BEAM detects termination
      |
      v
{:DOWN, ...}
      |
      v
monitoring process
```

This fits naturally with the BEAM's message-passing model.

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
```

We're gradually building the primitives needed for fault-tolerant process systems.

The important idea from this part is:

> **Failure detection itself can be represented as message passing.**

A worker terminates, and the monitoring process receives a message describing what happened.

---

## What's Next?

Monitoring lets us **observe** another process's failure.

But sometimes processes need a stronger relationship.

What if the failure of one process should affect another process?

That's where **process links** come in.

In Part 4, we'll explore:

```elixir
spawn_link/1
```

and see how failures propagate between linked processes.

---

## Series

1. Process Mailbox
2. Correlated Request/Reply
3. **Process Monitoring**
4. Process Linking
5. Supervisor From Scratch

The examples intentionally use low-level process primitives first so we can understand what OTP abstractions are solving before using them.
