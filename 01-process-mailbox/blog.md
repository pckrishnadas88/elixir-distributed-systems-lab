# Building Distributed Systems in Elixir: Part 1 - Building a Stateful Process in Elixir Without GenServer


> **Series:** Distributed Systems from First Principles — Part 1

`GenServer` is one of the most widely used OTP behaviours in Elixir. It provides a convenient abstraction for building long-running stateful processes, but behind the abstraction lies a surprisingly small set of ideas.

At its core, a stateful server is simply:

* A process
* That owns private state
* Receives messages
* Updates its state
* Sends replies
* Waits for the next message

This article builds a minimal stateful counter using only:

* `spawn/1`
* `send/2`
* `receive`
* recursion

No `GenServer`, `Agent`, or other OTP behaviours are used.

The objective is not to replace OTP, but to understand the message-passing model it builds upon.

---

## The Problem

Imagine multiple clients need access to a shared counter.

In many programming languages, the counter might simply be a mutable object.

```text
Counter
-------
value = 0
```

Multiple threads can read or modify this value directly.

The BEAM follows a completely different design.

Processes never share memory.

Each process owns its own private state, and the only way to interact with another process is by sending it a message.

Instead of representing the counter as a shared variable, the counter itself becomes a process.

---

## High-Level Design

The counter process owns the state.

Clients never access that state directly.

Instead, they communicate using a simple protocol.

```text
                    +---------------------------+
                    |     Counter Process       |
                    |---------------------------|
                    | value = 0                 |
                    |                           |
                    | Mailbox                  |
                    +---------------------------+
                             ▲
                             │
                  Requests   │   Replies
                             │
                    +--------------------------+
                    |      Client Process      |
                    +--------------------------+
```

The communication protocol consists of three request messages.

```elixir
{:get, caller_pid}
{:increment, caller_pid}
:stop
```

The counter responds with:

```elixir
{:counter_value, value}
```

Notice that clients never call functions inside the counter process.

Instead, they exchange messages.

---

## Complete Implementation

```elixir
defmodule Counter do
  def start(initial_value \\ 0) do
    spawn(fn ->
      loop(initial_value)
    end)
  end

  def get(counter_pid) do
    send(counter_pid, {:get, self()})

    receive do
      {:counter_value, value} ->
        {:ok, value}
    after
      1000 ->
        {:error, :timeout}
    end
  end

  def increment(counter_pid) do
    send(counter_pid, {:increment, self()})

    receive do
      {:counter_value, value} ->
        {:ok, value}
    after
      1000 ->
        {:error, :timeout}
    end
  end

  def stop(counter_pid) do
    send(counter_pid, :stop)
  end

  defp loop(value) do
    receive do
      {:get, from_pid} ->
        send(from_pid, {:counter_value, value})
        loop(value)

      {:increment, from_pid} ->
        new_value = value + 1
        send(from_pid, {:counter_value, new_value})
        loop(new_value)

      :stop ->
        IO.puts("Counter stopped")

      unknown ->
        IO.inspect(unknown, label: "Unknown message")
        loop(value)
    end
  end
end

counter = Counter.start()

IO.inspect(Counter.get(counter), label: "Initial value")
IO.inspect(Counter.increment(counter), label: "After increment")
IO.inspect(Counter.increment(counter), label: "After second increment")
IO.inspect(Counter.get(counter), label: "Current value")

Counter.stop(counter)
```

Expected output:

```text
Initial value: {:ok, 0}
After increment: {:ok, 1}
After second increment: {:ok, 2}
Current value: {:ok, 2}
Counter stopped
```

---

## Understanding the Implementation

## 1. Starting the Server

```elixir
def start(initial_value \\ 0) do
  spawn(fn ->
    loop(initial_value)
  end)
end
```

`spawn/1` creates a new lightweight BEAM process.

That process immediately starts executing `loop/1`.

The value passed to `loop/1` becomes the process's private state.

The returned PID is **not** the counter value.

It is simply the address used to communicate with the counter process.

---

## 2. Reading the Counter

The implementation of `get/1` surprises many developers.

```elixir
send(counter_pid, {:get, self()})
```

At first glance, it feels strange that a function called `get()` sends a message.

The reason is simple.

The client cannot access another process's memory.

Instead of reading a variable, it asks the process that owns the state.

The message

```elixir
{:get, self()}
```

contains two pieces of information:

* `:get` → the requested operation
* `self()` → the PID of the caller

The caller PID is included because the counter needs to know where to send the reply.

Immediately after sending the request, the client waits.

```elixir
receive do
  {:counter_value, value} ->
    {:ok, value}
end
```

This `receive` does **not** read the message that was just sent.

Instead, it waits for a **new message** created by the counter process.

---

## 3. Updating the Counter

Incrementing the value follows exactly the same request–reply pattern.

The client sends:

```elixir
{:increment, caller_pid}
```

The counter updates its private state and sends the updated value back.

Notice that the client never performs the increment itself.

Only the counter process can modify its own state.

---

## 4. The Receive Loop

The receive loop is the heart of the server.

```elixir
receive do
  {:get, from_pid} ->
    ...

  {:increment, from_pid} ->
    ...

  :stop ->
    ...
end
```

Each incoming message is pattern matched.

The atom (`:get`, `:increment`, `:stop`) identifies which operation should be performed.

Once the operation completes, the process calls `loop/1` again.

```text
loop(0)

↓

loop(1)

↓

loop(2)
```

The state is never mutated.

Instead, every new state is passed into the next iteration of the loop.

---

## Message Flow

The interaction between the client and the counter looks like this.

```text
Client Process                     Counter Process

{:get, caller_pid}
-------------------------------------->

                           Reads current value

                           {:counter_value, 0}

<--------------------------------------

{:increment, caller_pid}
-------------------------------------->

                           value = value + 1

                           {:counter_value, 1}

<--------------------------------------
```

Two messages are exchanged.

The client sends a request.

The counter sends a reply.

The client never reads the message it originally sent.

---

## Why This Design?

Using message passing instead of shared memory provides an important guarantee.

Every request enters the process mailbox.

```text
Mailbox

1. {:increment, pid1}
2. {:increment, pid2}
3. {:get, pid3}
```

The counter processes one message at a time.

No two clients can modify the state simultaneously.

The process itself serializes access to its private state.

This model avoids many of the synchronization problems commonly associated with shared mutable memory.

---

## What Does GenServer Add?

This implementation already demonstrates the core behaviour of a stateful server.

`GenServer` builds on the same ideas while providing:

* Standard request handling
* Request correlation
* Timeouts
* Monitoring
* Supervision
* Debugging support
* System messages
* Consistent APIs

Understanding the raw implementation makes these abstractions much easier to appreciate.

---

## Summary

This small example demonstrates several fundamental ideas behind the BEAM.

* Processes own private state.
* Processes communicate only through messages.
* State is maintained through recursive receive loops.
* A `get()` operation is a request–reply conversation, not a memory read.
* A stateful server is simply a process implementing a message protocol.

Although intentionally minimal, this example forms the foundation for understanding `GenServer`, supervision trees, distributed messaging, and many other OTP concepts explored in later articles.

---

**Source Code**

The complete runnable example is available on GitHub:

**Repository:** *https://github.com/pckrishnadas88/elixir-distributed-systems-lab/tree/main/01-process-mailbox*
