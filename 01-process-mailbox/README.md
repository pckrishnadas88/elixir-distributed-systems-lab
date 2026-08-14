# 01 - Process Mailbox

A minimal stateful server implemented using raw Elixir process primitives.

This example intentionally avoids `GenServer`, `Agent`, `Registry`, and other OTP abstractions to demonstrate how a long-running process communicates using message passing.

---

## Learning Objective

Understand how a process can:

- own private state
- receive commands through its mailbox
- respond to callers
- implement a simple request–reply protocol

This example forms the foundation for understanding `GenServer`.

---

## Problem

Suppose multiple parts of an application need to share a counter.

In many languages, this might be implemented using a shared mutable variable.

On the BEAM, processes do not share memory.

Instead, one process owns the state, and all other processes communicate with it by sending messages.

---

## Architecture

```
                +-----------------------+
                |  Counter Process      |
                |-----------------------|
                | value = 0             |
                | mailbox               |
                +-----------------------+
                     ▲
                     │
        request      │      reply
                     │
+--------------------┴-------------------+
|                                        |
|           Client Process               |
+----------------------------------------+
```

---

## Protocol

### Requests

```elixir
{:get, caller_pid}
{:increment, caller_pid}
:stop
```

### Responses

```elixir
{:counter_value, value}
```

---

## Message Flow

```
Client                         Counter

{:get,self()}
---------------------------->

                    receive

                    value = 5

                    {:counter_value,5}

<----------------------------

receive

returns {:ok,5}
```

---

## Implementation

The counter owns the state.

```elixir
loop(current_value)
```

No other process can directly access `current_value`.

Clients communicate only by sending protocol messages.

---

## Running

```bash
elixir mailbox.exs
```

Example output

```
Counter PID: #PID<0.104.0>

Initial value:
{:ok, 0}

After increment:
{:ok, 1}

After second increment:
{:ok, 2}

Current value:
{:ok, 2}

Counter stopped
```

---

## Key Concepts

### Process

An isolated unit of execution with its own heap and mailbox.

---

### Mailbox

Every process owns a mailbox.

Messages sent using

```elixir
send(pid, message)
```

are appended to the destination process's mailbox.

---

### Request

The client asks the counter to perform an operation.

```elixir
{:get, caller_pid}
```

---

### Reply

The counter sends a new message back to the caller.

```elixir
{:counter_value, value}
```

---

### Pattern Matching

The receive loop dispatches messages by matching on their shape.

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

The atom acts as the command identifier.

---

## Why not call a function directly?

Because the counter process owns the state.

Another process cannot directly read or modify that state.

Instead it asks using a protocol message.

```
Client

"What is your value?"

↓

Counter

"My value is 5."
```

---

## Limitations

This implementation is intentionally simple.

It does **not** provide:

- supervision
- automatic restart
- process registration
- request correlation
- monitoring
- distribution across nodes
- fault tolerance

These problems are solved progressively in later examples.

---

## Next Example

**02 - Correlated Request Reply**

The next example introduces request identifiers using `make_ref/0` so multiple concurrent requests can safely receive the correct replies.

---

## References

- Erlang Processes
- Erlang Message Passing
- Elixir `spawn/1`
- Elixir `send/2`
- Elixir `receive`
 
- Blog post: [Building a stateful process in Elixir without GenServer](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)

- Adjacent posts:
    - Previous: None
    - Next: [Correlated request–reply in Elixir](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)