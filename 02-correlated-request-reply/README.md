# 02 - Correlated Request Reply

A minimal correlated request–reply protocol implemented using raw Elixir process primitives.

This example introduces unique request references using `make_ref/0` and shows how clients can match responses to the requests that produced them.

This example intentionally avoids `GenServer`, `Task`, and other OTP abstractions so the underlying message-passing mechanism remains visible.

---

## Learning Objective

Understand how processes can:

* create a unique identifier for each request
* include the identifier in a message
* return the identifier with the response
* match a response to a specific request
* distinguish process identity from request identity
* make requests from multiple concurrent client processes

This introduces request correlation, a common pattern in asynchronous and distributed systems.

---

## Problem

In the previous example, the client sent messages to a process and received responses.

A simple request–reply protocol can use the caller PID to tell the server where to send the response.

```elixir
{:request, caller_pid, value}
```

But a PID identifies a **process**, not an individual request.

The same process can make many requests.

```text
Client #PID<0.95.0>

    request 10
    request 20
    request 30
```

All three requests come from the same process.

We therefore need a separate identifier if we want to identify each request independently.

```text
Client #PID<0.95.0>

    request 10  -> ref1
    request 20  -> ref2
    request 30  -> ref3
```

This is called **request correlation**.

---

## Architecture

```text
                +-----------------------+
                |    Server Process     |
                |-----------------------|
                | mailbox               |
                +-----------------------+
                     ▲             │
                     │             │
        request      │             │ reply
                     │             ▼
                +-----------------------+
                |    Client Process     |
                |-----------------------|
                | ref = make_ref()      |
                | mailbox               |
                +-----------------------+
```

The client creates a unique reference before sending a request.

The server includes the same reference in the response.

---

## Protocol

### Request

```elixir
{:request, caller_pid, ref, value}
```

Example:

```elixir
{:request, self(), ref, 10}
```

### Response

```elixir
{:reply, ref, result}
```

Example:

```elixir
{:reply, ref, 20}
```

The same reference travels through the request and response.

---

## Message Flow

```text
Client                              Server

ref = make_ref()

{:request, self(), ref, 10}
---------------------------------->

                              receive

                              value * 2

                              result = 20

                              {:reply, ref, 20}

<----------------------------------

receive

{:reply, ^ref, result}

returns {:ok, 20}
```

The client waits for a response containing the reference created for that request.

---

## Implementation

The client first creates a unique reference.

```elixir
ref = make_ref()
```

It sends the reference together with its PID and the request value.

```elixir
send(server, {:request, self(), ref, value})
```

The server receives:

```elixir
{:request, from, ref, value}
```

After processing the request, it sends the result back to the client.

```elixir
send(from, {:reply, ref, result})
```

Notice that the server returns the **same reference** it received.

The client then waits for a matching response.

```elixir
receive do
  {:reply, ^ref, result} ->
    {:ok, result}
end
```

The pin operator ensures that the reply contains the reference belonging to this request.

---

## Project Structure

```text
02-correlated-request-reply/
├── client.ex
├── server.ex
├── example.exs
└── README.md
```

`server.ex` contains the server process.

`client.ex` contains the correlated request–reply logic.

`example.exs` starts the server and demonstrates sequential requests and concurrent clients.

---

## Running

```bash
elixir example.exs
```

Example sequential output:

```text
Client #PID<0.95.0> sending 10
Request ref: #Reference<...>
Server received request: 10
Client #PID<0.95.0> received matching reply :20
{:ok, 20}

Client #PID<0.95.0> sending 20
Request ref: #Reference<...>
Server received request: 20
Client #PID<0.95.0> received matching reply :40
{:ok, 40}

Client #PID<0.95.0> sending 30
Request ref: #Reference<...>
Server received request: 30
Client #PID<0.95.0> received matching reply :60
{:ok, 60}
```

The exact PID and reference values will be different each time the program runs.

---

## Key Concepts

### Reference

`make_ref/0` creates a unique reference.

```elixir
ref = make_ref()
```

Example:

```text
#Reference<0.2323213673.1614020613.213691>
```

Each call to `make_ref/0` creates a new reference.

```elixir
ref1 = make_ref()
ref2 = make_ref()
ref3 = make_ref()
```

Conceptually:

```text
ref1 != ref2
ref2 != ref3
```

Each request can therefore have its own identity.

---

### PID vs Reference

A PID and a reference solve different problems.

The PID identifies a process.

```elixir
self()
```

The reference identifies an individual request.

```elixir
make_ref()
```

For example:

```text
Client #PID<0.95.0>

request 10 -> ref1
request 20 -> ref2
request 30 -> ref3
```

The process remains the same.

The request identity changes.

```text
PID = where should the reply go?

REF = which request does the reply belong to?
```

---

### `self/0`

`self/0` returns the PID of the current process.

```elixir
self()
```

The client includes this PID in the request.

```elixir
{:request, self(), ref, value}
```

The server receives the PID as `from`.

```elixir
{:request, from, ref, value}
```

It can then reply directly to that process.

```elixir
send(from, {:reply, ref, result})
```

Message passing does not automatically create a reply channel.

The client explicitly tells the server where the response should be sent.

---

### Correlation ID

The reference acts as a correlation ID.

Request:

```elixir
{:request, self(), ref, value}
```

Response:

```elixir
{:reply, ref, result}
```

Because the same reference appears in both messages, the response can be associated with the original request.

```text
Request 10 ---- ref1 ----> Reply 20

Request 20 ---- ref2 ----> Reply 40

Request 30 ---- ref3 ----> Reply 60
```

---

### Pin Operator

Before waiting for the response, the client already has a value stored in `ref`.

```elixir
ref = make_ref()
```

The receive pattern uses:

```elixir
{:reply, ^ref, result}
```

The pin operator `^` means:

> Match against the existing value of `ref`.

It does not assign a new value to `ref`.

Suppose the client is waiting for:

```text
ref2
```

A message containing:

```text
{:reply, ref1, 20}
```

does not match.

A message containing:

```text
{:reply, ref2, 40}
```

does match.

---

### Selective Receive

A process can search its mailbox for a message matching a particular pattern.

Suppose a mailbox conceptually contains:

```text
{:reply, ref1, 20}
{:reply, ref2, 40}
{:reply, ref3, 60}
```

If the process is waiting for `ref2`:

```elixir
receive do
  {:reply, ^ref2, result} ->
    result
end
```

the message containing `ref2` can be selected.

Messages that do not match remain in the mailbox.

This behavior is called **selective receive**.

---

## Sequential Requests

The first part of the example sends requests sequentially.

```elixir
Client.request(server, 10)
Client.request(server, 20)
Client.request(server, 30)
```

The first request completes before the next request starts.

```text
request 10
    ↓
reply 20
    ↓
request 20
    ↓
reply 40
    ↓
request 30
    ↓
reply 60
```

All of these calls run from the same client process.

The PID therefore remains the same while every request receives a different reference.

```text
PID                     Request

#PID<0.95.0>             ref1
#PID<0.95.0>             ref2
#PID<0.95.0>             ref3
```

This demonstrates the difference between **process identity** and **request identity**.

---

## Concurrent Clients

The example can also start multiple client processes.

```elixir
for value <- [40, 50, 60] do
  spawn(fn ->
    IO.inspect(Client.request(server, value))
  end)
end
```

Now several processes can communicate with the same server.

```text
Client A #PID<0.101.0> ---- refA ----\
                                      \
Client B #PID<0.102.0> ---- refB -----> Server
                                      /
Client C #PID<0.103.0> ---- refC ----/
```

Each client has its own PID.

Each request also has its own reference.

Conceptually:

```text
Client PID          Request ID

#PID<0.101.0>       refA
#PID<0.102.0>       refB
#PID<0.103.0>       refC
```

The server does not need separate logic for each client.

It handles every request using the same protocol:

```elixir
{:request, from, ref, value}
```

and responds with:

```elixir
{:reply, ref, result}
```

---

## Why Use a Reference If Each Client Has a PID?

If every process makes exactly one request and then terminates, the PID could sometimes be enough to associate the response with that process.

But this creates an unnecessary restriction on the protocol.

A process may make many requests during its lifetime.

```text
One Client Process

        ├── request ref1
        ├── request ref2
        ├── request ref3
        └── request ref4
```

The process represents a unit of execution.

The reference represents a particular operation.

```text
Process = WHO

Reference = WHICH REQUEST
```

They solve different problems.

Using references means the protocol does not depend on creating a new process for every request.

---

## Concurrent Processes vs Request Correlation

BEAM processes are lightweight, and creating processes for concurrent work is normal.

However, processes should be created because the work requires independent concurrency, isolation, or lifecycle.

They should not be created only to avoid giving requests an identity.

A useful mental model is:

```text
Process
    =
unit of concurrency and isolation

Reference
    =
identity of an operation
```

This distinction becomes increasingly important as protocols become more complex.

---

## Limitations

This implementation is intentionally simple.

It does **not** provide:

* process monitoring
* supervision
* automatic restart
* process linking
* process registration
* distribution across nodes
* retries
* persistence
* fault tolerance

Another important question now appears:

```text
Client
   |
   | request
   v
Server
   |
   X crashes
```

How does the client know that the server process has terminated?

Simply waiting for a reply does not tell us why another process disappeared.

That introduces process monitoring.

---

## Next Example

**03 - Process Monitoring**

The next example introduces `Process.monitor/1`.

It will explore:

* monitoring another process
* detecting process termination
* receiving `:DOWN` messages
* distinguishing normal and abnormal termination

This begins the transition from basic message passing toward fault detection on the BEAM.

---

## References

* Erlang Processes
* Erlang Message Passing
* Elixir `spawn/1`
* Elixir `send/2`
* Elixir `receive`
* Elixir `self/0`
* Elixir `make_ref/0`
* Elixir pin operator (`^`)
