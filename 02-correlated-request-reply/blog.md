# Building Distributed Systems with Elixir — 02: Correlated Request–Reply

In the previous example, we built a minimal stateful process using `spawn`, `send`, and `receive`.

A client could send a request:

```elixir
{:get, self()}
```

and the server could reply:

```elixir
{:counter_value, value}
```

That gives us basic request–reply communication.

But there is an important question we haven't answered yet:

**If a process makes multiple requests, how do we know which reply belongs to which request?**

This is the problem we'll explore in this example.

We will continue using raw Elixir process primitives without `GenServer`, `Task`, or other OTP abstractions.

---

## The Problem

Every Elixir process has a PID.

A client can include its PID in a request:

```elixir
send(server, {:request, self(), value})
```

The server can then use that PID to send the response:

```elixir
send(from, {:reply, result})
```

For a simple request this works.

But a PID identifies a **process**, not an individual request.

The same process can make many requests during its lifetime.

```text
Client #PID<0.95.0>

    request 10
    request 20
    request 30
```

All three requests come from the same process.

So we have two separate questions:

```text
Where should the reply go?

Which request does the reply belong to?
```

A PID answers the first question.

We need something else to answer the second.

---

## Creating a Request ID

Elixir provides `make_ref/0`.

```elixir
ref = make_ref()
```

It creates a unique reference.

For example:

```text
#Reference<0.2323213673.1614020613.213691>
```

Calling it again produces another reference.

```elixir
ref1 = make_ref()
ref2 = make_ref()
```

These references can be used as request identifiers.

So instead of sending:

```elixir
{:request, self(), value}
```

we send:

```elixir
{:request, self(), ref, value}
```

Our protocol now contains both pieces of information:

```text
self() -> where should the response go?

ref    -> which request is this?
```

---

## The Server

The server is intentionally small.

```elixir
defmodule Server do
  def start do
    spawn(fn -> loop() end)
  end

  defp loop do
    receive do
      {:request, from, ref, value} ->
        IO.puts("Server received request: #{value}")

        result = value * 2

        send(from, {:reply, ref, result})

        loop()

      :stop ->
        IO.puts("Server stopped")
        :ok
    end
  end
end
```

The important part is the protocol.

The server receives:

```elixir
{:request, from, ref, value}
```

It processes the request:

```elixir
result = value * 2
```

and returns the **same reference**:

```elixir
send(from, {:reply, ref, result})
```

The server doesn't generate a new reference.

It simply returns the request identifier supplied by the client.

Conceptually:

```text
Client                              Server

       {:request, pid, ref1, 10}
---------------------------------->

                                  10 * 2

          {:reply, ref1, 20}
<----------------------------------
```

The request and response are now correlated by `ref1`.

---

## The Client

The client creates the reference.

```elixir
defmodule Client do
  def request(server, value) do
    ref = make_ref()

    IO.puts("Client #{inspect(self())} sending #{value}")
    IO.puts("Request ref: #{inspect(ref)}")

    send(server, {:request, self(), ref, value})

    receive do
      {:reply, ^ref, result} ->
        IO.puts(
          "Client #{inspect(self())} received matching reply :#{result}"
        )

        {:ok, result}
    end
  end
end
```

There is one particularly important line here:

```elixir
{:reply, ^ref, result}
```

Why `^ref` instead of just `ref`?

---

## Matching the Correct Reply

Before entering `receive`, the client already created a reference:

```elixir
ref = make_ref()
```

Suppose conceptually its value is:

```text
ref2
```

Now imagine the mailbox contains:

```text
{:reply, ref1, 20}
{:reply, ref2, 40}
{:reply, ref3, 60}
```

The client is interested only in the response for `ref2`.

That's what the pin operator does:

```elixir
receive do
  {:reply, ^ref, result} ->
    {:ok, result}
end
```

`^ref` means:

> Match against the value already stored in `ref`.

So:

```text
{:reply, ref1, 20}  -> doesn't match
{:reply, ref2, 40}  -> matches
{:reply, ref3, 60}  -> doesn't match
```

The process can select the matching message while unmatched messages remain in its mailbox.

This is **selective receive**.

---

## PID vs Request Reference

This distinction was the most useful part of this example for me.

Consider three requests made by the same process:

```text
Client #PID<0.95.0>

request 10 -> ref1
request 20 -> ref2
request 30 -> ref3
```

The PID doesn't change.

The request reference does.

So they represent different things:

```text
PID = process identity

REF = request identity
```

Or from the server's perspective:

```text
PID -> where do I send the reply?

REF -> which request am I replying to?
```

A process and an operation are not the same thing.

---

## Running the Example

The project is kept deliberately small:

```text
02-correlated-request-reply/
├── client.ex
├── server.ex
├── example.exs
└── README.md
```

The example starts the server and makes several requests.

```elixir
server = Server.start()

IO.inspect(Client.request(server, 10))
IO.inspect(Client.request(server, 20))
IO.inspect(Client.request(server, 30))

send(server, :stop)
```

Run:

```bash
elixir example.exs
```

Example output:

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

Server stopped
```

Something interesting is visible in this output.

The PID remains:

```text
#PID<0.95.0>
```

while every request gets a different reference.

That's exactly what we wanted.

---

## What About Multiple Client Processes?

BEAM processes are lightweight, so we can also have multiple clients communicating with the same server.

```elixir
for value <- [40, 50, 60] do
  spawn(fn ->
    IO.inspect(Client.request(server, value))
  end)
end
```

Now the architecture looks more like:

```text
Client A #PID<0.101.0> ---- refA ----\
                                      \
Client B #PID<0.102.0> ---- refB -----> Server
                                      /
Client C #PID<0.103.0> ---- refC ----/
```

Each client has its own PID.

Each request also has its own reference.

The server doesn't need to know anything about how those clients were created.

It understands only the protocol:

```elixir
{:request, from, ref, value}
```

and:

```elixir
{:reply, ref, result}
```

---

## If Every Request Has Its Own Process, Do We Still Need a Request ID?

This was a question I had while building the example.

Suppose we create one process for every request.

Each process has a unique PID.

Couldn't the PID itself identify the request?

For a very simple one-request-per-process design, it sometimes could.

But that mixes two different concepts.

A process represents a unit of execution and isolation.

A reference identifies an operation.

```text
Process   = WHO

Reference = WHICH REQUEST
```

A process may eventually have several operations in flight:

```text
One Client Process

    ├── request ref1
    ├── request ref2
    ├── request ref3
    └── request ref4
```

If our protocol already contains request identifiers, it doesn't depend on the assumption that every request needs a new process.

Processes should be created when we want concurrency or isolation, not merely as a substitute for request IDs.

---

## Why This Pattern Matters

The idea itself isn't specific to Elixir.

The general pattern is:

```text
Request
   |
   +---- request_id
   |
   v
Server
   |
   +---- same request_id
   |
   v
Response
```

Whenever requests and responses are asynchronous, some form of correlation can be useful.

The identifier might be:

* a BEAM reference
* a UUID
* an integer
* a protocol-specific request ID

The implementation changes, but the underlying problem is the same:

**Which response belongs to which request?**

---

## What We Have So Far

After the first two examples, our tiny protocol has evolved from:

```text
Client
   |
   | message
   v
Process
```

into:

```text
Client
   |
   | request + PID + request ID
   v
Server
   |
   | response + request ID
   v
Client
```

We now understand:

* process mailboxes
* `send`
* `receive`
* request–reply communication
* `make_ref/0`
* request IDs
* matching replies
* selective receive
* concurrent clients

But there is still a much bigger problem.

What happens if the process we're communicating with dies?

```text
Client
   |
   | request
   v
Server
   |
   X
```

How does another process know that it terminated?

That's where process monitoring comes in.

---

## Next: Process Monitoring

In the next example I'll explore:

```elixir
Process.monitor(pid)
```

and the `:DOWN` messages delivered when a monitored process terminates.

That will be the first step from basic message passing toward understanding how the BEAM detects and reacts to process failures.

Source code link : https://github.com/pckrishnadas88/elixir-distributed-systems-lab/tree/main/02-correlated-request-reply

---

## Series

1. [Process Mailbox](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)
2. **Correlated Request/Reply**
3. [Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)
4. [Process Linking](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-4-process-linking-okb)

---

← Previous: [Part 1 — Process Mailbox](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)

Next: [Part 3 — Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p) →

---

## Content License

This article is licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/). You may share and adapt it with attribution for non-commercial purposes. The accompanying source code remains licensed under the MIT License.
