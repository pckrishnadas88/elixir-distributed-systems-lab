# Building Distributed Systems in Elixir: Part 6 — Named Processes

In the previous part of this series, we built a tiny supervisor from scratch.

When a worker crashed, the supervisor started a replacement. That replacement had a new PID:

```text
old worker -> #PID<0.102.0>
new worker -> #PID<0.105.0>
```

This reveals an important limitation of sharing PIDs as a public interface.

A PID identifies one running incarnation of a process. It is excellent for sending a reply, setting up a monitor, or creating a link. It is not a stable address for a service that may stop and later be replaced.

In this part, we'll use named processes to give a worker a discoverable address:

```elixir
:worker
```

We'll build three small examples using:

```elixir
Process.register/2
Process.whereis/1
:global.register_name/2
:global.whereis_name/1
send/2
```

No `GenServer`.

No OTP `Registry`.

The goal is to understand the lookup problem that registries solve before reaching for those abstractions.

---

## The PID-Sharing Problem

Suppose one process starts a worker and gives its PID to a client:

```elixir
worker = spawn(fn -> worker_loop() end)
send(client, {:worker_started, worker})
```

The client can now send work directly:

```elixir
send(worker, {:work, self(), "hello"})
```

This works while that particular worker process is alive.

But process IDs are temporary. If the worker exits, the PID is no longer a route to the service:

```text
Client                         Worker

holds #PID<0.102.0>            #PID<0.102.0>
     |                               |
     |                               X exits
     |
     | send(#PID<0.102.0>, work)
     |------------------------------> no worker receives it
```

Sending to a dead local PID does not raise an error and does not restart a process. The message is simply not delivered to a living worker.

One answer is to tell every client about every new PID after a restart. That spreads lifecycle knowledge throughout the system.

Another answer is to make clients depend on a name and resolve that name when sending.

---

## Registering a Local Name

Our first worker waits for a stop message:

```elixir
defmodule Worker do
  def start do
    spawn(fn ->
      receive do
        :stop -> :ok
      end
    end)
  end
end
```

Starting it gives us a PID:

```elixir
pid = Worker.start()
```

We can associate that PID with a local name:

```elixir
Process.register(pid, :worker)
```

The registration belongs to the current BEAM node.

```text
local process registry

:worker  ->  #PID<0.102.0>
```

The atom `:worker` is now a registered local name. We can retrieve its current PID with:

```elixir
Process.whereis(:worker)
```

In the example, the returned PID is the same PID that we registered:

```elixir
registered_pid = Process.whereis(:worker)
```

`Process.whereis/1` returns `nil` if the name is not registered on this node.

---

## Sending to the Name

There is often no need to perform a lookup yourself. `send/2` accepts a local registered name:

```elixir
send(:worker, :stop)
```

The runtime resolves the name and delivers the message to its current owner.

```text
Caller                         Local Registry                  Worker
  |                                  |                           |
  | send(:worker, :stop)             |                           |
  |--------------------------------->| :worker -> worker PID     |
  |                                  |-------------------------->|
  |                                  |                           | receives :stop
```

This is the first benefit of a name: code that sends the message need not carry a PID around.

Run the local example:

```bash
elixir 01-process-registration.exs
```

Typical output is:

```text
Worker PID: #PID<...>
Registered as :worker
Lookup using name: #PID<...>
Stopping using name: #PID<...>
```

The PID values vary from run to run. The name in the code does not.

---

## Name Scope Matters

`Process.register/2` is local to one node.

If two disconnected nodes both run:

```elixir
Process.register(pid, :worker)
```

there is no conflict:

```text
Node A: :worker -> #PID<...>
Node B: :worker -> #PID<...>
```

Each node has its own local process registry. Therefore:

```elixir
Process.whereis(:worker)
```

answers only the question:

> Which local process on this node is registered as `:worker`?

It does not discover a process on another BEAM node.

That is exactly right for many services. A local cache, a local connection manager, or a supervisor-owned worker usually needs only node-local lookup.

For cluster-wide lookup, the system needs a broader naming mechanism.

---

## A Global Name

Erlang supplies the `:global` module for a namespace coordinated across connected nodes.

The second example registers the worker with:

```elixir
:global.register_name(:worker, pid)
```

and looks it up with:

```elixir
:global.whereis_name(:worker)
```

The example then sends a message to the PID returned by that lookup:

```elixir
send(:global.whereis_name(:worker), :stop)
```

Run it with:

```bash
elixir 02-global-lookup.exs
```

The script itself runs on a single node, so its output looks much like the local case. The distinction becomes meaningful after nodes are named and connected.

Conceptually, a connected cluster can coordinate one name:

```text
                       connected Erlang nodes

Node A                                                Node B
 Client                                               Worker
   |                                                    |
   | :global.whereis_name(:worker)                      |
   |--------------------> global name coordination -----|
   |<-------------------- worker PID -------------------|
   |                                                    |
   | send(worker_pid, message)                          |
   |--------------------------------------------------->|
```

` :global` (written as `:global` in code) provides a useful primitive, but global coordination is not free. It has failure behavior and operational trade-offs that matter in a real cluster.

---

## Hiding the PID From Clients

The third example changes the startup API.

Instead of returning the spawned PID, the worker registers itself and returns its public name:

```elixir
def start do
  pid =
    spawn(fn ->
      receive do
        {:work, from, request} ->
          send(from, {:reply, request, "processed: #{request}"})
      end
    end)

  Process.register(pid, :worker)
  :worker
end
```

The caller receives:

```elixir
worker = Worker.start()
# :worker
```

It does not receive or retain the worker PID.

The client sends its request to the name:

```elixir
send(worker_name, {:work, self(), request})
```

The message protocol is still explicit:

```elixir
{:work, caller_pid, request}
```

The worker replies directly to the caller:

```elixir
send(from, {:reply, request, "processed: #{request}"})
```

The name identifies the service endpoint. The caller PID identifies where that particular reply must go.

```text
Client                                          :worker
  |                                                 |
  | {:work, client_pid, "hello"}                    |
  |------------------------------------------------>|
  |                                                 |
  | {:reply, "hello", "processed: hello"}          |
  |<------------------------------------------------|
```

Run it with:

```bash
elixir 03-avoiding-pid-sharing.exs
```

Expected output:

```text
Worker available as: :worker
Response: processed: hello
```

---

## Names Are Not Supervision

It is tempting to think that a name solves the worker-restart problem. It does not.

When a registered process terminates, its registration is removed:

```text
:worker -> #PID<0.102.0>

worker exits

:worker -> no registered process
```

A replacement worker still must be started and registered:

```text
supervisor starts new worker
         |
         v
new worker registers :worker
         |
         v
:worker -> #PID<0.105.0>
```

Names address **discovery**. Supervisors address **lifecycle**. They are complementary.

In an OTP application, a common pattern is to start a supervised process with a name, often using `:via` and `Registry`. The supervisor creates the current process; the registry lets callers find it without depending on its old PID.

---

## One Name Means One Owner

A local registered name has one owner on its node. This is useful for a singleton service, but it also means the simple mechanism does not describe a worker pool.

For example, this is a single endpoint:

```text
:worker -> one PID
```

It cannot represent three workers under the same local atom without another routing process or a richer registry design:

```text
:workers -> router process -> worker_1, worker_2, worker_3
```

That distinction will matter when we build a worker pool later in the series.

---

## Correlating Replies

The client waits for:

```elixir
{:reply, ^request, response} -> response
```

Pinning `request` ensures it accepts a reply with the request value it sent.

For this small demonstration, the string itself is adequate. In a client that might have several requests in flight with the same value, use a unique reference instead:

```elixir
ref = make_ref()
send(:worker, {:work, self(), ref, request})

receive do
  {:reply, ^ref, response} -> response
end
```

This combines named lookup from this lesson with the correlated request-reply technique from Part 2.

---

## Limits of Global Registration

`:global` makes node-wide lookup possible, but it does not remove the hard parts of distributed systems.

A system still needs to decide what happens when:

* nodes cannot communicate because of a network partition
* a process dies and needs replacement
* two nodes attempt to claim the same service role
* clients have already looked up a PID that then exits

There is no universally correct answer. The appropriate naming and discovery strategy depends on whether the service must be local, replicated, partition-tolerant, or strongly coordinated.

For the small examples in this repository, the central idea is simpler:

> Use a process name when callers should depend on a service role rather than on one temporary PID.

That small separation between identity and process incarnation is one of the building blocks of a resilient BEAM system.

---

Source code link: https://github.com/pckrishnadas88/elixir-distributed-systems-lab/tree/main/06-named-processes

---

## Series

1. [Process Mailbox](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)
2. [Correlated Request/Reply](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)
3. [Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)
4. [Process Linking](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-4-process-linking-okb)
5. [Supervisor From Scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh)
6. **Named Processes**

---

← Previous: [Part 5 — Supervisor From Scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh)

Next: Part 7 — Worker Pool *(coming soon)* →

---

## Content License

This article is licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/). You may share and adapt it with attribution for non-commercial purposes. The accompanying source code remains licensed under the MIT License.
