# 06 - Named Processes

Small examples that register Elixir processes under names instead of requiring callers to share PIDs.

This lesson uses raw process primitives and intentionally avoids `GenServer`, `Registry`, `Supervisor`, and other OTP abstractions. It introduces the difference between a process's short-lived PID and the stable name through which other processes can find it.

---

## Learning Objective

Understand how to:

* register a local process name with `Process.register/2`
* find a locally registered process with `Process.whereis/1`
* send directly to a registered name
* register and find a name through `:global`
* avoid exposing implementation PIDs to clients
* recognize why names alone do not solve process restarts or network partitions

---

## Problem

Every Elixir process has a PID:

```elixir
#PID<0.123.0>
```

It is precise, but it is not a good long-term public identity. A PID changes when the process is restarted:

```text
worker before restart -> #PID<0.123.0>
worker after restart  -> #PID<0.127.0>
```

If clients store and share the original PID, they will keep sending to a process that no longer exists.

Instead, a worker can be available through a name such as `:worker`:

```elixir
Process.register(pid, :worker)
```

Clients can then send messages without knowing the current PID:

```elixir
send(:worker, :stop)
```

The name becomes a lookup handle. The PID remains the identity of this particular running process.

---

## Architecture

### Local Registration

```text
                    Process.register(pid, :worker)

Client                                             Worker
  |                                                   |
  | send(:worker, message)                            |
  |-----------------------> local name lookup --------|
  |                                                   |
  |                                               receives message
```

The local process registry belongs to one BEAM node. On that node, `:worker` resolves to one PID or no PID.

### Global Registration

```text
 Node A                                             Node B

 Client                                             Worker
   |                                                   |
   | :global.whereis_name(:worker)                     |
   |-----------------------> global name service ------|
   |<----------------------- worker PID ----------------|
   |                                                   |
   | send(pid, :stop)                                  |
   |--------------------------------------------------->|
```

`:global` coordinates names among connected Erlang nodes. It is useful for demonstrating node-wide lookup, but it is not a substitute for a complete distributed-service design.

---

## Examples

### 1. Local Process Registration

Run:

```bash
elixir 01-process-registration.exs
```

The script starts a worker and registers it locally:

```elixir
pid = Worker.start()
Process.register(pid, :worker)
```

It finds the worker by name:

```elixir
registered_pid = Process.whereis(:worker)
```

and stops it by sending to that name:

```elixir
send(:worker, :stop)
```

Example output:

```text
Worker PID: #PID<...>
Registered as :worker
Lookup using name: #PID<...>
Stopping using name: #PID<...>
```

The exact PID changes between runs.

### 2. Global Lookup

Run:

```bash
elixir 02-global-lookup.exs
```

The worker is registered through the Erlang `:global` name service:

```elixir
:global.register_name(:worker, pid)
```

The current PID can be looked up with:

```elixir
:global.whereis_name(:worker)
```

The script runs on one node, so the result is still local. When nodes are connected, `:global` coordinates the namespace across those nodes.

### 3. Avoiding PID Sharing

Run:

```bash
elixir 03-avoiding-pid-sharing.exs
```

`Worker.start/0` registers the process internally and returns its public name:

```elixir
Process.register(pid, :worker)
:worker
```

The client only receives the name and uses it as the destination:

```elixir
send(worker_name, {:work, self(), request})
```

The worker replies directly to the caller PID:

```elixir
send(from, {:reply, request, "processed: #{request}"})
```

Example output:

```text
Worker available as: :worker
Response: processed: hello
```

---

## Protocol

The first two scripts use one command:

```elixir
:stop
```

The third script uses a request-reply protocol.

### Request

```elixir
{:work, caller_pid, request}
```

### Reply

```elixir
{:reply, request, response}
```

The client pins `request` when receiving the reply:

```elixir
{:reply, ^request, response} -> response
```

For a concurrent client, a unique reference from `make_ref/0` would be safer than using the request value as the correlation value.

---

## Implementation

### Registering a Local Name

`Process.register/2` associates a name with a live PID on the current node:

```elixir
Process.register(pid, :worker)
```

The name must be unique in that local registry. Attempting to register the same local name for another process fails.

Use `Process.whereis/1` to resolve it:

```elixir
Process.whereis(:worker)
```

It returns the PID while the name is registered, or `nil` if no local process owns that name.

### Sending to a Name

`send/2` accepts a registered local atom as its destination:

```elixir
send(:worker, :stop)
```

The runtime resolves `:worker` at send time. This means callers do not have to look up and keep a PID merely to deliver a message.

### Registering Through `:global`

The global version uses:

```elixir
:global.register_name(:worker, pid)
```

and:

```elixir
:global.whereis_name(:worker)
```

Unlike `Process.register/2`, this namespace is intended to be coordinated across connected nodes. In a distributed system, that coordination has costs and failure behavior that must be considered carefully.

### Hiding the PID From the Client

In the final example, the worker startup code owns the PID and exposes only `:worker`:

```elixir
def start do
  pid = spawn(fn -> loop() end)
  Process.register(pid, :worker)
  :worker
end
```

The client depends on the message protocol and public name, not on the worker's current process identifier.

---

## Local Names, Global Names, and PIDs

| Identifier | Scope | Typical use |
| --- | --- | --- |
| PID | One process incarnation | Direct reply, monitoring, links |
| `Process.register/2` name | One BEAM node | Local service lookup |
| `:global` name | Connected Erlang nodes | Demonstrating cluster-wide lookup |

A registered name is not permanent ownership of an identity. Registration disappears when its owning process dies. A replacement process must register the name again.

---

## Limitations

### Names Do Not Restart Processes

If the registered worker dies, its registration disappears with it:

```text
:worker -> #PID<old>  worker exits  ->  :worker is unregistered
```

Something else still needs to restart the worker and register the replacement. In an OTP application, that is usually a supervisor combined with a registered process name or registry.

### Local Names Are Node-Local

`Process.whereis(:worker)` searches only the current node. Two different nodes can each have a local `:worker`.

### Global Names Need Distributed Nodes

The global example demonstrates the API in one VM. To use a global name across machines, nodes must be named, connected, and able to communicate. Network partitions and simultaneous registration attempts require a carefully chosen consistency and recovery strategy.

### Do Not Create Atoms From Untrusted Input

Names in these examples are fixed atoms such as `:worker`. Atoms are not garbage collected, so code must never create arbitrary atoms from untrusted external input.

---

## Why OTP Provides Registries

Raw registration is excellent for a small, known set of singleton processes. Production systems often need more:

* multiple processes per logical key
* safe concurrent registration
* automatic cleanup and restart integration
* node-aware or distributed lookup policies

OTP's `Registry`, `:via` tuples, `DynamicSupervisor`, and application-specific discovery layers build on the same core need: callers need a stable way to find the process currently responsible for a job.
