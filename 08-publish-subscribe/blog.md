# Building Distributed Systems in Elixir: Part 8 — Publish / Subscribe

In the previous part of this series, we built a **Worker Pool** from scratch.

We saw how a coordinator process could distribute pending jobs one by one to a pool of available workers, strictly bounding concurrency and establishing demand-driven coordination:

```text
Job 1 ---> [ Coordinator ] ---> Worker 1
Job 2 ---> [ Coordinator ] ---> Worker 2
```

Every interaction in that system was **1-to-1 (point-to-point)**: one coordinator assigning one job to exactly one worker process.

But what happens when an event needs to be received by *multiple* independent components simultaneously?

Consider a web application or distributed backend:
* A customer completes a checkout.
* The **Inventory Service** must adjust stock levels.
* The **Email Service** must dispatch a confirmation receipt.
* The **Fraud Detection Service** must analyze transaction velocity.
* The **Analytics Service** must log the conversion event.

If the order-processing process had to know about and send a message directly to every one of those services, your system becomes tightly coupled, brittle, and difficult to extend.

In this part, we will build a **Publish / Subscribe (PubSub) Broker** from scratch using raw process primitives:

```elixir
spawn/1
send/2
receive/1
Process.monitor/1
```

No `GenServer`.

No `Registry`.

No `Phoenix.PubSub`.

No RabbitMQ or Redis.

The goal is to understand the core mechanics of 1-to-many communication, topic routing tables, fan-out broadcasting, and subscriber lifecycle management before relying on high-level libraries.

---

## The Problem: Why Point-to-Point Messaging Falls Short

On the BEAM, message passing with `send/2` is inherently point-to-point:

```elixir
send(target_pid, message)
```

You have a sender, a recipient PID, and an isolated mailbox.

If a component needs to notify three different processes about an event, the naive approach is to loop over a list of PIDs:

```text
Naively Coupled Publisher:
                    +---> [ Inventory Service ]
                    |
[ Order Producer ] -+---> [ Email Service ]
                    |
                    +---> [ Analytics Service ]
```

While this works for trivial scripts, it introduces three fundamental architectural problems:

1. **Spatial Coupling**: The producer must know the concrete identity (or PID) of every recipient. If you add a new reporting service, you must modify the order producer.
2. **Temporal Coupling**: Every recipient must already be known and alive when the producer starts. Subscribers cannot dynamically register or leave without coordinating directly with the producer.
3. **Failure Exposure & Blast Radius**: If the producer attempts to synchronously verify delivery to each recipient, a slow or crashing consumer can delay or crash the producer itself.

To solve this, distributed architectures rely on the **Publish / Subscribe** pattern.

---

## The Publish / Subscribe Architecture

The Pub/Sub pattern introduces a central intermediary: the **Broker**.

```text
                                  +---------------------------------------+
                                  |                Broker                 |
                                  |---------------------------------------|
                                  | topics:                               |
                                  |   :tech_news => [Alice, Bob]          |
                                  |   :sports    => [Alice, Charlie]      |
                                  |   :weather   => [Charlie]             |
                                  | monitors:                             |
                                  |   Alice   => #Reference<0.1.1>        |
                                  |   Bob     => #Reference<0.1.2>        |
                                  |   Charlie => #Reference<0.1.3>        |
                                  +---------------------------------------+
                                        /             |             \
            {:broadcast, topic, msg}   /              |              \   {:broadcast, topic, msg}
                                      v               v               v
                              +---------------+ +---------------+ +---------------+
                              |  Subscriber   | |  Subscriber   | |  Subscriber   |
                              |    (Alice)    | |     (Bob)     | |   (Charlie)   |
                              +---------------+ +---------------+ +---------------+
```

The system is organized around three roles:

1. **Publisher**: Produces events tagged with a **topic** (e.g. `:tech_news` or `:sports`). The publisher sends its event to the Broker and has zero knowledge of who (if anyone) is listening.
2. **Broker**: Maintains an in-memory routing table mapping topics to lists of subscriber PIDs. When an event arrives, the broker fans out the message to every subscriber registered for that topic.
3. **Subscriber**: Expresses interest in specific topics by registering with the broker. When a broadcast arrives, it handles the event in its own process mailbox without blocking other subscribers.

---

## Step 1: Designing the Subscriber Process

A subscriber is an independent process that receives broadcast notifications. It maintains a small internal state tracking its human-readable name and the total number of events received.

Create `subscriber.exs`:

```elixir
defmodule Subscriber do
  # ============================================================
  # Subscriber Process
  #
  # A subscriber:
  # 1. Starts with an assigned human-readable name.
  # 2. Listens for broadcast messages sent by the Broker.
  # 3. Processes each received broadcast event.
  # 4. Supports graceful shutdown and crash demonstration.
  # ============================================================

  def start(name) do
    spawn(fn ->
      IO.puts("Subscriber #{name} started with PID #{inspect(self())}")
      loop(name, 0)
    end)
  end

  # ============================================================
  # Subscriber Loop
  #
  # Maintains process state:
  # - name: identifier for logging
  # - received_count: number of broadcasts processed
  # ============================================================

  defp loop(name, received_count) do
    receive do
      # Broadcast message received from the Broker.
      {:broadcast, topic, message} ->
        updated_count = received_count + 1

        IO.puts(
          "[#{name}] Received on #{inspect(topic)}: " <>
            "#{inspect(message)} (total received: #{updated_count})"
        )

        loop(name, updated_count)

      # Query total received message count.
      {:get_count, caller, ref} ->
        send(caller, {:count, ref, received_count})
        loop(name, received_count)

      # Deliberate crash to demonstrate Broker monitor cleanup.
      :simulate_crash ->
        IO.puts("[#{name}] Simulating sudden process crash!")
        exit(:simulated_crash)

      # Graceful shutdown.
      :stop ->
        IO.puts("[#{name}] Stopped gracefully")
    end
  end
end
```

### How the Subscriber Works
* When a message arrives in the form `{:broadcast, topic, message}`, the subscriber updates its local counter and prints the event.
* Because every subscriber runs in its own BEAM process, slow handling in one subscriber does not prevent another subscriber from processing its messages.
* We include `:simulate_crash` so we can test how the broker responds when a subscriber dies unexpectedly.

---

## Step 2: Designing the Broker State & Protocol

The Broker is the heart of the system. What state must it hold?

1. **`topics`**: A map from topic names to lists of subscriber PIDs:
   ```elixir
   %{
     :tech_news => [#PID<0.109.0>, #PID<0.110.0>],
     :sports    => [#PID<0.109.0>, #PID<0.111.0>]
   }
   ```
2. **`monitors`**: A map tracking which subscriber processes are currently monitored:
   ```elixir
   %{
     #PID<0.109.0> => #Reference<0.1.1>,
     #PID<0.110.0> => #Reference<0.1.2>
   }
   ```

### Why Do We Need `Process.monitor/1` in PubSub?

If a subscriber crashes without unsubscribing, its PID remains in the broker's topic map.

In standard Erlang/Elixir, sending a message via `send/2` to a dead PID does **not** crash the sender—it silently fails as a no-op. However:
1. The dead PID stays in the broker's memory forever (a **memory leak**).
2. The broker wastes CPU cycles traversing and sending to defunct PIDs on every broadcast.
3. Subscriber counts reported by the broker become inaccurate.

By monitoring subscribers with `Process.monitor/1` (from [Part 3: Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)), the broker automatically receives a `{:DOWN, ...}` notification when a subscriber crashes, allowing it to prune the dead PID immediately!

---

## Step 3: Implementing the Broker

Create `broker.exs`:

```elixir
defmodule Broker do
  # ============================================================
  # Publish / Subscribe Broker
  #
  # The Broker is a central coordination process responsible for:
  # 1. Tracking topics and their subscribed process IDs.
  # 2. Monitoring subscribers so dead processes are pruned automatically.
  # 3. Broadcasting messages to all subscribers registered for a topic.
  # 4. Handling dynamic subscriptions and unsubscriptions.
  # ============================================================

  # ============================================================
  # Client API
  # ============================================================

  def start do
    spawn(fn ->
      IO.puts("Broker started with PID #{inspect(self())}")
      loop(%{topics: %{}, monitors: %{}})
    end)
  end

  def subscribe(broker, topic, subscriber_pid \\ nil) do
    target_pid = subscriber_pid || self()
    ref = make_ref()
    send(broker, {:subscribe, self(), ref, topic, target_pid})

    receive do
      {:ok, ^ref} -> :ok
    after
      5000 -> {:error, :timeout}
    end
  end

  def unsubscribe(broker, topic, subscriber_pid \\ nil) do
    target_pid = subscriber_pid || self()
    ref = make_ref()
    send(broker, {:unsubscribe, self(), ref, topic, target_pid})

    receive do
      {:ok, ^ref} -> :ok
    after
      5000 -> {:error, :timeout}
    end
  end

  def publish(broker, topic, message) do
    send(broker, {:publish, topic, message})
    :ok
  end

  def subscribers(broker, topic) do
    ref = make_ref()
    send(broker, {:get_subscribers, self(), ref, topic})

    receive do
      {:subscribers, ^ref, subs} -> subs
    after
      5000 -> {:error, :timeout}
    end
  end

  def stop(broker) do
    send(broker, :stop)
    :ok
  end

  # ============================================================
  # Server Loop
  # ============================================================

  defp loop(state) do
    receive do
      # ========================================================
      # SUBSCRIBE
      # Register subscriber PID under a topic and monitor it.
      # ========================================================
      {:subscribe, caller, ref, topic, subscriber_pid} ->
        existing_subscribers = Map.get(state.topics, topic, [])

        # Avoid duplicate subscriptions for the same topic.
        updated_subscribers =
          if subscriber_pid in existing_subscribers do
            existing_subscribers
          else
            existing_subscribers ++ [subscriber_pid]
          end

        # Monitor subscriber if not already monitored.
        monitors =
          case Map.get(state.monitors, subscriber_pid) do
            nil ->
              mref = Process.monitor(subscriber_pid)
              Map.put(state.monitors, subscriber_pid, mref)

            _existing_ref ->
              state.monitors
          end

        updated_topics = Map.put(state.topics, topic, updated_subscribers)

        IO.puts(
          "Broker: #{inspect(subscriber_pid)} subscribed to topic #{inspect(topic)}"
        )

        send(caller, {:ok, ref})

        loop(%{state | topics: updated_topics, monitors: monitors})

      # ========================================================
      # UNSUBSCRIBE
      # Remove subscriber PID. Demonitor if no longer on any topic.
      # ========================================================
      {:unsubscribe, caller, ref, topic, subscriber_pid} ->
        existing_subscribers = Map.get(state.topics, topic, [])
        updated_subscribers = List.delete(existing_subscribers, subscriber_pid)

        updated_topics =
          if updated_subscribers == [] do
            Map.delete(state.topics, topic)
          else
            Map.put(state.topics, topic, updated_subscribers)
          end

        IO.puts(
          "Broker: #{inspect(subscriber_pid)} unsubscribed from topic #{inspect(topic)}"
        )

        # Demonitor if subscriber has no remaining topic subscriptions.
        still_subscribed? =
          Enum.any?(updated_topics, fn {_t, subs} ->
            subscriber_pid in subs
          end)

        monitors =
          if not still_subscribed? and Map.has_key?(state.monitors, subscriber_pid) do
            mref = Map.get(state.monitors, subscriber_pid)
            Process.demonitor(mref, [:flush])
            Map.delete(state.monitors, subscriber_pid)
          else
            state.monitors
          end

        send(caller, {:ok, ref})

        loop(%{state | topics: updated_topics, monitors: monitors})

      # ========================================================
      # PUBLISH / BROADCAST
      # Fan-out: send message to all registered topic subscribers.
      # ========================================================
      {:publish, topic, message} ->
        subscribers = Map.get(state.topics, topic, [])

        IO.puts(
          "Broker: Broadcasting to #{length(subscribers)} subscriber(s) " <>
            "on topic #{inspect(topic)}: #{inspect(message)}"
        )

        Enum.each(subscribers, fn sub_pid ->
          send(sub_pid, {:broadcast, topic, message})
        end)

        loop(state)

      # ========================================================
      # GET SUBSCRIBERS
      # ========================================================
      {:get_subscribers, caller, ref, topic} ->
        subs = Map.get(state.topics, topic, [])
        send(caller, {:subscribers, ref, subs})
        loop(state)

      # ========================================================
      # SUBSCRIBER CRASH DETECTION
      # Clean up dead subscriber from all topic lists.
      # ========================================================
      {:DOWN, _ref, :process, dead_pid, reason} ->
        IO.puts(
          "Broker: Subscriber #{inspect(dead_pid)} exited (#{inspect(reason)}). " <>
            "Pruning from all topics."
        )

        # Remove dead PID across all topic lists.
        updated_topics =
          state.topics
          |> Enum.map(fn {topic, subs} ->
            {topic, List.delete(subs, dead_pid)}
          end)
          |> Enum.reject(fn {_topic, subs} -> subs == [] end)
          |> Map.new()

        updated_monitors = Map.delete(state.monitors, dead_pid)

        loop(%{state | topics: updated_topics, monitors: updated_monitors})

      # ========================================================
      # STOP
      # ========================================================
      :stop ->
        IO.puts("Broker: Stopped gracefully")
    end
  end
end
```

### Key Technical Details

1. **Synchronous Subscribe / Unsubscribe**: Notice how `subscribe/3` and `unsubscribe/3` use correlated request-reply (from [Part 2](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)) with `make_ref()`. This ensures that when the client's `Broker.subscribe(...)` call returns, the subscriber is guaranteed to be recorded in the broker's state. There is no race condition where a message published immediately after subscription is dropped.
2. **Asynchronous Publish**: Publishing via `Broker.publish/3` is fire-and-forget. The publisher sends `{:publish, topic, message}` and immediately proceeds without waiting for subscribers to read or process the message.
3. **Fan-Out via `Enum.each/2`**: Delivering to $N$ subscribers requires looping through the topic's PID list and executing `send(sub_pid, {:broadcast, topic, message})`. Because `send/2` merely enqueues the term into the recipient process's mailbox and returns immediately, the fan-out loop is exceptionally fast.

---

## Step 4: Putting It All Together

Now let's write a demonstration script that puts our PubSub broker through real-world scenarios:
1. Spawning multiple subscribers (`Alice`, `Bob`, `Charlie`).
2. Registering multi-topic subscriptions.
3. Broadcasting messages across different topics.
4. Unsubscribing a process dynamically.
5. Simulating a subscriber crash and watching the broker automatically prune the dead process.
6. Performing a graceful shutdown.

Create `pubsub.exs`:

```elixir
Code.require_file("subscriber.exs", __DIR__)
Code.require_file("broker.exs", __DIR__)

IO.puts("=== 1. STARTING BROKER AND SUBSCRIBERS ===")
broker = Broker.start()

alice = Subscriber.start("Alice")
bob = Subscriber.start("Bob")
charlie = Subscriber.start("Charlie")

Process.sleep(50)

IO.puts("\n=== 2. REGISTERING TOPIC SUBSCRIPTIONS ===")
# Alice subscribes to :tech_news and :sports
Broker.subscribe(broker, :tech_news, alice)
Broker.subscribe(broker, :sports, alice)

# Bob subscribes to :tech_news
Broker.subscribe(broker, :tech_news, bob)

# Charlie subscribes to :sports and :weather
Broker.subscribe(broker, :sports, charlie)
Broker.subscribe(broker, :weather, charlie)

Process.sleep(50)

IO.puts("\n=== 3. BROADCAST MESSAGING ===")

IO.puts("\n--- Broadcast to :tech_news (Alice and Bob should receive) ---")
Broker.publish(broker, :tech_news, "Elixir 1.18 Released!")
Process.sleep(50)

IO.puts("\n--- Broadcast to :sports (Alice and Charlie should receive) ---")
Broker.publish(broker, :sports, "Local team wins championship!")
Process.sleep(50)

IO.puts("\n--- Broadcast to :weather (Only Charlie should receive) ---")
Broker.publish(broker, :weather, "Sunny, 24C with light breeze")
Process.sleep(50)

IO.puts("\n--- Broadcast to :finance (No subscribers) ---")
Broker.publish(broker, :finance, "Market closes at record high")
Process.sleep(50)

IO.puts("\n=== 4. DYNAMIC UNSUBSCRIPTION ===")
IO.puts("Bob unsubscribes from :tech_news")
Broker.unsubscribe(broker, :tech_news, bob)
Process.sleep(50)

IO.puts("\n--- Broadcast to :tech_news (Now only Alice should receive) ---")
Broker.publish(broker, :tech_news, "Distributed Systems Lab Part 8 published!")
Process.sleep(50)

IO.puts("\n=== 5. FAULT TOLERANCE & MONITOR CLEANUP ===")
IO.puts("Simulating sudden crash for Charlie...")
send(charlie, :simulate_crash)
Process.sleep(100)

IO.puts("\n--- Broadcast to :sports (Charlie was pruned; only Alice receives) ---")
Broker.publish(broker, :sports, "Upcoming match schedule updated")
Process.sleep(50)

IO.puts("\n=== 6. CLEANUP & SHUTDOWN ===")
send(alice, :stop)
send(bob, :stop)
Broker.stop(broker)
Process.sleep(50)

IO.puts("\nAll pub/sub demonstrations completed successfully.")
```

---

## Running the System

Execute the script with:

```bash
elixir pubsub.exs
```

Here is the exact execution output:

```text
=== 1. STARTING BROKER AND SUBSCRIBERS ===
Subscriber Charlie started with PID #PID<0.111.0>
Subscriber Bob started with PID #PID<0.110.0>
Subscriber Alice started with PID #PID<0.109.0>
Broker started with PID #PID<0.108.0>

=== 2. REGISTERING TOPIC SUBSCRIPTIONS ===
Broker: #PID<0.109.0> subscribed to topic :tech_news
Broker: #PID<0.109.0> subscribed to topic :sports
Broker: #PID<0.110.0> subscribed to topic :tech_news
Broker: #PID<0.111.0> subscribed to topic :sports
Broker: #PID<0.111.0> subscribed to topic :weather

=== 3. BROADCAST MESSAGING ===

--- Broadcast to :tech_news (Alice and Bob should receive) ---
Broker: Broadcasting to 2 subscriber(s) on topic :tech_news: "Elixir 1.18 Released!"
[Alice] Received on :tech_news: "Elixir 1.18 Released!" (total received: 1)
[Bob] Received on :tech_news: "Elixir 1.18 Released!" (total received: 1)

--- Broadcast to :sports (Alice and Charlie should receive) ---
Broker: Broadcasting to 2 subscriber(s) on topic :sports: "Local team wins championship!"
[Alice] Received on :sports: "Local team wins championship!" (total received: 2)
[Charlie] Received on :sports: "Local team wins championship!" (total received: 1)

--- Broadcast to :weather (Only Charlie should receive) ---
Broker: Broadcasting to 1 subscriber(s) on topic :weather: "Sunny, 24C with light breeze"
[Charlie] Received on :weather: "Sunny, 24C with light breeze" (total received: 2)

--- Broadcast to :finance (No subscribers) ---
Broker: Broadcasting to 0 subscriber(s) on topic :finance: "Market closes at record high"

=== 4. DYNAMIC UNSUBSCRIPTION ===
Bob unsubscribes from :tech_news
Broker: #PID<0.110.0> unsubscribed from topic :tech_news

--- Broadcast to :tech_news (Now only Alice should receive) ---
Broker: Broadcasting to 1 subscriber(s) on topic :tech_news: "Distributed Systems Lab Part 8 published!"
[Alice] Received on :tech_news: "Distributed Systems Lab Part 8 published!" (total received: 3)

=== 5. FAULT TOLERANCE & MONITOR CLEANUP ===
Simulating sudden crash for Charlie...
[Charlie] Simulating sudden process crash!
Broker: Subscriber #PID<0.111.0> exited (:simulated_crash). Pruning from all topics.

--- Broadcast to :sports (Charlie was pruned; only Alice receives) ---
Broker: Broadcasting to 1 subscriber(s) on topic :sports: "Upcoming match schedule updated"
[Alice] Received on :sports: "Upcoming match schedule updated" (total received: 4)

=== 6. CLEANUP & SHUTDOWN ===
[Alice] Stopped gracefully
[Bob] Stopped gracefully
Broker: Stopped gracefully

All pub/sub demonstrations completed successfully.
```

Let's dissect what just happened:
1. **Topic Isolation**: When `"Elixir 1.18 Released!"` was published to `:tech_news`, only Alice and Bob received it. Charlie's mailbox remained untouched.
2. **Selective Overlap**: When `"Local team wins championship!"` was published to `:sports`, Alice and Charlie received it, while Bob was untouched.
3. **Empty Topics**: When publishing to `:finance`, the broker detected 0 subscribers and safely did nothing without errors.
4. **Dynamic Unsubscribe**: When Bob unsubscribed from `:tech_news`, the next broadcast only reached Alice.
5. **Monitor Pruning**: When Charlie crashed, the broker received `{:DOWN, ...}`, removed Charlie's PID from `:sports` and `:weather`, and logged the pruning. The subsequent broadcast to `:sports` succeeded cleanly with zero attempts to send to a dead process.

---

## Deep Dive: The Cost of Fan-Out and Unbounded Mailboxes

While Publish/Subscribe provides clean decoupling, it introduces specific runtime behaviors on the BEAM that every engineer must understand:

### 1. Data Copying vs. Reference Counting
When sending a message to $N$ subscribers on the same BEAM node:
* Terms smaller than 64 bytes are copied into each subscriber process's private heap.
* Binaries larger than 64 bytes ("refc binaries") are allocated on a shared global heap; sending them simply passes a lightweight pointer and increments a reference counter.

This makes broadcasting large payloads (e.g., JSON payloads, images, or audio chunks) very memory-efficient in Elixir!

### 2. The Slow Consumer Problem
In our implementation, broadcasting is fire-and-forget:

```elixir
Enum.each(subscribers, fn sub -> send(sub, {:broadcast, topic, message}) end)
```

What happens if a publisher produces 1,000 messages per second, but Subscriber $B$ does slow database operations and can only process 10 messages per second?

Because process mailboxes are unbounded by default on the BEAM:
* Messages pile up in Subscriber $B$'s mailbox.
* Memory usage grows linearly.
* Message queue traversal becomes progressively slower.
* Eventually, the BEAM node exhausts all available RAM and crashes.

This critical challenge is known as **lack of backpressure**.

---

## How Real-World Elixir Systems Handle PubSub

In production Elixir and Erlang systems, several battle-tested abstractions build on these exact principles:

1. **`Registry`**: Built into Elixir standard library since Elixir 1.4. Provides decentralized, concurrent pub/sub via concurrent read/write ETS tables with `dispatch/3`.
2. **`Phoenix.PubSub`**: The industry standard for real-time web applications (e.g. Phoenix Channels, LiveView). Uses process partitioning across multiple pool shards to eliminate single-process message bottlenecks, and integrates Erlang's `:pg` for distributed cluster-wide broadcasting.
3. **`:pg` (Process Groups)**: Built into OTP 23+. Manages distributed process groups across connected BEAM nodes with automatic network partition handling and node joining/leaving.

---

## Key Technical Takeaways

1. **Decoupling**: Pub/Sub replaces $M \times N$ direct point-to-point connections with an intermediary broker, allowing publishers and subscribers to scale independently.
2. **Topic Routing**: Storing subscriptions as `%{topic => [pids]}` enables efficient, content-based message routing.
3. **Lifecycle Management**: Unsubscribing must clean up monitor references with `Process.demonitor(ref, [:flush])` to avoid leaking system monitors.
4. **Crash Cleanup**: Using `Process.monitor/1` enables the broker to automatically purge dead processes on `{:DOWN, ...}`, preventing zombie subscriptions.

---

## Next in the Series

Now that our system can broadcast unlimited messages to multiple subscribers, what happens when a subscriber cannot keep up with the incoming flood of events?

In **Part 9: Backpressure**, we'll explore fast producers, slow consumers, mailbox memory exhaustion, and how to build demand-driven flow control to keep systems resilient under heavy load.

---

## Series Navigation

1. [Building a Stateful Process in Elixir Without GenServer](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)
2. [Correlated Request/Reply](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)
3. [Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)
4. [Process Linking](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-4-process-linking-okb)
5. [Supervisor From Scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh)
6. [Named Processes](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-6-named-processes-3k8a)
7. [Worker Pool](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-7-worker-pool-f23)
8. **Publish / Subscribe**
9. Backpressure *(coming soon)*

---

← Previous: [Part 7 — Worker Pool](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-7-worker-pool-f23)

Next: Part 9 — Backpressure *(coming soon)* →

---

Source code repository: [github.com/pckrishnadas88/elixir-distributed-systems-lab](https://github.com/pckrishnadas88/elixir-distributed-systems-lab/tree/main/08-publish-subscribe)

---

## Content License

This article is licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/). You may share and adapt it with attribution for non-commercial purposes. The accompanying source code remains licensed under the MIT License.

