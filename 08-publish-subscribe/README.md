# 08 - Publish / Subscribe

A minimal Publish / Subscribe broker and broadcast messaging implementation using raw Elixir process primitives.

This lesson demonstrates how to decouple message producers from consumers, manage dynamic topic rosters, broadcast messages to multiple concurrent subscribers, and clean up dead subscribers automatically using `Process.monitor/1`—all without `GenServer`, `Registry`, `Phoenix.PubSub`, or external message brokers.

---

## Learning Objective

Understand how to:

* implement 1-to-many communication across concurrent processes
* decouple publishers from subscribers (spatial and temporal decoupling)
* manage dynamic topic subscriptions in process state (`%{topic => [pid]}`)
* execute fan-out broadcast messaging to multiple target processes
* detect crashed or terminated subscribers and prune dead PIDs using `Process.monitor/1`
* safely unsubscribe processes and demonitor them to avoid resource leaks
* analyze the concurrency trade-offs and memory implications of fan-out message distribution

---

## Problem

In previous lessons, our communication patterns were strictly **1-to-1 (point-to-point)**:

1. **Request / Reply**: Client `#PID<0.100.0>` sends a request to Server `#PID<0.101.0>`.
2. **Worker Pool**: Coordinator dispatches 1 task to 1 available worker at a time.

```text
1-to-1 Point-to-Point:
[ Sender ] ---------------------> [ Receiver ]
```

When building real-world distributed systems, multiple components often need to react to the same event simultaneously:

* When an order is placed, notify the **Inventory Worker**, the **Email Notifier**, and the **Analytics Logger**.
* When telemetry metrics change, notify all connected **Dashboard Sessions**.

If the event producer had to know about and send to each receiver directly:

```text
Tightly Coupled Producer:
                    +---> [ Inventory Service ]
                    |
[ Order Producer ] -+---> [ Email Service ]
                    |
                    +---> [ Analytics Service ]
```

This creates critical problems:
1. **Spatial Coupling**: The producer must know the PIDs or identities of every recipient.
2. **Temporal Coupling**: Adding or removing listeners requires modifying or notifying the producer.
3. **Failure Exposure**: If sending to one recipient blocks or crashes the producer, other notifications fail.

A **Publish / Subscribe Broker** introduces a decoupled intermediary. Producers publish events to a named **topic**, and the broker fans out the message to all registered **subscribers**.

```text
Decoupled Pub/Sub:
[ Publisher ] ---> {:publish, :orders, event} ---> [ Broker ]
                                                        |
                     +----------------------------------+----------------------------------+
                     |                                  |                                  |
             {:broadcast, ...}                  {:broadcast, ...}                  {:broadcast, ...}
                     v                                  v                                  v
           [ Inventory Worker ]                  [ Email Notifier ]               [ Analytics Logger ]
```

---

## Architecture

### System Topology

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

### Topic Subscription Flow

```text
Client / Subscriber                                    Broker
       |                                                 |
       |  {:subscribe, self(), ref, :tech_news, pid}     |
       |------------------------------------------------>|  1. Add pid to :tech_news list
       |                                                 |  2. Process.monitor(pid)
       |                   {:ok, ref}                    |
       |<------------------------------------------------|  3. Return confirmation
```

### Broadcast Flow

```text
Publisher                                             Broker                                         Subscribers
    |                                                    |                                                |
    |  {:publish, :tech_news, "Elixir 1.18 Released!"}   |                                                |
    |--------------------------------------------------->|                                                |
    |                                                    |--- {:broadcast, :tech_news, msg} ------------->| [Alice]
    |                                                    |--- {:broadcast, :tech_news, msg} ------------->| [Bob]
```

### Automatic Fault Pruning Flow

```text
Subscriber (Charlie)                                  Broker                                         Other Subscribers
       |                                                 |                                                |
       |  [ Crashes / Exits ]                            |                                                |
       X - - - - - - - - - - - - - - - - - - - - - - - ->|                                                |
                                                         |  1. Receives {:DOWN, ref, :process, pid, ...}  |
                                                         |  2. Prunes dead PID from all topic lists       |
                                                         |  3. Subsequent broadcasts never send to dead X |
                                                         |                                                |
                                                         |--- {:broadcast, :sports, msg} ---------------->| [Alice]
```

---

## Protocol

### Client to Broker Messages

1. **Subscribe**:
   ```elixir
   {:subscribe, caller_pid, ref, topic, subscriber_pid}
   ```
   Confirmation reply:
   ```elixir
   {:ok, ref}
   ```

2. **Unsubscribe**:
   ```elixir
   {:unsubscribe, caller_pid, ref, topic, subscriber_pid}
   ```
   Confirmation reply:
   ```elixir
   {:ok, ref}
   ```

3. **Publish / Broadcast**:
   ```elixir
   {:publish, topic, message}
   ```

4. **Query Subscribers**:
   ```elixir
   {:get_subscribers, caller_pid, ref, topic}
   ```
   Reply:
   ```elixir
   {:subscribers, ref, [subscriber_pid, ...]}
   ```

5. **Stop Broker**:
   ```elixir
   :stop
   ```

### Broker to Subscriber Messages

1. **Broadcast Event**:
   ```elixir
   {:broadcast, topic, message}
   ```

### System Signals Handled by Broker

1. **Process Crash / Termination**:
   ```elixir
   {:DOWN, ref, :process, dead_pid, reason}
   ```

---

## Project Structure

```text
08-publish-subscribe/
├── subscriber.exs
├── broker.exs
├── pubsub.exs
├── README.md
└── blog.md
```

* `subscriber.exs`: Implements `Subscriber` process module (maintains state, logs broadcasts, tracks received counts, supports crash simulation).
* `broker.exs`: Implements `Broker` process module (stores topic-to-subscriber mappings, monitors subscriber processes, executes fan-out delivery, prunes dead PIDs).
* `pubsub.exs`: Standalone demonstration script coordinating the broker, spawning multiple subscribers, broadcasting messages, demonstrating unsubscriptions, and testing monitor cleanup upon crash.

---

## Running

Execute the demonstration script using the Elixir CLI:

```bash
elixir pubsub.exs
```

### Example Output

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

---

## Key Concepts

### 1. Spatial and Temporal Decoupling

* **Spatial Decoupling**: Publishers never hold references or PIDs for individual subscribers. They interact exclusively with the broker using abstract topics (atoms or strings).
* **Temporal Decoupling**: Subscribers can join, leave, or crash at any time without coordinating with or interrupting active publishers.

### 2. Topic-Based Routing & Fan-Out

The broker stores subscriptions as a dictionary:

```elixir
%{
  :tech_news => [#PID<0.109.0>, #PID<0.110.0>],
  :sports    => [#PID<0.109.0>]
}
```

Broadcasting is an $O(N)$ fan-out operation across the subscribers of the target topic:

```elixir
Enum.each(subscribers, fn sub_pid ->
  send(sub_pid, {:broadcast, topic, message})
end)
```

Because `send/2` is asynchronous and non-blocking on the BEAM, broadcasting places messages into each subscriber's process mailbox almost instantaneously without waiting for consumers to finish processing.

### 3. Automatic Cleanup via Process Monitoring

If a subscriber process crashes without explicitly unsubscribing, its PID would remain in the broker's topic map indefinitely—causing the broker to repeatedly send messages to a dead PID (a "zombie subscription").

By creating a monitor (`Process.monitor(subscriber_pid)`) upon subscription:
1. The broker receives `{:DOWN, ref, :process, dead_pid, reason}` when the subscriber terminates.
2. The broker automatically purges the dead PID from all topic lists.
3. Demonitoring occurs cleanly when a subscriber unsubscribes from its final topic.

---

## Limitations

* **Single Broker Bottleneck**: A single process broker serializes all publish and subscribe requests. If tens of thousands of messages are published per second, the broker's single mailbox becomes a bottleneck.
* **Mailbox Accumulation (No Backpressure)**: If a subscriber processes messages slowly, its process mailbox will grow without bound, eventually consuming all system memory. (This sets up **Lesson 09: Backpressure**).
* **Transient Delivery**: Messages are not persisted. If a subscriber is offline or restarted during a broadcast, the message is lost.
* **Local Node Only**: This implementation coordinates processes on a single BEAM node. (Distributed pub/sub across networked nodes will be explored in later modules).

---

## Next Step

Project 09 explores **Backpressure**—analyzing what happens when fast producers overwhelm slow consumers, how mailboxes grow uncontrollably, and how to protect systems against memory exhaustion.

---

## References

* Blog post: [Building Distributed Systems in Elixir: Part 8 — Publish / Subscribe](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-8-publish-subscribe)
* Previous: [07 - Worker Pool](../07-worker-pool/README.md)
* Next: [09 - Backpressure](../09-backpressure/README.md) *(upcoming)*
* Elixir documentation: [`send/2`](https://hexdocs.pm/elixir/Process.html#send/2)
* Elixir documentation: [`Process.monitor/1`](https://hexdocs.pm/elixir/Process.html#monitor/1)
* Elixir documentation: [`Process.demonitor/2`](https://hexdocs.pm/elixir/Process.html#demonitor/2)

