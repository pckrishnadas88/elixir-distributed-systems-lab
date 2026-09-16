# 09 - Backpressure

A first-principles implementation of flow control and backpressure mechanisms using raw Elixir process primitives.

This lesson explores what happens when a fast producer overwhelms a slow consumer, demonstrates how BEAM process mailboxes expand uncontrollably under uncoordinated load, and builds two flow control solutions from scratch: synchronous **Stop-and-Wait** and credit-based **Demand-Driven Windowing** (the foundational design of `GenStage` and Reactive Streams).

---

## Learning Objective

Understand how to:

* observe and measure process mailbox growth using `Process.info(pid, :message_queue_len)`
* track per-process heap memory consumption with `Process.info(pid, :memory)`
* identify the dangers of uncoordinated push messaging on the BEAM
* implement synchronous Stop-and-Wait acknowledgment flow control
* design a pull-based, credit-windowed demand pipeline from scratch
* compare Push vs. Pull concurrency models and understand their latency/throughput trade-offs
* recognize how production tools (`GenStage`, `Broadway`) manage load boundaries

---

## The Problem: Unbounded Mailboxes

In Elixir and Erlang, every process possesses its own private mailbox. By design, process mailboxes on the BEAM are **unbounded FIFO queues**.

When Process A calls `send(ProcessB, message)`, the message is immediately placed into Process B's mailbox without blocking Process A.

```text
Fast Producer (10,000 msgs/sec) =====> [ Mailbox: 10k... 50k... 100k ] =====> Slow Consumer (100 msgs/sec)
                                            ^
                                            |
                                  Unbounded Memory Growth
                                  GC overhead degrades node
                                  Risk of VM OOM Crash
```

If the producer generates events faster than the consumer can process them:
1. **Mailbox Explosion**: Hundreds of thousands of unprocessed messages pile up in the consumer's message queue.
2. **Heap Inflation**: The consumer process heap continuously expands to store queued terms, consuming available RAM.
3. **Garbage Collection Degradation**: The BEAM garbage collector must repeatedly scan the growing message queue, consuming excessive CPU.
4. **Out-of-Memory (OOM) Termination**: The host OS or BEAM runtime eventually runs out of memory, killing the entire virtual machine.

**Backpressure** is the feedback mechanism by which a consumer signals its processing capacity back to upstream producers, ensuring producers only emit messages that consumers are ready to process.

---

## Architecture & Solutions

This lesson provides three progressive scripts demonstrating the problem and two fundamental backpressure solutions:

### 1. Uncoordinated Push (The Failure Mode)

The producer blasts messages into the consumer mailbox with zero flow control:

```text
Producer                                                   Consumer
   |                                                          |
   |--- {:item, 1} ------------------------------------------>| [processes 15ms]
   |--- {:item, 2} ------------------------------------------>| (in mailbox)
   |--- {:item, 3} ------------------------------------------>| (in mailbox)
   |...                                                       | ...
   |--- {:item, 100} ---------------------------------------->| (100 items queued!)
   | [Producer finishes in 1ms]                               |
   |                                                          | [Drains backlog slowly over 1.5s]
```

### 2. Stop-and-Wait Flow Control (Credit of 1)

The producer sends exactly one item and pauses until the consumer explicitly acknowledges receipt:

```text
Producer                                                   Consumer
   |                                                          |
   |--- {:item, self(), ref, 1} ----------------------------->|
   |                                                          | [processes item 1]
   |<-- {:ack, ref} ------------------------------------------|
   |                                                          |
   |--- {:item, self(), ref, 2} ----------------------------->|
   |                                                          | [processes item 2]
   |<-- {:ack, ref} ------------------------------------------|
```

* **Mailbox size**: Strictly bounded ($\le 1$).
* **Trade-off**: High latency penalty; the pipeline is idle during message round-trips.

### 3. Demand-Driven Windowing (The GenStage Model)

The consumer maintains a demand budget (window size, e.g. `5`). It pulls work by requesting demand (`{:ask, count}`), and the producer dispatches batches only up to the accumulated demand:

```text
Producer                                                   Consumer
   |                                                          |
   |                                                          | Consumer starts with window = 5
   |<-- {:ask, 5} --------------------------------------------|
   |                                                          |
   |--- {:data, [1, 2, 3, 4, 5]} ---------------------------->|
   |                                                          | [processes items 1..5]
   |<-- {:ask, 5} (replenish demand) -------------------------|
   |                                                          |
   |--- {:data, [6, 7, 8, 9, 10]} --------------------------->|
```

* **Mailbox size**: Strictly bounded by the requested demand window size.
* **Throughput**: Maximized by pipelining work in manageable batches without queue backlog.

---

## Protocols

### Stop-and-Wait Messages (`02-ack-backpressure.exs`)

* Producer $\to$ Consumer:
  ```elixir
  {:item, reply_to_pid, make_ref(), item_data}
  ```
* Consumer $\to$ Producer:
  ```elixir
  {:ack, ref}
  ```

### Demand-Driven Pipeline Messages (`03-demand-driven-pipeline.exs`)

* Consumer $\to$ Producer:
  ```elixir
  {:subscribe, consumer_pid}
  {:ask, count}
  ```
* Producer $\to$ Consumer:
  ```elixir
  {:data, [item1, item2, ...]}
  :stream_done
  ```

---

## Project Structure

```text
09-backpressure/
├── 01-unbounded-mailbox.exs
├── 02-ack-backpressure.exs
├── 03-demand-driven-pipeline.exs
├── README.md
└── blog.md
```

* `01-unbounded-mailbox.exs`: Demonstrates uncoordinated push and captures mailbox growth and memory stats via `Process.info/2`.
* `02-ack-backpressure.exs`: Implements lockstep Stop-and-Wait flow control with correlation references.
* `03-demand-driven-pipeline.exs`: Implements a credit-based demand pipeline buffering work and dispatching on consumer demand.

---

## Running the Examples

### 1. Unbounded Mailbox Growth

```bash
elixir 01-unbounded-mailbox.exs
```

Example output:
```text
=== EXPERIMENT 1: UNBOUNDED MAILBOX GROWTH ===
Producer: Blasting 100 items into consumer mailbox without waiting...
Producer: Finished sending all 100 items!

--- Immediate Snapshot After Producer Finished ---
Items queued in mailbox : 99
Process memory usage    : 12008 bytes
--------------------------------------------------

Waiting for consumer to drain backlog...
Consumer: processed 20 items | Mailbox queue length: 80 | Memory: 10360 bytes
Consumer: processed 40 items | Mailbox queue length: 60 | Memory: 9104 bytes
Consumer: processed 60 items | Mailbox queue length: 40 | Memory: 12280 bytes
Consumer: processed 80 items | Mailbox queue length: 20 | Memory: 10576 bytes
Consumer: processed 100 items | Mailbox queue length: 0 | Memory: 8648 bytes

--- Final Snapshot After Backlog Drained ---
Items queued in mailbox : 0
Process memory usage    : 8704 bytes
---------------------------------------------

Consumer: stopped
Experiment 1 complete.
```

### 2. Stop-and-Wait (Ack-Based) Flow Control

```bash
elixir 02-ack-backpressure.exs
```

Example output:
```text
=== EXPERIMENT 2: ACK-BASED (STOP-AND-WAIT) FLOW CONTROL ===
Producer: Sending 50 items with stop-and-wait acknowledgment...
Consumer: processed item 10 (total: 10) | Mailbox queue length: 0
Consumer: processed item 20 (total: 20) | Mailbox queue length: 0
Consumer: processed item 30 (total: 30) | Mailbox queue length: 0
Consumer: processed item 40 (total: 40) | Mailbox queue length: 0
Consumer: processed item 50 (total: 50) | Mailbox queue length: 0
Producer: All 50 items acknowledged by consumer!

Final consumer mailbox queue length: 0
Experiment 2 complete.
Consumer: stopped
```

### 3. Demand-Driven Pipeline

```bash
elixir 03-demand-driven-pipeline.exs
```

Example output:
```text
=== EXPERIMENT 3: DEMAND-DRIVEN FLOW CONTROL PIPELINE ===
Consumer: Started with window size 5
Consumer: Requesting initial demand of 5 item(s)...
Producer: Consumer #PID<0.106.0> connected
Producer: Received demand for 5 item(s) (total accumulated demand: 5)
Producer: Dispatching batch of 5 item(s) to consumer (remaining buffer: 15, remaining demand: 0)

Consumer: >>> Received batch of 5 items [Mailbox queue length: 0]
Consumer: processed item 1
Consumer: processed item 2
Consumer: processed item 3
Consumer: processed item 4
Consumer: processed item 5
Consumer: Batch done. Replenishing demand (+5)...
Producer: Received demand for 5 item(s) (total accumulated demand: 5)
Producer: Dispatching batch of 5 item(s) to consumer (remaining buffer: 10, remaining demand: 0)

Consumer: >>> Received batch of 5 items [Mailbox queue length: 0]
Consumer: processed item 6
Consumer: processed item 7
Consumer: processed item 8
Consumer: processed item 9
Consumer: processed item 10
Consumer: Batch done. Replenishing demand (+5)...
Producer: Received demand for 5 item(s) (total accumulated demand: 5)
Producer: Dispatching batch of 5 item(s) to consumer (remaining buffer: 5, remaining demand: 0)

Consumer: >>> Received batch of 5 items [Mailbox queue length: 0]
Consumer: processed item 11
Consumer: processed item 12
Consumer: processed item 13
Consumer: processed item 14
Consumer: processed item 15
Consumer: Batch done. Replenishing demand (+5)...
Producer: Received demand for 5 item(s) (total accumulated demand: 5)
Producer: Dispatching batch of 5 item(s) to consumer (remaining buffer: 0, remaining demand: 0)
Producer: Buffer empty, notifying consumer that stream is done

Consumer: >>> Received batch of 5 items [Mailbox queue length: 0]
Consumer: processed item 16
Consumer: processed item 17
Consumer: processed item 18
Consumer: processed item 19
Consumer: processed item 20
Consumer: Batch done. Replenishing demand (+5)...

Consumer: Received stream_done signal. Total processed: 20
Producer: Received demand for 5 item(s) (total accumulated demand: 5)

Pipeline successfully completed! Total items processed: 20
Final consumer mailbox queue length: 0
Consumer: stopped
Producer: stopped
Experiment 3 complete.
```

---

## Key Concepts

### 1. Observing Mailboxes with `Process.info/2`

You can inspect a process's mailbox size without sending it a message or interrupting its execution:

```elixir
{:message_queue_len, queue_len} = Process.info(consumer_pid, :message_queue_len)
{:memory, memory_bytes} = Process.info(consumer_pid, :memory)
```

This inspection is performed directly by the BEAM runtime out-of-band and is invaluable for monitoring, telemetry, and health checks.

### 2. Push vs. Pull

| Feature | Push Model | Pull (Demand) Model |
| :--- | :--- | :--- |
| **Driver** | Producer pushes when ready | Consumer requests when ready |
| **Mailbox Risk** | Unbounded growth | Strictly bounded by requested demand |
| **Flow Control** | Reactive / difficult | Inherent in the protocol |
| **Throughput** | High until crash / OOM | Tunable via window/batch size |

### 3. What Happens When Upstream Cannot Be Throttled?

In internal BEAM pipelines, pull-based demand easily halts upstream producers. But what happens if the producer is an external input that cannot be stopped (such as physical network traffic or sensor streams)?

Systems must choose a bounded buffer strategy:
1. **Drop Newest**: Discard incoming messages when buffer is full.
2. **Drop Oldest**: Discard unhandled old messages to prioritize freshness.
3. **Reject with Error**: Return HTTP 429 (Too Many Requests) or TCP backpressure.

---

## Production Context

* **`GenStage`**: The foundational specification for exchange of events with backpressure between Elixir processes. Defines `Producer`, `ProducerConsumer`, and `Consumer` stages exchanging demand via `handle_demand/2`.
* **`Broadway`**: Built on top of `GenStage` for building concurrent, multi-stage data ingestion pipelines (e.g. Amazon SQS, RabbitMQ, Apache Kafka) with automatic rate limiting, concurrency partitioning, and failure handling.

---

## Next Step

This concludes the **Process Coordination** section of the lab!

Next, we enter **Distributed BEAM**:
In **10 - Connecting Nodes**, we move beyond single-node concurrency and learn how multiple BEAM instances discover, connect, and cluster with each other using `Node.connect/1`.

---

## References

* Blog post: [Building Distributed Systems in Elixir: Part 9 — Backpressure](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-9-backpressure)
* Previous: [08 - Publish / Subscribe](../08-publish-subscribe/README.md)
* Next: [10 - Connecting Nodes](../10-connecting-nodes/README.md) *(upcoming)*
* Elixir documentation: [`Process.info/2`](https://hexdocs.pm/elixir/Process.html#info/2)
* HexDocs: [`GenStage`](https://hexdocs.pm/gen_stage/GenStage.html)

