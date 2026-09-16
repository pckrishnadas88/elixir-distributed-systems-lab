# Building Distributed Systems in Elixir: Part 9 — Backpressure

In the previous part of this series, we built a **Publish / Subscribe Broker** from scratch.

We saw how a central coordinator could fan out events across multiple topics to concurrent subscriber processes, completely decoupling publishers from subscribers:

```text
Publisher ---> {:publish, :orders, event} ---> [ Broker ]
                                                   |
                     +-----------------------------+-----------------------------+
                     |                             |                             |
             {:broadcast, ...}             {:broadcast, ...}             {:broadcast, ...}
                     v                             v                             v
            [ Inventory Worker ]          [ Email Notifier ]            [ Analytics Logger ]
```

Broadcasting with `send/2` in Elixir feels like magic: you can dispatch tens of thousands of messages in a fraction of a millisecond.

However, that non-blocking speed conceals one of the most dangerous failure modes in distributed systems:

**What happens if the publisher emits 10,000 messages per second, but the email notifier can only send 50 emails per second?**

In this part, we will explore **Backpressure** from first principles using raw process primitives:

```elixir
spawn/1
send/2
receive/1
Process.info/2
```

No `GenStage`.

No `Broadway`.

No `Flow`.

We will watch a BEAM process mailbox explode in real time, examine why unbounded message queues lead to catastrophic Out-Of-Memory (OOM) crashes, and implement two fundamental flow control solutions: **Stop-and-Wait** and credit-based **Demand-Driven Windowing** (the exact mechanism powering `GenStage`).

---

## The Hidden Trap: Unbounded Process Mailboxes

In Elixir and Erlang, every process has its own private heap and its own private mailbox.

By default, BEAM process mailboxes are **unbounded FIFO queues**.

When Process A sends a message to Process B:

```elixir
send(target_pid, {:work, data})
```

Two critical things happen:
1. The message payload is copied to Process B's heap (unless it is a refc binary $> 64$ bytes).
2. The message is placed at the tail of Process B's mailbox queue.
3. `send/2` immediately returns `{:work, data}` without waiting for Process B to acknowledge, inspect, or process the message.

Because `send/2` never blocks, an eager producer receives zero feedback about whether the consumer is keeping up!

```text
Fast Producer (10,000 msgs/sec) =====> [ Mailbox: 10k... 50k... 100k ] =====> Slow Consumer (100 msgs/sec)
                                            ^
                                            |
                                  Unbounded Memory Growth
                                  GC overhead degrades node
                                  Risk of VM OOM Crash
```

If the producer outpaces the consumer over a sustained period:
1. **Mailbox Explosion**: Unhandled messages pile up in the consumer's message queue.
2. **Heap Expansion**: The BEAM runtime continuously allocates memory to store the growing queue.
3. **Garbage Collection Thrashing**: The BEAM's generational GC must repeatedly inspect the growing queue during garbage collection passes, consuming massive amounts of CPU.
4. **OOM Killer**: Eventually, the host machine runs out of physical RAM. The Linux kernel's Out-Of-Memory (OOM) killer wakes up and terminates the entire BEAM OS process (`beam.smp`).

To build resilient distributed systems, we need **flow control**—a mechanism that allows downstream consumers to apply **backpressure** on upstream producers.

---

## Experiment 1: Watching a Mailbox Explode in Real Time

Let's build a minimal script that demonstrates this exact failure mode.

We'll create:
1. A **Slow Consumer** that simulates real-world processing latency (e.g. database writes or external HTTP calls) with `Process.sleep(15)`.
2. A **Fast Producer** that blasts 100 items into the consumer's mailbox in a tight loop.
3. A telemetry check using `Process.info(pid, :message_queue_len)` and `Process.info(pid, :memory)` to observe the queue size.

Create `01-unbounded-mailbox.exs`:

```elixir
defmodule SlowConsumer do
  def start(parent) do
    spawn(fn ->
      loop(parent, 0)
    end)
  end

  defp loop(parent, processed_count) do
    receive do
      {:item, _item_id} ->
        # Simulate slow processing (e.g., database query or external API call)
        Process.sleep(15)

        updated_count = processed_count + 1

        # Periodic logging every 20 items
        if rem(updated_count, 20) == 0 do
          {:message_queue_len, queue_len} = Process.info(self(), :message_queue_len)
          {:memory, memory_bytes} = Process.info(self(), :memory)

          IO.puts(
            "Consumer: processed #{updated_count} items | " <>
              "Mailbox queue length: #{queue_len} | Memory: #{memory_bytes} bytes"
          )
        end

        loop(parent, updated_count)

      :stop ->
        IO.puts("Consumer: stopped")
    end
  end
end

defmodule FastProducer do
  def produce(consumer_pid, count) do
    IO.puts("Producer: Blasting #{count} items into consumer mailbox without waiting...")

    Enum.each(1..count, fn i ->
      send(consumer_pid, {:item, i})
    end)

    IO.puts("Producer: Finished sending all #{count} items!")
  end
end

# ============================================================
# RUN THE EXPERIMENT
# ============================================================

IO.puts("=== EXPERIMENT 1: UNBOUNDED MAILBOX GROWTH ===")
consumer = SlowConsumer.start(self())

total_items = 100
FastProducer.produce(consumer, total_items)

# Process.info/2 inspects the target process externally from the runtime
# WITHOUT placing an inspection message in the target's mailbox!
{:message_queue_len, queue_len} = Process.info(consumer, :message_queue_len)
{:memory, memory} = Process.info(consumer, :memory)

IO.puts("\n--- Immediate Snapshot After Producer Finished ---")
IO.puts("Items queued in mailbox : #{queue_len}")
IO.puts("Process memory usage    : #{memory} bytes")
IO.puts("--------------------------------------------------\n")

# Allow consumer to process remaining backlog
IO.puts("Waiting for consumer to drain backlog...")
Process.sleep(1700)

{:message_queue_len, final_queue_len} = Process.info(consumer, :message_queue_len)
{:memory, final_memory} = Process.info(consumer, :memory)

IO.puts("\n--- Final Snapshot After Backlog Drained ---")
IO.puts("Items queued in mailbox : #{final_queue_len}")
IO.puts("Process memory usage    : #{final_memory} bytes")
IO.puts("---------------------------------------------\n")

send(consumer, :stop)
Process.sleep(50)
IO.puts("Experiment 1 complete.")
```

### Running Experiment 1

Execute the script:

```bash
elixir 01-unbounded-mailbox.exs
```

Output:

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

### What This Tells Us

Notice the immediate snapshot:
```text
Items queued in mailbox : 99
```

The producer finished in less than 1 millisecond. But because each item takes 15ms to process, 99 items sat unhandled in the mailbox. If the producer had produced 1,000,000 items instead of 100, the consumer's memory footprint would balloon into gigabytes.

---

## Experiment 2: The Lockstep Solution (Stop-and-Wait)

How can we prevent the producer from getting ahead of the consumer?

The simplest solution is **Stop-and-Wait flow control** (credit of 1):
1. The producer sends an item tagged with a unique correlation reference (`make_ref()`).
2. The producer blocks in `receive` waiting for an acknowledgment (`{:ack, ^ref}`).
3. The consumer processes the item, then sends `{:ack, ref}` back to the producer.

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

Create `02-ack-backpressure.exs`:

```elixir
defmodule AckConsumer do
  def start do
    spawn(fn ->
      loop(0)
    end)
  end

  defp loop(processed_count) do
    receive do
      {:item, reply_to, ref, item_id} ->
        # Inspect mailbox before processing: at most 0 other messages waiting!
        {:message_queue_len, queue_len} = Process.info(self(), :message_queue_len)

        # Simulate work
        Process.sleep(15)

        updated_count = processed_count + 1

        if rem(updated_count, 10) == 0 do
          IO.puts(
            "Consumer: processed item #{item_id} (total: #{updated_count}) | " <>
              "Mailbox queue length: #{queue_len}"
          )
        end

        # Send acknowledgment back to producer
        send(reply_to, {:ack, ref})

        loop(updated_count)

      :stop ->
        IO.puts("Consumer: stopped")
    end
  end
end

defmodule AckProducer do
  def produce(consumer_pid, count) do
    IO.puts("Producer: Sending #{count} items with stop-and-wait acknowledgment...")

    Enum.each(1..count, fn i ->
      ref = make_ref()
      send(consumer_pid, {:item, self(), ref, i})

      # Block until consumer acknowledges receipt of this item
      receive do
        {:ack, ^ref} ->
          :ok
      after
        5000 ->
          IO.puts("Producer: ERROR - consumer timed out!")
      end
    end)

    IO.puts("Producer: All #{count} items acknowledged by consumer!")
  end
end

# ============================================================
# RUN THE EXPERIMENT
# ============================================================

IO.puts("=== EXPERIMENT 2: ACK-BASED (STOP-AND-WAIT) FLOW CONTROL ===")
consumer = AckConsumer.start()

total_items = 50
AckProducer.produce(consumer, total_items)

{:message_queue_len, final_queue_len} = Process.info(consumer, :message_queue_len)
IO.puts("\nFinal consumer mailbox queue length: #{final_queue_len}")

send(consumer, :stop)
IO.puts("Experiment 2 complete.")
```

### Running Experiment 2

```bash
elixir 02-ack-backpressure.exs
```

Output:

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

### The Trade-off of Stop-and-Wait

Notice that throughout the entire run, the consumer's mailbox queue length was **strictly 0**. The consumer was never overwhelmed.

However, lockstep coordination carries a heavy cost:
* The producer is forced to sit completely idle while the consumer works.
* If message transit time over a network is 20ms, every single message suffers a 40ms round-trip delay.
* Concurrency is effectively reduced to 1.

Can we achieve bounded memory **without** destroying pipeline throughput?

---

## Experiment 3: Demand-Driven Flow Control (The GenStage Model)

The gold standard for high-throughput backpressure is the **pull-based windowed demand model** (the core design behind `GenStage` and Reactive Streams).

Instead of the producer pushing items, **the consumer pulls work by asking for demand**:

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

### How Demand Flow Control Works

1. **Consumer Window**: The consumer decides how many messages it is capable of buffering concurrently (e.g., `window_size = 5`).
2. **Demand Request**: The consumer sends an initial demand signal: `send(producer, {:ask, 5})`.
3. **Producer Budget**: The producer maintains an integer counter `demand`. When it receives `{:ask, n}`, it increments `demand` by `n`.
4. **Batch Dispatch**: The producer dispatches up to `min(demand, buffer_size)` items in a single batch and decrements its demand counter.
5. **Demand Replenishment**: When the consumer finishes processing a batch, it requests more demand equal to the items it just completed: `send(producer, {:ask, length(batch)})`.

This guarantees two crucial properties:
* The consumer's mailbox can **never exceed the window size**.
* Items flow in pipelined batches, eliminating round-trip latency overhead.

Create `03-demand-driven-pipeline.exs`:

```elixir
defmodule DemandProducer do
  def start(items) do
    spawn(fn ->
      loop(%{buffer: items, demand: 0, consumer: nil})
    end)
  end

  defp loop(state) do
    receive do
      # Register the consumer
      {:subscribe, consumer_pid} ->
        IO.puts("Producer: Consumer #{inspect(consumer_pid)} connected")
        loop(%{state | consumer: consumer_pid})

      # Consumer requests 'count' more items (demand signal)
      {:ask, count} ->
        new_demand = state.demand + count

        IO.puts(
          "Producer: Received demand for #{count} item(s) " <>
            "(total accumulated demand: #{new_demand})"
        )

        state = %{state | demand: new_demand}
        state = dispatch(state)
        loop(state)

      :stop ->
        IO.puts("Producer: stopped")
    end
  end

  defp dispatch(%{consumer: nil} = state), do: state

  defp dispatch(%{demand: demand, buffer: buffer, consumer: consumer} = state)
       when demand > 0 and buffer != [] do
    # Take at most 'demand' items from the buffer
    batch_size = min(demand, length(buffer))
    {batch, remaining_buffer} = Enum.split(buffer, batch_size)
    remaining_demand = demand - batch_size

    IO.puts(
      "Producer: Dispatching batch of #{length(batch)} item(s) to consumer " <>
        "(remaining buffer: #{length(remaining_buffer)}, remaining demand: #{remaining_demand})"
    )

    send(consumer, {:data, batch})

    updated_state = %{state | buffer: remaining_buffer, demand: remaining_demand}

    # If buffer is now empty, notify consumer
    if remaining_buffer == [] do
      IO.puts("Producer: Buffer empty, notifying consumer that stream is done")
      send(consumer, :stream_done)
    end

    updated_state
  end

  defp dispatch(state), do: state
end

defmodule DemandConsumer do
  def start(parent, producer_pid, window_size) do
    spawn(fn ->
      IO.puts("Consumer: Started with window size #{window_size}")

      # Step 1: Connect to producer
      send(producer_pid, {:subscribe, self()})

      # Step 2: Request initial demand window
      IO.puts("Consumer: Requesting initial demand of #{window_size} item(s)...")
      send(producer_pid, {:ask, window_size})

      loop(parent, producer_pid, window_size, 0)
    end)
  end

  defp loop(parent, producer_pid, window_size, processed_count) do
    receive do
      {:data, batch} ->
        {:message_queue_len, queue_len} = Process.info(self(), :message_queue_len)

        IO.puts(
          "\nConsumer: >>> Received batch of #{length(batch)} items " <>
            "[Mailbox queue length: #{queue_len}]"
        )

        # Process each item in the batch
        Enum.each(batch, fn item ->
          # Simulate processing latency
          Process.sleep(15)
          IO.puts("Consumer: processed item #{item}")
        end)

        updated_count = processed_count + length(batch)

        # Replenish demand: ask for the number of processed items
        IO.puts("Consumer: Batch done. Replenishing demand (+#{length(batch)})...")
        send(producer_pid, {:ask, length(batch)})

        loop(parent, producer_pid, window_size, updated_count)

      :stream_done ->
        IO.puts("\nConsumer: Received stream_done signal. Total processed: #{processed_count}")
        send(parent, {:consumer_finished, processed_count})
        loop(parent, producer_pid, window_size, processed_count)

      :stop ->
        IO.puts("Consumer: stopped")
    end
  end
end

# ============================================================
# RUN THE EXPERIMENT
# ============================================================

IO.puts("=== EXPERIMENT 3: DEMAND-DRIVEN FLOW CONTROL PIPELINE ===")

total_items = 20
window_size = 5

items = Enum.to_list(1..total_items)
producer = DemandProducer.start(items)
consumer = DemandConsumer.start(self(), producer, window_size)

receive do
  {:consumer_finished, count} ->
    IO.puts("\nPipeline successfully completed! Total items processed: #{count}")
after
  10_000 ->
    IO.puts("Pipeline timed out!")
end

# Verify final queue length
{:message_queue_len, final_queue_len} = Process.info(consumer, :message_queue_len)
IO.puts("Final consumer mailbox queue length: #{final_queue_len}")

send(consumer, :stop)
send(producer, :stop)
Process.sleep(50)
IO.puts("Experiment 3 complete.")
```

### Running Experiment 3

```bash
elixir 03-demand-driven-pipeline.exs
```

Output:

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

## Push vs. Pull: When to Use Which?

| Attribute | Uncoordinated Push | Stop-and-Wait | Demand-Driven Windowing |
| :--- | :--- | :--- | :--- |
| **Control Driver** | Producer | Alternating | Consumer |
| **Mailbox Risk** | High / Unbounded | Zero ($\le 1$) | Bounded by window size |
| **Throughput** | High (until crash) | Low (latency bound) | High (batch pipelining) |
| **Ideal For** | Fire-and-forget events | Low-volume sync RPC | High-throughput data streams |

---

## What If Upstream Cannot Be Throttled?

In pure process pipelines, the consumer can tell the producer to halt production.

However, in many real-world architectures, input arrives from an **external source** that has no concept of backpressure:
* A high-volume UDP socket or sensor feed.
* Webhook bursts hitting an HTTP API.

When the input rate exceeds physical system capacity, memory cannot grow indefinitely. A system must enforce an explicit **bounded buffer policy**:

1. **Drop Newest**: Discard newly incoming events when the internal queue is full.
2. **Drop Oldest**: Discard the oldest events in the queue so the system always processes freshest data.
3. **Reject at the Boundary**: Return HTTP 429 (Too Many Requests) or close TCP connections to push back against client traffic.

---

## Production Elixir & Erlang Equivalents

The demand-driven pattern we built from scratch is the exact foundation of Elixir's official data processing ecosystem:

1. **`GenStage`**: Formalizes the demand protocol. A `Consumer` declares `min_demand` and `max_demand`, and the `Producer` implements `handle_demand(demand, state)` to emit events.
2. **`Broadway`**: Built on `GenStage` for industrial pipelines (Amazon SQS, RabbitMQ, Kafka) with automatic rate-limiting, batching, and graceful backpressure.
3. **Erlang Sockets (`active: :once` / `active: N`)**: The Erlang VM allows TCP sockets to be configured in `{active, N}` mode. The socket sends $N$ packets as messages to the controlling process, then pauses reading from the OS socket buffer until the process requests more data via `inet:setopts(sock, [{active, N}])`.

---

## Key Technical Takeaways

1. **The BEAM Mailbox is Unbounded**: Asynchronous `send/2` returns immediately, hiding downstream bottlenecks until process memory runs out.
2. **Monitor with `Process.info/2`**: `Process.info(pid, :message_queue_len)` allows external monitoring of process health without placing inspection messages in the process mailbox.
3. **Stop-and-Wait Protects Memory, but Penalizes Latency**: Ping-pong acknowledgment bounds queue length to $\le 1$, but reduces throughput to single-item serialization.
4. **Demand-Driven Flow Control Maximizes Throughput Safely**: Credit windows let the consumer pull batches of work at its own pace, achieving optimal concurrency and zero mailbox backlog.

---

## Series Navigation

1. [Building a Stateful Process in Elixir Without GenServer](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)
2. [Correlated Request/Reply](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)
3. [Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)
4. [Process Linking](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-4-process-linking-okb)
5. [Supervisor From Scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh)
6. [Named Processes](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-6-named-processes-3k8a)
7. [Worker Pool](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-7-worker-pool-f23)
8. [Publish / Subscribe](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-8-publish-subscribe-cl3)
9. **Backpressure**
10. Connecting Nodes *(coming soon)*

---

← Previous: [Part 8 — Publish / Subscribe](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-8-publish-subscribe-cl3)

Next: Part 10 — Connecting Nodes *(coming soon)* →

---

Source code repository: [github.com/pckrishnadas88/elixir-distributed-systems-lab](https://github.com/pckrishnadas88/elixir-distributed-systems-lab/tree/main/09-backpressure)

---

## Content License

This article is licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/). You may share and adapt it with attribution for non-commercial purposes. The accompanying source code remains licensed under the MIT License.

