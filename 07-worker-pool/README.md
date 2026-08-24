# 07 - Worker Pool

A minimal worker pool and task distribution implementation using raw Elixir process primitives.

This lesson demonstrates how to coordinate multiple worker processes, track worker availability, distribute tasks dynamically, and manage pool lifecycle without `GenServer`, `Task`, `Poolboy`, or OTP supervision.

---

## Learning Objective

Understand how to:

* coordinate work across a pool of concurrent worker processes
* track worker availability using explicit process messages
* distribute pending tasks dynamically as workers become free
* combine completion reporting and re-availability to prevent race conditions
* implement demand-driven backpressure (workers pull work when idle)
* perform clean lifecycle shutdown across a coordinator and its workers

---

## Problem

Spawning an unbounded number of processes for concurrent tasks is convenient on the BEAM, but has trade-offs when dealing with heavy or resource-constrained operations:

```text
10,000 requests ---> spawn 10,000 processes ---> database overload / memory exhaustion
```

A **worker pool** constrains concurrency by creating a fixed set of workers (e.g., 3 workers) and managing a queue of pending jobs (e.g., 5 jobs).

```text
5 Pending Jobs ---> [ Coordinator ] ---> Work distributed to 3 Workers as they become free
```

This prevents overwhelming external resources while maximizing throughput.

---

## Architecture

```text
                  +--------------------------------+
                  |          WorkerPool            |
                  +--------------------------------+
                                  |
                                  | starts & waits for :pool_finished
                                  v
                  +--------------------------------+
                  |          Coordinator           |
                  |--------------------------------|
                  | workers          = [P1,P2,P3]  |
                  | available_workers= [(1,P1)...] |
                  | waiting_jobs     = [10,20...]  |
                  | completed_jobs   = 0           |
                  +--------------------------------+
                       /          |          \
           {:work, job}          |           {:work, job}
                     v            v            v
                +---------+  +---------+  +---------+
                | Worker 1|  | Worker 2|  | Worker 3|
                +---------+  +---------+  +---------+
```

### Worker Lifecycle & State Flow

```text
Worker 1                                           Coordinator
   |                                                    |
   | {:available, 1, self()}                            |  (initial announce)
   |--------------------------------------------------->|
   |                                                    |
   |                   {:work, 10}                      |
   |<---------------------------------------------------|  (distribute job)
   |                                                    |
   | [ processes job 10 ]                               |
   |                                                    |
   | {:available, 1, self(), 10, 100}                   |  (completed job 10 + available)
   |--------------------------------------------------->|
```

---

## Protocol

### Worker to Coordinator Messages

1. Initial availability upon startup:
   ```elixir
   {:available, worker_id, worker_pid}
   ```

2. Job completion + re-availability:
   ```elixir
   {:available, worker_id, worker_pid, job, result}
   ```

### Coordinator to Worker Messages

1. Assign job:
   ```elixir
   {:work, job}
   ```

2. Stop worker:
   ```elixir
   :stop
   ```

### Coordinator to Parent Messages

1. All jobs completed:
   ```elixir
   :pool_finished
   ```

---

## Project Structure

```text
07-worker-pool/
├── worker.exs
├── coordinator.exs
├── worker_pool.exs
├── README.md
└── blog.md
```

* `worker.exs`: Implements `Worker` module (announces availability, waits for work, processes jobs, sends completion).
* `coordinator.exs`: Implements `Coordinator` module (spawns workers, tracks availability queue, distributes jobs, signals completion).
* `worker_pool.exs`: Script entry point loading modules and executing a 3-worker / 5-job pool example.

---

## Running

Execute the pool script using Elixir CLI:

```bash
elixir worker_pool.exs
```

Example output:

```text
Worker 1 started
Worker 2 started
Worker 3 started
Coordinator: Worker 1 is available
Coordinator: sending job 10 to Worker 1
Coordinator: Worker 2 is available
Worker 1 processing job 10
Coordinator: sending job 20 to Worker 2
Worker 2 processing job 20
Coordinator: Worker 3 is available
Coordinator: sending job 30 to Worker 3
Worker 3 processing job 30
Worker 1 finished job 10 -> 100
Coordinator: Worker 1 completed 10 -> 100
Coordinator: Worker 1 is available
Coordinator: sending job 40 to Worker 1
Worker 1 processing job 40
Worker 1 finished job 40 -> 1600
Coordinator: Worker 1 completed 40 -> 1600
Coordinator: Worker 1 is available
Coordinator: sending job 50 to Worker 1
Worker 1 processing job 50
Worker 3 finished job 30 -> 900
Coordinator: Worker 3 completed 30 -> 900
Coordinator: Worker 3 is available
Worker 2 finished job 20 -> 400
Coordinator: Worker 2 completed 20 -> 400
Coordinator: Worker 2 is available
Worker 1 finished job 50 -> 2500
Coordinator: Worker 1 completed 50 -> 2500
Coordinator: Worker 1 is available
Worker pool stopped
Worker 1 stopped
Worker 2 stopped
Worker 3 stopped
All jobs completed
```

---

## Key Concepts

### Demand-Driven Distribution

Instead of pushing jobs randomly or round-robin to potentially busy workers, jobs are distributed only to workers that have announced availability (`available_workers`). Work is pulled on demand.

### Atomic Completion and Re-Availability

A worker signals completion and re-availability in a single message:

```elixir
{:available, worker_id, self(), job, result}
```

This ensures the coordinator updates job completion metrics and adds the worker back to the available list in one step, preventing duplicate registrations.

### Bounded Concurrency

With 3 workers, at most 3 jobs execute simultaneously regardless of total queue length. Pending jobs (`waiting_jobs`) remain buffered in coordinator state until a worker completes its current task.

### Graceful Pool Shutdown

When `completed_jobs == total_jobs`:
1. Coordinator sends `:stop` to all worker PIDs.
2. Each worker receives `:stop`, prints shutdown notice, and exits.
3. Coordinator sends `:pool_finished` to the parent process and exits.

---

## Limitations

* **No Crash Recovery**: If a worker process crashes, the coordinator does not monitor it and will hang waiting for all jobs to complete.
* **In-Memory Jobs**: Pending jobs are stored in process state; if the coordinator crashes, queued work is lost.
* **Static Pool Size**: Number of workers is fixed at startup and cannot scale dynamically.

---

## Next Step

Project 08 explores dynamic supervision and failure recovery patterns for managed process pools.

---

## References

- Blog post: [Building Distributed Systems in Elixir: Part 7 — Worker Pool](blog.md)
- Previous: [06 - Named Processes](../06-named-processes/README.md)
- Elixir documentation: [`Process.sleep/1`](https://hexdocs.pm/elixir/Process.html#sleep/1)
- Elixir documentation: [`send/2`](https://hexdocs.pm/elixir/Process.html#send/2)
