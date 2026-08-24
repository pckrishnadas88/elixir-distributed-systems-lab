# Building Distributed Systems in Elixir: Part 7 — Worker Pool

In the previous part of this series, we explored named processes.

We saw how registering a process under an atom like `:worker` provided a stable public handle, separating a service's logical address from the temporary PID of its current process incarnation.

However, a single registered local process name points to exactly one running process:

```text
:worker  ->  #PID<0.123.0>
```

What happens when a system needs to process many jobs concurrently, but cannot afford to run an unbounded number of processes or send all requests to a single worker?

In this part, we'll build a **Worker Pool** from scratch using raw process primitives:

```elixir
spawn/1
send/2
receive/1
```

Our pool will manage a fixed set of workers and a queue of waiting jobs. When a worker completes its task, it announces its availability to pull the next job from the queue.

No `GenServer`.

No `Task` or `Task.async_stream`.

No `Poolboy` or OTP `Supervisor`.

The goal is to understand the mechanics of bounded concurrency, backpressure, and load coordination before relying on library abstractions.

---

## The Problem: Unbounded Concurrency vs. Resource Limits

BEAM processes are extremely lightweight. It is common in Elixir to spawn a process for every incoming request:

```elixir
Enum.each(jobs, fn job ->
  spawn(fn -> process_job(job) end)
Enum.each)
```

If you have 10 jobs, spawning 10 processes works seamlessly.

If you have 100,000 jobs—and each job performs a database query or external API request—spawning 100,000 concurrent processes creates critical problems:

1. **Resource Exhaustion**: Excessive socket connections, file descriptors, or database connections will overwhelm downstream services.
2. **CPU Thrashing**: Context switching across tens of thousands of active processes degrades system throughput.
3. **Lack of Backpressure**: Uncontrolled work acceptance leads to memory spikes and eventual failure under bursty load.

To protect system stability, we need **bounded concurrency**.

A **worker pool** constrains the maximum number of active workers running concurrently (e.g., 3 workers), while buffering remaining tasks in a queue (e.g., 5 jobs).

---

## Pool Architecture & Design

Our worker pool system consists of three main components:

1. **Parent / Client Process**: Initiates the pool and waits for overall completion.
2. **Coordinator Process**: Manages pool state, tracks idle workers, buffers pending jobs, and distributes tasks.
3. **Worker Processes**: Receive tasks from the coordinator, process them, and report results back.

```text
                  +--------------------------------+
                  |         Parent Process         |
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

---

## Step 1: Designing the Worker Process

Each worker is responsible for:
1. Announcing that it is started and available.
2. Waiting for a job from the coordinator.
3. Processing the assigned job.
4. Reporting the result along with its renewed availability.
5. Shutting down gracefully when sent a `:stop` signal.

Here is the complete implementation in `worker.exs`:

```elixir
defmodule Worker do
  def start(worker_id, coordinator) do
    spawn(fn ->
      start_working(worker_id, coordinator)
    end)
  end

  defp start_working(worker_id, coordinator) do
    IO.puts("Worker #{worker_id} started")

    # Announce initial availability to coordinator
    send(coordinator, {:available, worker_id, self()})

    # Wait for work
    wait_for_work(worker_id, coordinator)
  end

  defp wait_for_work(worker_id, coordinator) do
    receive do
      {:work, job} ->
        IO.puts("Worker #{worker_id} processing job #{job}")

        # Simulate variable work duration
        Process.sleep(:rand.uniform(1000))

        result = job * job

        IO.puts("Worker #{worker_id} finished job #{job} -> #{result}")

        # Report result AND announce availability for the next job
        send(coordinator, {:available, worker_id, self(), job, result})

        # Loop to wait for next job
        wait_for_work(worker_id, coordinator)

      :stop ->
        IO.puts("Worker #{worker_id} stopped")
    end
  end
end
```

### Notice the Availability Pattern

Notice how availability is communicated:
- **On startup**: Worker sends `{:available, worker_id, self()}`.
- **After job completion**: Worker sends `{:available, worker_id, self(), job, result}`.

The worker **never** sends an availability message when entering `wait_for_work/2` directly. Combining completion reporting and re-availability into one message ensures that one message represents exactly one availability event.

---

## Step 2: Designing the Coordinator Process

The coordinator maintains the central pool state in its recursive loop parameters:

* `workers`: List of all spawned worker PIDs (used for shutdown).
* `available_workers`: Queue of `[{worker_id, worker_pid}]` ready for work.
* `waiting_jobs`: List of pending jobs waiting to be dispatched.
* `completed_jobs`: Counter tracking completed jobs.
* `total_jobs`: Total job count to determine when the pool finish condition is met.

Here is the implementation in `coordinator.exs`:

```elixir
defmodule Coordinator do
  def start(parent, worker_count, jobs) do
    spawn(fn ->
      run(parent, worker_count, jobs)
    end)
  end

  defp run(parent, worker_count, jobs) do
    workers =
      Enum.map(1..worker_count, fn worker_id ->
        Worker.start(worker_id, self())
      end)

    coordinator_loop(
      parent,
      workers,
      [],
      jobs,
      0,
      length(jobs)
    )
  end

  defp coordinator_loop(
         parent,
         workers,
         available_workers,
         waiting_jobs,
         completed_jobs,
         total_jobs
       ) do

    # Task Distribution Step
    {available_workers, waiting_jobs} =
      distribute_jobs(available_workers, waiting_jobs)

    # Check if all work is completed
    if completed_jobs == total_jobs do
      stop_workers(workers)
      send(parent, :pool_finished)
      IO.puts("Worker pool stopped")
    else
      receive do
        {:available, worker_id, worker_pid} ->
          IO.puts("Coordinator: Worker #{worker_id} is available")

          coordinator_loop(
            parent,
            workers,
            available_workers ++ [{worker_id, worker_pid}],
            waiting_jobs,
            completed_jobs,
            total_jobs
          )

        {:available, worker_id, worker_pid, job, result} ->
          IO.puts("Coordinator: Worker #{worker_id} completed #{job} -> #{result}")
          IO.puts("Coordinator: Worker #{worker_id} is available")

          coordinator_loop(
            parent,
            workers,
            available_workers ++ [{worker_id, worker_pid}],
            waiting_jobs,
            completed_jobs + 1,
            total_jobs
          )
      end
    end
  end

  # Distribution matching
  defp distribute_jobs([], waiting_jobs), do: {[], waiting_jobs}
  defp distribute_jobs(available_workers, []), do: {available_workers, []}

  defp distribute_jobs(
         [{worker_id, worker_pid} | remaining_workers],
         [job | remaining_jobs]
       ) do
    IO.puts("Coordinator: sending job #{job} to Worker #{worker_id}")

    send(worker_pid, {:work, job})

    distribute_jobs(remaining_workers, remaining_jobs)
  end

  defp stop_workers(workers) do
    Enum.each(workers, fn worker_pid ->
      send(worker_pid, :stop)
    end)
  end
end
```

---

## Step 3: Demand-Driven Task Distribution

The key logic lies inside `distribute_jobs/2`:

```elixir
defp distribute_jobs(
       [{worker_id, worker_pid} | remaining_workers],
       [job | remaining_jobs]
     ) do
  IO.puts("Coordinator: sending job #{job} to Worker #{worker_id}")
  send(worker_pid, {:work, job})
  distribute_jobs(remaining_workers, remaining_jobs)
end
```

### Why Pull-Based / Demand-Driven?

Rather than pushing jobs round-robin or randomly to workers (which might still be busy processing prior tasks), jobs are allocated **only** when a worker explicitly signals that it is free (`available_workers`).

```text
Worker becomes free ---> sends :available ---> Coordinator pairs Worker with next job
```

This naturally implements **backpressure**:
* Fast workers process more jobs.
* Slow workers process fewer jobs.
* Concurrency never exceeds the available worker count.

---

## Step 4: Putting It All Together

The entry point in `worker_pool.exs` ties everything together:

```elixir
Code.require_file("worker.exs", __DIR__)
Code.require_file("coordinator.exs", __DIR__)

defmodule WorkerPool do
  def start(worker_count, jobs) do
    parent = self()

    Coordinator.start(
      parent,
      worker_count,
      jobs
    )

    receive do
      :pool_finished ->
        IO.puts("All jobs completed")
    end
  end
end

# Run the pool with 3 workers and 5 jobs
jobs = [10, 20, 30, 40, 50]
WorkerPool.start(3, jobs)
```

---

## Execution & Output Trace

Run the pool with:

```bash
elixir worker_pool.exs
```

Example execution trace:

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

### Trace Analysis

1. **Initialization**: 3 workers start up concurrently and announce availability.
2. **Initial Batch**: Jobs `10`, `20`, and `30` are immediately assigned to Workers `1`, `2`, and `3`.
3. **Dynamic Queue Processing**: Worker 1 finishes job `10` first, becomes available, and immediately gets job `40`. Upon finishing `40`, it pulls job `50`.
4. **Completion & Shutdown**: Once all 5 jobs report results (`completed_jobs == 5`), the coordinator stops all workers and signals `:pool_finished` to the parent.

---

## Key Technical Takeaways

### 1. Atomic Completion and Availability
Combining completion notification and availability into `{:available, worker_id, worker_pid, job, result}` prevents race conditions. The coordinator updates metrics and enqueues the idle worker in a single state transition.

### 2. State-Driven Concurrency Regulation
State in recursive loops (`available_workers`, `waiting_jobs`) acts as a natural semaphore. Concurrency is strictly bounded by `length(workers)`.

### 3. Graceful Lifecycle Teardown
Broadcasting `:stop` to workers ensures clean termination without leaving dangling processes on the BEAM node.

---

## Limitations of Raw Worker Pools

While this implementation clearly illustrates work distribution mechanics, production applications require additional guarantees:

1. **No Failure Handling / Supervision**: If a worker process crashes during execution, its job is lost, and the coordinator will block indefinitely waiting for `completed_jobs == total_jobs`.
2. **In-Memory Buffering**: Queued jobs live only in process memory. If the node or coordinator crashes, unprocessed work is lost.
3. **Static Sizing**: The pool cannot dynamically scale up or down based on load metrics.

In real-world applications, OTP libraries such as `NimblePool`, `Poolboy`, or `Broadway` combine these pooling principles with supervision trees, telemetry, and dynamic scaling.

---

Source code link: https://github.com/pckrishnadas88/elixir-distributed-systems-lab/tree/main/07-worker-pool

---

## Series

1. [Building a Stateful Process in Elixir Without GenServer](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)
2. [Correlated Request/Reply](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)
3. [Process Monitoring](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)
4. [Process Linking](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-4-process-linking-okb)
5. [Supervisor From Scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh)
6. [Named Processes](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-6-named-processes-3k8a)
7. **Worker Pool**

---

← Previous: [Part 6 — Named Processes](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-6-named-processes-3k8a)

Next: Part 8 — Publish / Subscribe *(coming soon)* →

---

## Content License

This article is licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/). You may share and adapt it with attribution for non-commercial purposes. The accompanying source code remains licensed under the MIT License.
