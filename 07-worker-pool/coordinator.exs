defmodule Coordinator do
  # ============================================================
  # Coordinator
  #
  # Responsible for:
  #
  # - tracking available workers
  # - keeping waiting jobs
  # - distributing jobs
  # - tracking completed jobs
  # - stopping workers
  # ============================================================

  def start(parent, worker_count, jobs) do
    spawn(fn ->
      run(parent, worker_count, jobs)
    end)
  end

  defp run(parent, worker_count, jobs) do
    # ==========================================================
    # STEP 1
    #
    # Create multiple workers.
    #
    # For worker_count = 3:
    #
    # Worker 1
    # Worker 2
    # Worker 3
    # ==========================================================

    workers =
      Enum.map(1..worker_count, fn worker_id ->
        Worker.start(
          worker_id,
          self()
        )
      end)

    # ==========================================================
    # STEP 2
    #
    # Start the coordinator loop.
    #
    # available_workers = []
    # waiting_jobs      = jobs
    # completed_jobs    = 0
    # ==========================================================

    coordinator_loop(
      parent,
      workers,
      [],
      jobs,
      0,
      length(jobs)
    )
  end

  # ============================================================
  # Coordinator loop
  # ============================================================

  defp coordinator_loop(
         parent,
         workers,
         available_workers,
         waiting_jobs,
         completed_jobs,
         total_jobs
       ) do

    # ==========================================================
    # STEP 3
    #
    # Give waiting jobs to available workers.
    #
    # This is TASK DISTRIBUTION.
    # ==========================================================

    {available_workers, waiting_jobs} =
      distribute_jobs(
        available_workers,
        waiting_jobs
      )

    # ==========================================================
    # STEP 4
    #
    # Check whether every job has completed.
    # ==========================================================

    if completed_jobs == total_jobs do
      # All work is finished.
      stop_workers(workers)

      # Tell the parent process:
      #
      # "The entire worker pool has finished."
      send(
        parent,
        :pool_finished
      )

      IO.puts("Worker pool stopped")

    else
      # ========================================================
      # STEP 5
      #
      # Wait for a worker message.
      # ========================================================

      receive do

        # ======================================================
        # A worker has started and says:
        #
        # "I am available."
        # ======================================================

        {:available, worker_id, worker_pid} ->
          IO.puts(
            "Coordinator: Worker #{worker_id} is available"
          )

          available_workers =
            available_workers ++
              [{worker_id, worker_pid}]

          coordinator_loop(
            parent,
            workers,
            available_workers,
            waiting_jobs,
            completed_jobs,
            total_jobs
          )

        # ======================================================
        # A worker has finished a job.
        #
        # The same message also tells us that the worker
        # is available again.
        #
        # This avoids registering the same worker twice.
        # ======================================================

        {:available, worker_id, worker_pid, job, result} ->
          IO.puts(
            "Coordinator: Worker #{worker_id} " <>
              "completed #{job} -> #{result}"
          )

          IO.puts(
            "Coordinator: Worker #{worker_id} is available"
          )

          # The worker has completed one more job.
          completed_jobs =
            completed_jobs + 1

          # The worker is now available again.
          available_workers =
            available_workers ++
              [{worker_id, worker_pid}]

          coordinator_loop(
            parent,
            workers,
            available_workers,
            waiting_jobs,
            completed_jobs,
            total_jobs
          )
      end
    end
  end

  # ============================================================
  # TASK DISTRIBUTION
  #
  # No available workers.
  #
  # Keep the jobs waiting.
  # ============================================================

  defp distribute_jobs([], waiting_jobs) do
    {[], waiting_jobs}
  end

  # ============================================================
  # No waiting jobs.
  #
  # Keep the available workers.
  # ============================================================

  defp distribute_jobs(
         available_workers,
         []
       ) do
    {available_workers, []}
  end

  # ============================================================
  # We have:
  #
  # - an available worker
  # - a waiting job
  #
  # Give the job to that worker.
  #
  # LOAD BALANCING
  #
  # We do not randomly select a worker.
  #
  # A worker must first become available.
  # Whichever worker becomes available gets
  # the next waiting job.
  # ============================================================

  defp distribute_jobs(
         [{worker_id, worker_pid} | remaining_workers],
         [job | remaining_jobs]
       ) do

    IO.puts(
      "Coordinator: sending job #{job} " <>
        "to Worker #{worker_id}"
    )

    # Send the job to the available worker.
    send(
      worker_pid,
      {:work, job}
    )

    # The worker is now busy.
    #
    # Remove it from the available workers list.
    distribute_jobs(
      remaining_workers,
      remaining_jobs
    )
  end

  # ============================================================
  # Stop all workers.
  # ============================================================

  defp stop_workers(workers) do
    Enum.each(
      workers,
      fn worker_pid ->
        send(worker_pid, :stop)
      end
    )
  end
end
