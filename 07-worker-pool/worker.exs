defmodule Worker do
  # ============================================================
  # Worker Process
  #
  # A worker:
  #
  # 1. Announces that it is available.
  # 2. Waits for a job.
  # 3. Processes the job.
  # 4. Reports the result.
  # 5. Announces that it is available again.
  # 6. Waits for another job.
  # ============================================================

  def start(worker_id, coordinator) do
    spawn(fn ->
      start_working(worker_id, coordinator)
    end)
  end

  # ============================================================
  # STEP 1
  #
  # This runs only ONCE when the worker starts.
  # ============================================================

  defp start_working(worker_id, coordinator) do
    IO.puts(
      "Worker #{worker_id} started"
    )

    # Tell the coordinator:
    #
    # "I am available."
    send(
      coordinator,
      {:available, worker_id, self()}
    )

    # Now wait for the first job.
    wait_for_work(
      worker_id,
      coordinator
    )
  end

  # ============================================================
  # STEP 2
  #
  # The worker waits for a message from the coordinator.
  #
  # IMPORTANT:
  #
  # This function does NOT send :available when it starts.
  #
  # The worker only sends :available:
  #
  # - once when it starts
  # - once after it finishes a job
  # ============================================================

  defp wait_for_work(worker_id, coordinator) do
    receive do

      # ========================================================
      # STEP 3
      #
      # Coordinator sends:
      #
      #     {:work, job}
      #
      # The worker processes the job.
      # ========================================================

      {:work, job} ->
        IO.puts(
          "Worker #{worker_id} processing job #{job}"
        )

        # Simulate work.
        Process.sleep(:rand.uniform(1000))

        # Do the actual work.
        result = job * job

        IO.puts(
          "Worker #{worker_id} finished job #{job} -> #{result}"
        )

        # ======================================================
        # STEP 4
        #
        # Tell the coordinator:
        #
        # "I finished this job and I am available again."
        #
        # ONE message represents ONE availability event.
        # ======================================================

        send(
          coordinator,
          {:available, worker_id, self(), job, result}
        )

        # ======================================================
        # STEP 5
        #
        # Wait for the next job.
        #
        # We do NOT send :available again here.
        # ======================================================

        wait_for_work(
          worker_id,
          coordinator
        )

      # ========================================================
      # STEP 6
      #
      # The coordinator tells the worker to stop.
      # ========================================================

      :stop ->
        IO.puts(
          "Worker #{worker_id} stopped"
        )
    end
  end
end
