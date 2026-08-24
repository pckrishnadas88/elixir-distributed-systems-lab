# ============================================================
# Load the other modules.
# ============================================================

Code.require_file("worker.exs", __DIR__)
Code.require_file("coordinator.exs", __DIR__)


defmodule WorkerPool do
  # ============================================================
  # Start the worker pool.
  # ============================================================

  def start(worker_count, jobs) do
    parent = self()

    # Start the coordinator.
    Coordinator.start(
      parent,
      worker_count,
      jobs
    )

    # Wait until the coordinator tells us
    # that all jobs have completed.
    receive do
      :pool_finished ->
        IO.puts("All jobs completed")
    end
  end
end


# ============================================================
# RUN THE EXAMPLE
#
# 3 workers
# 5 jobs
# ============================================================

jobs = [10, 20, 30, 40, 50]

WorkerPool.start(
  3,
  jobs
)
