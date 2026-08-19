defmodule Worker do
  def start do
    spawn(fn ->
      receive do
        :stop ->
          :ok
      end
    end)
  end
end

# Start the worker process
pid = Worker.start()

IO.puts("Worker PID: #{inspect(pid)}")

# Register the process globally
:global.register_name(:worker, pid)

IO.puts("Registered globally as :worker")

# Look up the process using its global name
registered_pid = :global.whereis_name(:worker)

IO.puts("Global lookup: #{inspect(registered_pid)}")

# Stop the worker using its global name
IO.puts("Stopping worker using global name: :worker")

send(:global.whereis_name(:worker), :stop)
