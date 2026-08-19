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

# Register the process with a name
Process.register(pid, :worker)

IO.puts("Registered as :worker")

# Look up the process using its registered name
registered_pid = Process.whereis(:worker)

IO.puts("Lookup using name: #{inspect(registered_pid)}")

# Stop the worker using its registered name
IO.puts("Stopping using name: #{inspect(registered_pid)}")

send(:worker, :stop)
