defmodule Worker do
  def start do
    pid =
      spawn(fn ->
        receive do
          {:work, from, request} ->
            send(from, {:reply, request, "processed: #{request}"})
        end
      end)

    Process.register(pid, :worker)

    :worker
  end
end

defmodule Client do
  def request(worker_name, request) do
    send(worker_name, {:work, self(), request})

    receive do
      {:reply, ^request, response} ->
        response
    end
  end
end

# Start the worker.
# The PID is kept inside the worker setup.
worker = Worker.start()

IO.puts("Worker available as: #{inspect(worker)}")

# The client only knows the worker name.
response = Client.request(worker, "hello")

IO.puts("Response: #{response}")
