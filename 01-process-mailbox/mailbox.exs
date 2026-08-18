defmodule Counter do
  # Start an isolated counter process; its state lives only in the recursive loop.
  def start(initial_value \\ 0) do
    spawn(fn ->
      loop(initial_value)
    end)
  end

  def increment(counter_pid) do
    # Include our PID so the counter knows where to send the asynchronous reply.
    send(counter_pid, {:increment, self()})

    receive do
      {:counter_value, value} ->
        {:ok, value}
    after
      # A missing/dead counter must not leave the caller waiting indefinitely.
      1_000 ->
        {:error, :timeout}
    end
  end

  def get(counter_pid) do
    # Reading state uses the same request/reply mailbox protocol as incrementing.
    send(counter_pid, {:get, self()})

    receive do
      {:counter_value, value} ->
        {:ok, value}
    after
      1_000 ->
        {:error, :timeout}
    end
  end

  def stop(counter_pid) do
    # This is asynchronous: the caller does not wait for termination confirmation.
    send(counter_pid, :stop)
  end

  defp loop(value) do
    # `receive` removes one matching message at a time and carries state forward.
    receive do
      {:increment, from_pid} ->
        new_value = value + 1

        send(from_pid, {:counter_value, new_value})
        loop(new_value)

      {:get, from_pid} ->
        send(from_pid, {:counter_value, value})
        loop(value)

      :stop ->
        # Returning instead of recurring terminates this process normally.
        IO.puts("Counter stopped")

      unknwon_message ->
        # Keep the process alive when an unexpected protocol message arrives.
        IO.inspect(unknwon_message, label: "Unknwon message")
        loop(value)
    end
  end
end

# The main process acts as a client and owns the replies sent by the counter.
counter = Counter.start()

IO.inspect(counter, label: "Counter PID")
IO.inspect(Counter.get(counter), label: "Initial value")
IO.inspect(Counter.increment(counter), label: "After increment")
IO.inspect(Counter.increment(counter), label: "After second increment")
IO.inspect(Counter.get(counter), label: "Current value")

Counter.stop(counter)
