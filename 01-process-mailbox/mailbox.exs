defmodule Counter do
  def start(initial_value \\ 0) do
    spawn(fn ->
      loop(initial_value)
    end)
  end

  def increment(counter_pid) do
    send(counter_pid, {:increment, self()})

    receive do
      {:counter_value, value} ->
        {:ok, value}
    after
      1_000 ->
        {:error, :timeout}
    end
  end

  def get(counter_pid) do
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
    send(counter_pid, :stop)
  end

  defp loop(value) do
    receive do
      {:increment, from_pid} ->
        new_value = value + 1

        send(from_pid, {:counter_value, new_value})
        loop(new_value)

      {:get, from_pid} ->
        send(from_pid, {:counter_value, value})
        loop(value)

      :stop ->
        IO.puts("Counter stopped")

      unknwon_message ->
        IO.inspect(unknwon_message, label: "Unknwon message")
        loop(value)
    end
  end
end

counter = Counter.start()

IO.inspect(counter, label: "Counter PID")
IO.inspect(Counter.get(counter), label: "Initial value")
IO.inspect(Counter.increment(counter), label: "After increment")
IO.inspect(Counter.increment(counter), label: "After second increment")
IO.inspect(Counter.get(counter), label: "Current value")

Counter.stop(counter)
