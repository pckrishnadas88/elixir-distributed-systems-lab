defmodule ProcessLinking do
  def run("propagate") do
    IO.puts("Parent started: #{inspect(self())}")

    # Links are bidirectional: an abnormal worker exit also exits this parent.
    worker = spawn_link(fn ->
      IO.puts("Worker started: #{inspect(self())}")
      Process.sleep(500)

      raise "worker crashed"
    end
      )
    IO.puts("Parent linked to worker: #{inspect(worker)}")
    IO.puts("The worker crash will terminate the parent.")

    Process.sleep(:infinity)
  end

  def run("trap") do
    # Convert linked-process exit signals into ordinary mailbox messages.
    Process.flag(:trap_exit, true)
    IO.puts("Parent started: #{inspect(self())}")
    IO.puts("Parent is trapping exits")

    # The link still exists, but the trapped exit will be handled by `receive` below.
    worker = spawn_link(fn ->
      IO.puts("Worker started: #{inspect(self())}")
      Process.sleep(500)

      raise "Worker crashed"
    end)

    IO.puts("Parent linked to worker: #{inspect(worker)}")

    receive do
      {:EXIT, ^worker, reason} ->
        # Pin the PID so an exit from a different linked process cannot match.
        IO.puts("Parent received an EXIT message.")
        IO.puts("Worker: #{inspect(worker)}")
        IO.puts("Reason: #{inspect(reason)}")
        IO.puts("Parent is still alive.")
    end

  end


  def run(_) do
    IO.puts("""
      Usage:

        elixir process_linking.exs propagate
        elixir process_linking.exs trap
    """)
  end
end

# Select the behavior from the first command-line argument.
mode = List.first(System.argv())

ProcessLinking.run(mode)
