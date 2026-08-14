defmodule ProcessLinking do
  def run("propagate") do
    IO.puts("Parent started: #{inspect(self())}")

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
    Process.flag(:trap_exit, true)
    IO.puts("Parent started: #{inspect(self())}")
    IO.puts("Parent is trapping exits")

    worker = spawn_link(fn ->
      IO.puts("Worker started: #{inspect(self())}")
      Process.sleep(500)

      raise "Worker crashed"
    end)

    IO.puts("Parent linked to worker: #{inspect(worker)}")

    receive do
      {:EXIT, ^worker, reason} ->
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

mode = List.first(System.argv())

ProcessLinking.run(mode)
