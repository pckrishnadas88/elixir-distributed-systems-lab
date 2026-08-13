defmodule Worker do
  def start(name) do
    spawn(fn -> loop(name) end)
  end

  defp loop(name) do
    receive do
      {:work, from, value} ->
        IO.puts("#{name} received #{value}")
        send(from, {:done, name, value})
        loop(name)

      :stop ->
        IO.puts("#{name} stopping normally")
        :ok

      :crash ->
        raise "#{name} crashed"
    end
  end
end

defmodule MonitorDemo do
  def normal_exit do
    worker = Worker.start("worker-1")
    ref = Process.monitor(worker)

    send(worker, :stop)

    receive do
      {:DOWN, ^ref, :process, ^worker, reason} ->
        IO.puts("worker-1 terminated")
        IO.puts("reason: #{inspect(reason)}")
    end
  end

  def crash_exit do
    worker = Worker.start("worker-2")
    ref = Process.monitor(worker)

    send(worker, :crash)

    receive do
      {:DOWN, ^ref, :process, ^worker, reason} ->
        IO.puts("worker-2 terminated")
        IO.puts("reason: #{inspect(reason)}")
    end
  end

  def multiple_workers do
    worker1 = Worker.start("worker-1")
    worker2 = Worker.start("worker-2")
    worker3 = Worker.start("worker-3")

    ref1 = Process.monitor(worker1)
    ref2 = Process.monitor(worker2)
    ref3 = Process.monitor(worker3)

    send(worker1, :stop)
    send(worker2, :crash)
    send(worker3, :stop)

    wait_for_workers(%{
      ref1 => "worker-1",
      ref2 => "worker-2",
      ref3 => "worker-3"
    })
  end

  defp wait_for_workers(monitors) when map_size(monitors) == 0 do
    IO.puts("all workers terminated")
  end

  defp wait_for_workers(monitors) do
    receive do
      {:DOWN, ref, :process, pid, reason} ->
        case Map.pop(monitors, ref) do
          {nil, monitors} ->
            wait_for_workers(monitors)

          {name, remaining} ->
            IO.puts("#{name} terminated")
            IO.puts("pid: #{inspect(pid)}")
            IO.puts("reason: #{inspect(reason)}")
            IO.puts("")

            wait_for_workers(remaining)
        end
    end
  end
end

IO.puts("\n--- Normal Exit ---")
MonitorDemo.normal_exit()

IO.puts("\n--- Crash Exit ---")
MonitorDemo.crash_exit()

IO.puts("\n--- Multiple Workers ---")
MonitorDemo.multiple_workers()
