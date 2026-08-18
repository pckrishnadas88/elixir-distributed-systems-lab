defmodule Worker do
  def start(name) do
    # Link each worker to its supervisor so failures arrive as exit signals.
    spawn_link(fn -> loop(name, 0) end)
  end

  defp loop(name, count) do
    receive do
      {:work, caller, value} ->
        # State is immutable, so the next recursive call receives the updated count.
        new_count = count + 1
        send(caller, {:done, name, value, new_count})
        loop(name, new_count)

      :crash ->
        # An abnormal exit lets the supervisor demonstrate restart handling.
        raise "#{name} crashed"

      :stop ->
        IO.puts("#{name} stopping normally")
        :ok
    end
  end
end

defmodule ManualSupervisor do
  def start(worker_names) do
    # The supervisor itself is unlinked from its caller in this minimal example.
    spawn(fn -> init(worker_names) end)
  end

  def children(supervisor) do
    # Correlate the reply so unrelated `:children` messages are not consumed.
    ref = make_ref()
    send(supervisor, {:children, self(), ref})

    receive do
      {:children, ^ref, children} -> children
    after
      1_000 -> {:error, :timeout}
    end
  end

  defp init(worker_names) do
    # Receive linked workers' exits as messages instead of dying with them.
    Process.flag(:trap_exit, true)

    # Map PIDs to names; PIDs change after restarts while names stay stable.
    children =
      Map.new(worker_names, fn name ->
        pid = start_child(name)
        {pid, name}
      end)

    loop(children)
  end

  defp loop(children) do
    receive do
      {:EXIT, pid, :normal} ->
        # Normal stops intentionally remove a child rather than restarting it.
        name = Map.fetch!(children, pid)
        IO.puts("supervisor: #{name} stopped normally; not restarting")
        loop(Map.delete(children, pid))

      {:EXIT, pid, reason} ->
        # Any non-normal exit uses a one-for-one restart strategy.
        name = Map.fetch!(children, pid)
        IO.puts("supervisor: #{name} exited with #{inspect(reason)}")

        children = Map.delete(children, pid)
        new_pid = start_child(name)
        IO.puts("supervisor: restarted #{name} as #{inspect(new_pid)}")

        loop(Map.put(children, new_pid, name))

      {:children, caller, ref} ->
        # Present the public view as name => PID, reversing the internal map.
        by_name = Map.new(children, fn {pid, name} -> {name, pid} end)
        send(caller, {:children, ref, by_name})
        loop(children)

      :stop ->
        # Ask all children to exit normally, then wait for every linked exit signal.
        Enum.each(Map.keys(children), &send(&1, :stop))
        wait_for_children(Map.keys(children))
    end
  end

  defp start_child(name) do
    pid = Worker.start(name)
    IO.puts("supervisor: started #{name} as #{inspect(pid)}")
    pid
  end

  defp wait_for_children([]), do: :ok

  defp wait_for_children(children) do
    receive do
      # Remove each PID as its final exit signal arrives.
      {:EXIT, pid, _reason} -> wait_for_children(List.delete(children, pid))
    end
  end
end

defmodule SupervisorDemo do
  def run do
    supervisor = ManualSupervisor.start([:worker_1, :worker_2])
    children_before = ManualSupervisor.children(supervisor)

    worker_1_before = children_before.worker_1
    worker_2_before = children_before.worker_2

    # Crash one child and later compare PIDs to verify isolated replacement.
    send(worker_1_before, :crash)

    children_after = wait_for_restart(supervisor, :worker_1, worker_1_before)

    IO.puts("worker_1 restarted: #{children_after.worker_1 != worker_1_before}")
    IO.puts("worker_2 unchanged: #{children_after.worker_2 == worker_2_before}")

    send(children_after.worker_1, {:work, self(), :job_after_restart})

    receive do
      message -> IO.inspect(message, label: "reply")
    after
      1_000 -> IO.puts("reply timed out")
    end

    send(supervisor, :stop)
  end

  defp wait_for_restart(supervisor, name, old_pid, attempts \\ 50)

  defp wait_for_restart(_supervisor, _name, _old_pid, 0) do
    raise "worker was not restarted"
  end

  defp wait_for_restart(supervisor, name, old_pid, attempts) do
    # Poll the supervisor's current child map until the named child has a new PID.
    children = ManualSupervisor.children(supervisor)

    case Map.get(children, name) do
      pid when is_pid(pid) and pid != old_pid ->
        children

      _ ->
        Process.sleep(10)
        wait_for_restart(supervisor, name, old_pid, attempts - 1)
    end
  end
end

# Execute the self-contained restart demonstration.
SupervisorDemo.run()
