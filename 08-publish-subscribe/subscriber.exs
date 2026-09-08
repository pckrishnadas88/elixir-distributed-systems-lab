defmodule Subscriber do
  # ============================================================
  # Subscriber Process
  #
  # A subscriber:
  #
  # 1. Starts with an assigned human-readable name.
  # 2. Listens for broadcast messages sent by the Broker.
  # 3. Processes each received broadcast event.
  # 4. Supports graceful shutdown and crash demonstration.
  # ============================================================

  def start(name) do
    spawn(fn ->
      IO.puts("Subscriber #{name} started with PID #{inspect(self())}")
      loop(name, 0)
    end)
  end

  # ============================================================
  # Subscriber Loop
  #
  # Maintains process state:
  # - name: identifier for logging
  # - received_count: number of broadcasts processed
  # ============================================================

  defp loop(name, received_count) do
    receive do
      # ========================================================
      # Broadcast message received from the Broker.
      # ========================================================
      {:broadcast, topic, message} ->
        updated_count = received_count + 1

        IO.puts(
          "[#{name}] Received on #{inspect(topic)}: " <>
            "#{inspect(message)} (total received: #{updated_count})"
        )

        loop(name, updated_count)

      # ========================================================
      # Query total received message count.
      # ========================================================
      {:get_count, caller, ref} ->
        send(caller, {:count, ref, received_count})
        loop(name, received_count)

      # ========================================================
      # Deliberate crash to demonstrate Broker's process
      # monitor pruning dead subscriber PIDs.
      # ========================================================
      :simulate_crash ->
        IO.puts("[#{name}] Simulating sudden process crash!")
        exit(:simulated_crash)

      # ========================================================
      # Graceful shutdown.
      # ========================================================
      :stop ->
        IO.puts("[#{name}] Stopped gracefully")
    end
  end
end
