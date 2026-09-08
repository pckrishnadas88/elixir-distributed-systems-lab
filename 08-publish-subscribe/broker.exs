defmodule Broker do
  # ============================================================
  # Publish / Subscribe Broker
  #
  # The Broker is a central coordination process responsible for:
  #
  # 1. Tracking topics and their subscribed process IDs.
  # 2. Monitoring subscribers so dead processes are pruned automatically.
  # 3. Broadcasting messages to all subscribers registered for a topic.
  # 4. Handling dynamic subscriptions and unsubscriptions.
  # ============================================================

  # ============================================================
  # Client API
  # ============================================================

  def start do
    spawn(fn ->
      IO.puts("Broker started with PID #{inspect(self())}")
      loop(%{topics: %{}, monitors: %{}})
    end)
  end

  def subscribe(broker, topic, subscriber_pid \\ nil) do
    target_pid = subscriber_pid || self()
    ref = make_ref()
    send(broker, {:subscribe, self(), ref, topic, target_pid})

    receive do
      {:ok, ^ref} -> :ok
    after
      5000 -> {:error, :timeout}
    end
  end

  def unsubscribe(broker, topic, subscriber_pid \\ nil) do
    target_pid = subscriber_pid || self()
    ref = make_ref()
    send(broker, {:unsubscribe, self(), ref, topic, target_pid})

    receive do
      {:ok, ^ref} -> :ok
    after
      5000 -> {:error, :timeout}
    end
  end

  def publish(broker, topic, message) do
    send(broker, {:publish, topic, message})
    :ok
  end

  def subscribers(broker, topic) do
    ref = make_ref()
    send(broker, {:get_subscribers, self(), ref, topic})

    receive do
      {:subscribers, ^ref, subs} -> subs
    after
      5000 -> {:error, :timeout}
    end
  end

  def stop(broker) do
    send(broker, :stop)
    :ok
  end

  # ============================================================
  # Server Loop
  #
  # State structure:
  # %{
  #   topics: %{topic_name => [pid1, pid2, ...]},
  #   monitors: %{pid => monitor_ref}
  # }
  # ============================================================

  defp loop(state) do
    receive do
      # ========================================================
      # SUBSCRIBE
      #
      # Register a subscriber PID under a topic.
      # If not already monitored, monitor the subscriber.
      # ========================================================
      {:subscribe, caller, ref, topic, subscriber_pid} ->
        existing_subscribers = Map.get(state.topics, topic, [])

        # Avoid duplicate subscriptions for the same topic.
        updated_subscribers =
          if subscriber_pid in existing_subscribers do
            existing_subscribers
          else
            existing_subscribers ++ [subscriber_pid]
          end

        # Monitor subscriber if not already monitored.
        monitors =
          case Map.get(state.monitors, subscriber_pid) do
            nil ->
              mref = Process.monitor(subscriber_pid)
              Map.put(state.monitors, subscriber_pid, mref)

            _existing_ref ->
              state.monitors
          end

        updated_topics = Map.put(state.topics, topic, updated_subscribers)

        IO.puts(
          "Broker: #{inspect(subscriber_pid)} subscribed to topic #{inspect(topic)}"
        )

        send(caller, {:ok, ref})

        loop(%{state | topics: updated_topics, monitors: monitors})

      # ========================================================
      # UNSUBSCRIBE
      #
      # Remove a subscriber PID from a topic.
      # If subscriber is no longer on any topic, demonitor it.
      # ========================================================
      {:unsubscribe, caller, ref, topic, subscriber_pid} ->
        existing_subscribers = Map.get(state.topics, topic, [])
        updated_subscribers = List.delete(existing_subscribers, subscriber_pid)

        updated_topics =
          if updated_subscribers == [] do
            Map.delete(state.topics, topic)
          else
            Map.put(state.topics, topic, updated_subscribers)
          end

        IO.puts(
          "Broker: #{inspect(subscriber_pid)} unsubscribed from topic #{inspect(topic)}"
        )

        # Check if the process is still subscribed to any other topic.
        still_subscribed? =
          Enum.any?(updated_topics, fn {_t, subs} ->
            subscriber_pid in subs
          end)

        monitors =
          if not still_subscribed? and Map.has_key?(state.monitors, subscriber_pid) do
            mref = Map.get(state.monitors, subscriber_pid)
            Process.demonitor(mref, [:flush])
            Map.delete(state.monitors, subscriber_pid)
          else
            state.monitors
          end

        send(caller, {:ok, ref})

        loop(%{state | topics: updated_topics, monitors: monitors})

      # ========================================================
      # PUBLISH / BROADCAST
      #
      # Fan-out: send message to all registered subscribers.
      # ========================================================
      {:publish, topic, message} ->
        subscribers = Map.get(state.topics, topic, [])

        IO.puts(
          "Broker: Broadcasting to #{length(subscribers)} subscriber(s) " <>
            "on topic #{inspect(topic)}: #{inspect(message)}"
        )

        Enum.each(subscribers, fn sub_pid ->
          send(sub_pid, {:broadcast, topic, message})
        end)

        loop(state)

      # ========================================================
      # GET SUBSCRIBERS
      #
      # Return the current list of subscribers for a topic.
      # ========================================================
      {:get_subscribers, caller, ref, topic} ->
        subs = Map.get(state.topics, topic, [])
        send(caller, {:subscribers, ref, subs})
        loop(state)

      # ========================================================
      # SUBSCRIBER CRASH / DOWN DETECTION
      #
      # When a subscriber crashes or terminates, clean up
      # its PID from all topic subscription lists automatically.
      # ========================================================
      {:DOWN, _ref, :process, dead_pid, reason} ->
        IO.puts(
          "Broker: Subscriber #{inspect(dead_pid)} exited (#{inspect(reason)}). " <>
            "Pruning from all topics."
        )

        # Remove dead PID from all topic lists.
        updated_topics =
          state.topics
          |> Enum.map(fn {topic, subs} ->
            {topic, List.delete(subs, dead_pid)}
          end)
          |> Enum.reject(fn {_topic, subs} -> subs == [] end)
          |> Map.new()

        updated_monitors = Map.delete(state.monitors, dead_pid)

        loop(%{state | topics: updated_topics, monitors: updated_monitors})

      # ========================================================
      # STOP
      # ========================================================
      :stop ->
        IO.puts("Broker: Stopped gracefully")
    end
  end
end
