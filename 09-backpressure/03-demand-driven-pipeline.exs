# ============================================================
# 09 - Backpressure: 03 - Demand-Driven Flow Control Pipeline
#
# Demonstrates:
# - Credit-based windowed demand flow control (GenStage foundation).
# - A consumer pulling batches of work based on capacity ({:ask, count}).
# - A producer dispatching only when accumulated demand allows.
# - Strictly bounded consumer mailbox with high pipeline throughput.
# ============================================================

defmodule DemandProducer do
  def start(items) do
    spawn(fn ->
      loop(%{buffer: items, demand: 0, consumer: nil})
    end)
  end

  defp loop(state) do
    receive do
      # Register the consumer
      {:subscribe, consumer_pid} ->
        IO.puts("Producer: Consumer #{inspect(consumer_pid)} connected")
        loop(%{state | consumer: consumer_pid})

      # Consumer requests 'count' more items (demand signal)
      {:ask, count} ->
        new_demand = state.demand + count

        IO.puts(
          "Producer: Received demand for #{count} item(s) " <>
            "(total accumulated demand: #{new_demand})"
        )

        state = %{state | demand: new_demand}
        state = dispatch(state)
        loop(state)

      :stop ->
        IO.puts("Producer: stopped")
    end
  end

  defp dispatch(%{consumer: nil} = state), do: state

  defp dispatch(%{demand: demand, buffer: buffer, consumer: consumer} = state)
       when demand > 0 and buffer != [] do
    # Take at most 'demand' items from the buffer
    batch_size = min(demand, length(buffer))
    {batch, remaining_buffer} = Enum.split(buffer, batch_size)
    remaining_demand = demand - batch_size

    IO.puts(
      "Producer: Dispatching batch of #{length(batch)} item(s) to consumer " <>
        "(remaining buffer: #{length(remaining_buffer)}, remaining demand: #{remaining_demand})"
    )

    send(consumer, {:data, batch})

    updated_state = %{state | buffer: remaining_buffer, demand: remaining_demand}

    # If buffer is now empty, notify consumer
    if remaining_buffer == [] do
      IO.puts("Producer: Buffer empty, notifying consumer that stream is done")
      send(consumer, :stream_done)
    end

    updated_state
  end

  defp dispatch(state), do: state
end

defmodule DemandConsumer do
  def start(parent, producer_pid, window_size) do
    spawn(fn ->
      IO.puts("Consumer: Started with window size #{window_size}")

      # Step 1: Connect to producer
      send(producer_pid, {:subscribe, self()})

      # Step 2: Request initial demand window
      IO.puts("Consumer: Requesting initial demand of #{window_size} item(s)...")
      send(producer_pid, {:ask, window_size})

      loop(parent, producer_pid, window_size, 0)
    end)
  end

  defp loop(parent, producer_pid, window_size, processed_count) do
    receive do
      {:data, batch} ->
        {:message_queue_len, queue_len} = Process.info(self(), :message_queue_len)

        IO.puts(
          "\nConsumer: >>> Received batch of #{length(batch)} items " <>
            "[Mailbox queue length: #{queue_len}]"
        )

        # Process each item in the batch
        Enum.each(batch, fn item ->
          # Simulate processing latency
          Process.sleep(15)
          IO.puts("Consumer: processed item #{item}")
        end)

        updated_count = processed_count + length(batch)

        # Replenish demand: ask for the number of processed items
        IO.puts("Consumer: Batch done. Replenishing demand (+#{length(batch)})...")
        send(producer_pid, {:ask, length(batch)})

        loop(parent, producer_pid, window_size, updated_count)

      :stream_done ->
        IO.puts("\nConsumer: Received stream_done signal. Total processed: #{processed_count}")
        send(parent, {:consumer_finished, processed_count})
        loop(parent, producer_pid, window_size, processed_count)

      :stop ->
        IO.puts("Consumer: stopped")
    end
  end
end

# ============================================================
# RUN THE EXPERIMENT
# ============================================================

IO.puts("=== EXPERIMENT 3: DEMAND-DRIVEN FLOW CONTROL PIPELINE ===")

total_items = 20
window_size = 5

items = Enum.to_list(1..total_items)
producer = DemandProducer.start(items)
consumer = DemandConsumer.start(self(), producer, window_size)

receive do
  {:consumer_finished, count} ->
    IO.puts("\nPipeline successfully completed! Total items processed: #{count}")
after
  10_000 ->
    IO.puts("Pipeline timed out!")
end

# Verify final queue length
{:message_queue_len, final_queue_len} = Process.info(consumer, :message_queue_len)
IO.puts("Final consumer mailbox queue length: #{final_queue_len}")

send(consumer, :stop)
send(producer, :stop)
Process.sleep(50)
IO.puts("Experiment 3 complete.")
