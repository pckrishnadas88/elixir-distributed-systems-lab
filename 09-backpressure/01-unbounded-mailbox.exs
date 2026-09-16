# ============================================================
# 09 - Backpressure: 01 - Unbounded Mailbox Explosion
#
# Demonstrates:
# - A fast producer sending messages with no rate-limiting.
# - A slow consumer processing messages with deliberate delay.
# - The BEAM process mailbox growing without bound.
# ============================================================

defmodule SlowConsumer do
  def start(parent) do
    spawn(fn ->
      loop(parent, 0)
    end)
  end

  defp loop(parent, processed_count) do
    receive do
      {:item, _item_id} ->
        # Simulate slow processing (e.g., database query or external API call)
        Process.sleep(15)

        updated_count = processed_count + 1

        # Periodic logging every 20 items
        if rem(updated_count, 20) == 0 do
          {:message_queue_len, queue_len} = Process.info(self(), :message_queue_len)
          {:memory, memory_bytes} = Process.info(self(), :memory)

          IO.puts(
            "Consumer: processed #{updated_count} items | " <>
              "Mailbox queue length: #{queue_len} | Memory: #{memory_bytes} bytes"
          )
        end

        loop(parent, updated_count)

      :stop ->
        IO.puts("Consumer: stopped")
    end
  end
end

defmodule FastProducer do
  def produce(consumer_pid, count) do
    IO.puts("Producer: Blasting #{count} items into consumer mailbox without waiting...")

    Enum.each(1..count, fn i ->
      send(consumer_pid, {:item, i})
    end)

    IO.puts("Producer: Finished sending all #{count} items!")
  end
end

# ============================================================
# RUN THE EXPERIMENT
# ============================================================

IO.puts("=== EXPERIMENT 1: UNBOUNDED MAILBOX GROWTH ===")
consumer = SlowConsumer.start(self())

total_items = 100
FastProducer.produce(consumer, total_items)

# Process.info/2 inspects the target process externally from the runtime
# WITHOUT placing an inspection message in the target's mailbox!
{:message_queue_len, queue_len} = Process.info(consumer, :message_queue_len)
{:memory, memory} = Process.info(consumer, :memory)

IO.puts("\n--- Immediate Snapshot After Producer Finished ---")
IO.puts("Items queued in mailbox : #{queue_len}")
IO.puts("Process memory usage    : #{memory} bytes")
IO.puts("--------------------------------------------------\n")

# Allow consumer to process remaining backlog
IO.puts("Waiting for consumer to drain backlog...")
Process.sleep(1700)

{:message_queue_len, final_queue_len} = Process.info(consumer, :message_queue_len)
{:memory, final_memory} = Process.info(consumer, :memory)

IO.puts("\n--- Final Snapshot After Backlog Drained ---")
IO.puts("Items queued in mailbox : #{final_queue_len}")
IO.puts("Process memory usage    : #{final_memory} bytes")
IO.puts("---------------------------------------------\n")

send(consumer, :stop)
Process.sleep(50)
IO.puts("Experiment 1 complete.")
