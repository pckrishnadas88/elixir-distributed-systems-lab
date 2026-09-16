# ============================================================
# 09 - Backpressure: 02 - Stop-and-Wait (Ack-Based) Flow Control
#
# Demonstrates:
# - Synchronous acknowledgment between producer and consumer.
# - The producer waiting for consumer readiness before sending next item.
# - The consumer mailbox queue length never exceeding 1.
# ============================================================

defmodule AckConsumer do
  def start do
    spawn(fn ->
      loop(0)
    end)
  end

  defp loop(processed_count) do
    receive do
      {:item, reply_to, ref, item_id} ->
        # Inspect mailbox before processing: at most 0 other messages waiting!
        {:message_queue_len, queue_len} = Process.info(self(), :message_queue_len)

        # Simulate work
        Process.sleep(15)

        updated_count = processed_count + 1

        if rem(updated_count, 10) == 0 do
          IO.puts(
            "Consumer: processed item #{item_id} (total: #{updated_count}) | " <>
              "Mailbox queue length: #{queue_len}"
          )
        end

        # Send acknowledgment back to producer
        send(reply_to, {:ack, ref})

        loop(updated_count)

      :stop ->
        IO.puts("Consumer: stopped")
    end
  end
end

defmodule AckProducer do
  def produce(consumer_pid, count) do
    IO.puts("Producer: Sending #{count} items with stop-and-wait acknowledgment...")

    Enum.each(1..count, fn i ->
      ref = make_ref()
      send(consumer_pid, {:item, self(), ref, i})

      # Block until consumer acknowledges receipt of this item
      receive do
        {:ack, ^ref} ->
          :ok
      after
        5000 ->
          IO.puts("Producer: ERROR - consumer timed out!")
      end
    end)

    IO.puts("Producer: All #{count} items acknowledged by consumer!")
  end
end

# ============================================================
# RUN THE EXPERIMENT
# ============================================================

IO.puts("=== EXPERIMENT 2: ACK-BASED (STOP-AND-WAIT) FLOW CONTROL ===")
consumer = AckConsumer.start()

total_items = 50
AckProducer.produce(consumer, total_items)

{:message_queue_len, final_queue_len} = Process.info(consumer, :message_queue_len)
IO.puts("\nFinal consumer mailbox queue length: #{final_queue_len}")

send(consumer, :stop)
IO.puts("Experiment 2 complete.")
