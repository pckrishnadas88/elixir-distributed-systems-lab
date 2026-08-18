defmodule Server do
  # Spawn a server process whose behavior is defined by its mailbox loop.
  def start do
    spawn(fn -> loop() end)
  end

  defp loop do
    receive do
      {:request, from, ref, value} ->
        # Echo the caller's unique reference so concurrent replies stay identifiable.
        IO.puts("Server received request: #{value}")
        result = value * 2
        send(from, {:reply, ref, result})
        loop()

      :stop ->
        # Do not recur after a stop message; this ends the server normally.
        IO.puts("Server stopped")
        :ok
    end
  end
end
