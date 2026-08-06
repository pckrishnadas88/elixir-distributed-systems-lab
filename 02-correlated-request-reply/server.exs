defmodule Server do
  def start do
    spawn(fn -> loop() end)
  end

  defp loop do
    receive do
      {:request, from, ref, value} ->
        IO.puts("Server received request: #{value}")
        result = value * 2
        send(from, {:reply, ref, result})
        loop()

      :stop ->
        IO.puts("Server stopped")
        :ok
    end
  end
end
