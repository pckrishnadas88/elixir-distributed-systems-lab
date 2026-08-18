defmodule Client do
  def request(server, value) do
    # Each request gets a unique token, even when one process has several in flight.
    ref = make_ref()
    IO.puts("Client #{inspect(self())} sending #{value}")
    IO.puts("Request ref: #{inspect(ref)}")
    send(server, {:request, self(), ref, value})

    receive do
      # Pinning (`^ref`) prevents this receive from consuming another request's reply.
      {:reply, ^ref, result} ->
        IO.puts("Client #{inspect(self())} received matching reply :#{result}")
        {:ok, result}
    end
  end
end
