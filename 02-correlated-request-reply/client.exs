defmodule Client do
  def request(server, value) do
    ref = make_ref()
    IO.puts("Client #{inspect(self())} sending #{value}")
    IO.puts("Request ref: #{inspect(ref)}")
    send(server, {:request, self(), ref, value})

    receive do
      {:reply, ^ref, result} ->
        IO.puts("Client #{inspect(self())} received matching reply :#{result}")
        {:ok, result}
    end
  end
end
