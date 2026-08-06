Code.require_file("server.exs", __DIR__)
Code.require_file("client.exs", __DIR__)

server = Server.start()

IO.inspect(Client.request(server, 10))
IO.inspect(Client.request(server, 20))
IO.inspect(Client.request(server, 30))

send(server, :stop)
