# Load the example's two modules relative to this file, not the shell's directory.
Code.require_file("server.exs", __DIR__)
Code.require_file("client.exs", __DIR__)

# All requests below share one server process but use distinct correlation references.
server = Server.start()

IO.inspect(Client.request(server, 10))
IO.inspect(Client.request(server, 20))
IO.inspect(Client.request(server, 30))

send(server, :stop)
