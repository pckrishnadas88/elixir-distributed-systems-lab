# ============================================================
# 08 - Publish / Subscribe Demo
#
# Demonstrates:
# - Multiple subscribers registered under different topics
# - 1-to-many broadcast messaging
# - Dynamic unsubscription
# - Automatic dead-process cleanup via Process.monitor
# ============================================================

Code.require_file("subscriber.exs", __DIR__)
Code.require_file("broker.exs", __DIR__)

IO.puts("=== 1. STARTING BROKER AND SUBSCRIBERS ===")
broker = Broker.start()

alice = Subscriber.start("Alice")
bob = Subscriber.start("Bob")
charlie = Subscriber.start("Charlie")

Process.sleep(50)

IO.puts("\n=== 2. REGISTERING TOPIC SUBSCRIPTIONS ===")
# Alice subscribes to :tech_news and :sports
Broker.subscribe(broker, :tech_news, alice)
Broker.subscribe(broker, :sports, alice)

# Bob subscribes to :tech_news
Broker.subscribe(broker, :tech_news, bob)

# Charlie subscribes to :sports and :weather
Broker.subscribe(broker, :sports, charlie)
Broker.subscribe(broker, :weather, charlie)

Process.sleep(50)

IO.puts("\n=== 3. BROADCAST MESSAGING ===")

IO.puts("\n--- Broadcast to :tech_news (Alice and Bob should receive) ---")
Broker.publish(broker, :tech_news, "Elixir 1.18 Released!")
Process.sleep(50)

IO.puts("\n--- Broadcast to :sports (Alice and Charlie should receive) ---")
Broker.publish(broker, :sports, "Local team wins championship!")
Process.sleep(50)

IO.puts("\n--- Broadcast to :weather (Only Charlie should receive) ---")
Broker.publish(broker, :weather, "Sunny, 24C with light breeze")
Process.sleep(50)

IO.puts("\n--- Broadcast to :finance (No subscribers) ---")
Broker.publish(broker, :finance, "Market closes at record high")
Process.sleep(50)

IO.puts("\n=== 4. DYNAMIC UNSUBSCRIPTION ===")
IO.puts("Bob unsubscribes from :tech_news")
Broker.unsubscribe(broker, :tech_news, bob)
Process.sleep(50)

IO.puts("\n--- Broadcast to :tech_news (Now only Alice should receive) ---")
Broker.publish(broker, :tech_news, "Distributed Systems Lab Part 8 published!")
Process.sleep(50)

IO.puts("\n=== 5. FAULT TOLERANCE & MONITOR CLEANUP ===")
IO.puts("Simulating sudden crash for Charlie...")
send(charlie, :simulate_crash)
Process.sleep(100)

IO.puts("\n--- Broadcast to :sports (Charlie was pruned; only Alice receives) ---")
Broker.publish(broker, :sports, "Upcoming match schedule updated")
Process.sleep(50)

IO.puts("\n=== 6. CLEANUP & SHUTDOWN ===")
send(alice, :stop)
send(bob, :stop)
Broker.stop(broker)
Process.sleep(50)

IO.puts("\nAll pub/sub demonstrations completed successfully.")
