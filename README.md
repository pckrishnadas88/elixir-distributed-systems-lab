# Building Distributed Systems in Elixir

[![GitHub Stars](https://img.shields.io/github/stars/pckrishnadas88/elixir-distributed-systems-lab?style=flat)](https://github.com/pckrishnadas88/elixir-distributed-systems-lab/stargazers)
[![GitHub Forks](https://img.shields.io/github/forks/pckrishnadas88/elixir-distributed-systems-lab?style=flat)](https://github.com/pckrishnadas88/elixir-distributed-systems-lab/network/members)
[![CI](https://img.shields.io/github/actions/workflow/status/pckrishnadas88/elixir-distributed-systems-lab/ci.yml?label=CI)](https://github.com/pckrishnadas88/elixir-distributed-systems-lab/actions)
[![Elixir](https://img.shields.io/badge/Elixir-1.18%2B-4B275F?logo=elixir&logoColor=white)](https://elixir-lang.org/)
[![License](https://img.shields.io/github/license/pckrishnadas88/elixir-distributed-systems-lab)](https://github.com/pckrishnadas88/elixir-distributed-systems-lab/blob/main/LICENSE)

> Learn distributed systems by building them from first principles using raw Elixir process primitives.

This repository is a collection of **small, self-contained, runnable examples** that demonstrate the building blocks of distributed systems on the BEAM.

The goal is **not** to learn Phoenix or build CRUD applications.

Instead, the focus is understanding how distributed systems are constructed from the ground up before introducing OTP abstractions such as `GenServer`, `Supervisor`, `Registry`, or distributed libraries.

Every example can be executed independently, explained in detail, and paired with a technical blog post.

---

# Learning Philosophy

Each project follows the same approach:

1. Explain the problem.
2. Design a minimal protocol.
3. Implement it using raw Elixir primitives.
4. Explain every message exchanged.
5. Discuss limitations.
6. Show how OTP solves those limitations.

The intention is to understand **why** an abstraction exists before using it.

---

# Repository Structure

```text
elixir-distributed-systems-lab/
│
├── 01-process-mailbox/
├── 02-request-reply/
├── 03-process-monitoring/
├── ...
└── README.md
```

Each example contains:

```text
README.md
example.exs
```

Every example is:

* Runnable with a single command
* Self-contained
* Minimal
* Focused on one concept
* Documented with diagrams
* Blog-ready

---

# Roadmap

## Foundations

* [x] **01 - Process Mailbox**

  * `spawn/1`
  * Process state
  * Mailboxes
  * `send`
  * `receive`
  * Request–reply protocol
  
  * Blog post: [Building a stateful process in Elixir without GenServer](https://dev.to/pckrishnadas88/building-a-stateful-process-in-elixir-without-genserver-58eb)

* [x] **02 - Correlated Request Reply**

  * `make_ref/0`
  * Request IDs
  * Matching replies
  * Concurrent clients
  
  * Blog post: [Correlated request–reply in Elixir](https://dev.to/pckrishnadas88/building-distributed-systems-with-elixir-02-correlated-request-reply-4b2a)

* [x] **03 - Process Monitoring**

  * `Process.monitor/1`
  * Detecting process termination
  * DOWN messages
  
  * Blog post: [Process monitoring in Elixir](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-3-process-monitoring-5b5p)

* [x] **04 - Process Linking**

  * `spawn_link/1`
  * Exit propagation
  * Linked processes

  * Blog post: [Process linking in Elixir](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-4-process-linking-okb)

* [x] **05 - Supervisor From Scratch**

  * Manual restart loop
  * One-for-one restart strategy
  * Why supervisors exist

  * Blog post: [Supervisor from scratch](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-5-supervisor-from-scratch-32mh)

---

## Process Coordination

* [x] **06 - Named Processes**

  * Process registration
  * Global lookup
  * Avoiding PID sharing

  * Blog post: [Named processes](https://dev.to/pckrishnadas88/building-distributed-systems-in-elixir-part-6-named-processes-l3)

* [ ] **07 - Worker Pool**

  * Task distribution
  * Multiple workers
  * Load balancing

* [ ] **08 - Publish / Subscribe**

  * Multiple subscribers
  * Broadcast messaging

* [ ] **09 - Backpressure**

  * Fast producers
  * Slow consumers
  * Mailbox growth

---

## Distributed BEAM

* [ ] **10 - Connecting Nodes**

  * `Node.connect/1`
  * Distributed Erlang

* [ ] **11 - Remote Message Passing**

  * Sending messages across nodes

* [ ] **12 - Detecting Node Failures**

  * `Node.monitor/2`
  * Network partitions

---

## Replication

* [ ] **13 - Distributed Counter**

  * State replication
  * Consistency challenges

* [ ] **14 - Primary / Replica**

  * Single writer
  * Replication protocol

* [ ] **15 - Heartbeats**

  * Failure detection
  * Liveness

* [ ] **16 - Leader Election**

  * Choosing a coordinator
  * Split-brain discussion

---

## Fault Tolerance

* [ ] **17 - Minimal Job Queue**

  * Job scheduling
  * Worker retries
  * Failed jobs

* [ ] **18 - Reliable Messaging**

  * ACK protocol
  * Retries
  * Duplicate handling

* [ ] **19 - Write Ahead Log**

  * Crash recovery
  * Persistent messages

---

## Distributed Data Structures

* [ ] **20 - Distributed Key-Value Store**

  * Replicated state
  * Request routing

* [ ] **21 - G-Counter (CRDT)**

  * Eventual consistency
  * Merge operations

---

## Final Project

* [ ] **22 - Tiny Distributed Message Queue**

A minimal distributed queue built entirely from concepts introduced in previous examples.

Features include:

* Multiple nodes
* Worker processes
* Reliable delivery
* Job retries
* Monitoring
* Supervision
* Persistence
* Failure recovery

---

# Who Is This Repository For?

Developers interested in:

* Distributed systems
* Erlang/OTP
* Elixir internals
* RabbitMQ concepts
* Fault tolerance
* Message passing
* Concurrent programming
* BEAM architecture

---

# Running an Example

```bash
cd 01-process-mailbox

elixir mailbox.exs
```

Each example is completely independent and can be studied in any order, although following the roadmap is recommended.

---

# Blog Series

Each project in this repository is accompanied by a technical article explaining:

* The real-world problem
* Design decisions
* Protocol diagrams
* Message flow
* Failure cases
* Trade-offs
* Relationship to OTP abstractions



---

# License

The source code in this repository is licensed under the [MIT License](LICENSE).

The written articles in each `blog.md` file are licensed under [Creative Commons Attribution-NonCommercial 4.0 International](https://creativecommons.org/licenses/by-nc/4.0/). You may share and adapt that content with attribution, but not for commercial purposes.
