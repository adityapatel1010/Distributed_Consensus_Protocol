# Distributed Consensus Protocols

This project is an in-depth implementation and evaluation of **distributed consensus protocols** designed to ensure **reliability, consistency, and fault tolerance** in distributed systems. It explores both crash-fault-tolerant and Byzantine-fault-tolerant consensus mechanisms through practical implementations and performance analysis.

The system focuses on understanding how modern distributed systems maintain agreement across unreliable networks and nodes while scaling to high-throughput workloads.

---

## Overview

Consensus lies at the core of distributed systems, enabling multiple nodes to agree on shared state despite failures. This project implements and evaluates three foundational consensus protocols:

- **Paxos** – for crash fault tolerance and leader-based agreement  
- **Raft** – for understandable and maintainable consensus with strong leader semantics  
- **PBFT (Practical Byzantine Fault Tolerance)** – for resilience against malicious or Byzantine failures  
- **Scalable Database System** – for resilience against malicious or Byzantine failures  

Each protocol is engineered to highlight its design principles, fault-handling behavior, and performance trade-offs.

---

## Key Features

### Multi-Protocol Consensus Implementation
- Full implementations of **Paxos**, **Raft**, and **PBFT**
- Designed to tolerate node crashes, message delays, and failures
- Clear separation of protocol roles such as leaders, acceptors, proposers, and replicas

### Fault Tolerance & Reliability
- Crash fault tolerance in Paxos and Raft
- Byzantine fault tolerance in PBFT with cryptographic signing and verification
- Simulation of failure and recovery scenarios to validate correctness

### Scalability & Performance Evaluation
- Paxos extended with **sharding and replication**
- **Two-Phase Commit (2PC)** used for cross-shard coordination
- Sequential consistency enforced during distributed transactions
- Performance benchmarking conducted using **YCSB workloads**
- Achieved throughput of **~3200 transactions per second** under evaluated configurations

### Distributed Communication
- Efficient inter-node communication using **gRPC**
- Asynchronous execution using **coroutines** for improved concurrency
- Designed to closely reflect real-world distributed system behavior

---

## Experimental Evaluation

The project evaluates:
- **Throughput and latency** under varying workloads
- **Scalability** with increased shards and replicas
- **Fault recovery behavior** during node crashes and restarts
- **Consistency guarantees** under concurrent and cross-shard operations

YCSB benchmarks are used to model realistic database workloads and measure system performance under stress.

---

## Learning Outcomes

This project provides hands-on experience with:
- Core principles of **distributed consensus**
- Trade-offs between **consistency, availability, and fault tolerance**
- Differences between **crash fault tolerance** and **Byzantine fault tolerance**
- Designing systems that scale while maintaining correctness
- Evaluating distributed systems using industry-standard benchmarks

---

## Conclusion

This project demonstrates a comprehensive understanding of consensus protocols and their role in building robust distributed systems. By implementing and evaluating Paxos, Raft, and PBFT, it highlights the strengths and limitations of each approach in terms of performance, scalability, and fault tolerance.

The work bridges theoretical foundations with practical system design, making it applicable to real-world systems such as distributed databases, coordination services, and replicated state machines.
