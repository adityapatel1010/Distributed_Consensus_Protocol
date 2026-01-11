# PBFT Node Logging and View-Change System

This project implements the core workflow of **Practical Byzantine Fault Tolerance (PBFT)** in C++.
Each replica maintains replicated logs, persists state to disk, and coordinates timers to drive view changes, leader election, and client request handling.

---

## Steps to Build and Run

### 1. Prerequisites

* **GCC compiler** (GCC 11+)

### 2. Build

For compiling protobuf/gRPC stubs (from `rpc_messages/msg.proto`):
```bash
cd rpc_messages
protoc -I=. --cpp_out=. msg.proto
protoc -I=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` msg.proto
cd ..
```

For compiling the PBFT replica (`pbft.cpp`):
```bash
g++ pbft.cpp rpc_messages/msg.pb.cc rpc_messages/msg.grpc.pb.cc -o node \
  -lgrpc++ -lgrpc -lprotobuf -lpthread -ldl -lcryptopp
```

### 3. Run

```bash
./node
```

---

## How to Run

1) Use 1 to process the inputs pending in queue after pressing 6

2) When starting the replicas, wait for the initial primary to form and enter 6 to load a PBFT request batch.

3) PrintView is automatically created in view folder

4) Use 3 for PrintDB, appended in the existing file in db folder for each node separately

5) Use 2 for PrintLogs, write a new fresh log into a file for each node separately

6) Use Ctrl+C to exit; this stops the replica process cleanly.


## References

* ChatGPT - For generating initial template of synchronus grpc, creating menu driven input and storing logs, db and view on file, ctrl+c to kill all processes, implemting crypography signature, digest and creating README. 

* Official GRPC github repo for understanding async grpc (No AI)

