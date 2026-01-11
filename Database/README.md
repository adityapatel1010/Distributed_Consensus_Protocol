# Node Logging and Timer Management System

This project implements a **distributed node logging and timer system** in C++.
Each node maintains logs, writes them to individual files, and manages timers for coordinated operations.

---

## Steps to Build and Run

### 1. Prerequisites

* **GCC compiler** (GCC 11+)

### 2. Build

For compiling proto
```bash
protoc -I=. --cpp_out=. msg.proto
protoc -I=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` msg.proto
```

For compiling node_async.cpp
```bash
g++ node_async.cpp rpc_messages/msg.pb.cc rpc_messages/msg.grpc.pb.cc -o node \
  -lgrpc++ -lgrpc -lprotobuf -lpthread -ldl
```

### 3. Run

```bash
./node
```

---

## How to Run

1) Use 1 to process the inputs pending in queue after pressing 6

2) When starting wait to make initial leader and enter 6 to load a set
    - If a set contains FL then, first wait for the initial commands present above it to process, then again press 6. You will notice Leader fail and then a new leader election. Then, again press Enter to continue parsing rest of the input after LF in the same set (need to press 1 again for processing input)
    - In case of no LF, no need to do anything extra

3) PrintView is automatically created in view folder

4) Use 3 for PrintDB, appended in the existing file in db folder for each node separately

5) Use 2 for PrintLogs, write a new fresh log into a file for each node separately

6) Use Ctrl+C to exit the code, will kill all processes


## References

* ChatGPT, Claude & Gemini - For generating initial template of synchronus grpc, creating menu driven input and storing logs, db and view on file, ctrl+c to kill all processes and creating README. 

* Official GRPC github repo for understanding async grpc (No AI)

