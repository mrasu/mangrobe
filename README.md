# Mangrobe

Time-series Table Protocol - like Iceberg, but built for time-series, streaming, and multi-tenancy.

# Features

* **Fast file registration**. Adding new files won't conflict.
* **Easy operation**. No Hadoop or ZooKeeper required.
* **Easy integration**. No complicated implementation needed — just gRPC.
* **Multi-tenancy**. No conflicts across tenants.

# Examples

You can see examples in [examples](./mangrobe-lab/examples) directory

# Overview

![](./docs/img/overview.png)

# Mangrobe Protocol

Refer to [api.proto](./spec/proto/api.proto) for details.

## Add Files
Use `AddFiles` to register new files.

```mermaid
sequenceDiagram
    Writer->>API Server: gRPC(AddFiles)
    API Server-->>Writer: Done
```

## Compact Files
Use `CompactFiles` to compact existing files after acquiring the lock.

```mermaid
sequenceDiagram
    Writer->>API Server: gRPC(AcquireFileLock)
    API Server-->>Writer: 🔒 File Locked
    Writer->>API Server: gRPC(CompactFiles)
    API Server-->>Writer: Done
```

## Multi-stream
Locks are per file, so other streams and files outside the lock can still be updated.

```mermaid
sequenceDiagram
    participant Stream1
    participant Stream2
    participant Stream3
    participant API Server
    
    Stream1->>API Server: gRPC(AcquireFileLock)
    API Server-->>Stream1: 🔒 File Locked
    activate Stream1
    Note left of Stream1: 🔒 Compacting files
    Stream1->> API Server: gRPC(AddFiles)
    activate Stream1
    Note left of Stream1: Other files remain unlocked
    API Server-->>Stream1: Done
    deactivate Stream1
    Stream2->>API Server: gRPC(AddFiles)
    API Server-->>Stream2: Done
    Stream3->>API Server: gRPC(AddFiles)
    API Server-->>Stream3: Done
    Stream1->>API Server: gRPC(CompactFiles)
    deactivate Stream1
    API Server-->>Stream1: Done
```
