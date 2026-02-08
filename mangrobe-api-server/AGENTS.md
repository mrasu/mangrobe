# Repository Guidelines

## Project Structure & Module Organization
- `src/` is the gRPC API server for the refarence implementation of Mangrobe Protocl.
- `src/domain/`: core business models, rules, and ports (traits) with no infrastructure dependencies.
- `src/application/`: use cases/services orchestrating domain behavior and defining DTOs/adapters.
- `src/infrastructure/`: DB/repos/external service implementations for domain ports.
- `src/grpc/`: gRPC delivery layer (handlers + protobuf/app mappings).
- `src/generated/`: protobuf output from `build.rs` (do not edit by hand).
- `src/util/`: shared helpers/utilities (keep dependency direction in mind).
- `src/*.rs`: module roots and wiring (`domain.rs`, `application.rs`, `infrastructure.rs`, `grpc.rs`, `util.rs`), plus `main.rs` as the binary entry point.
- `migration/` is the SeaORM migration crate used for database schema changes.
- `../spec/proto/api.proto` (outside this crate) has the definition of Mangrobe Protocol. `build.rs` compiles it into `src/generated/` and emits a descriptor file.

## Architecture & Dependency Rules
- `domain/` is pure business logic and must not depend on `application/`, `infrastructure/`, `grpc/`, or external frameworks. Define traits (ports) here for persistence or integrations.
- `application/` orchestrates use cases. It can depend on `domain/`, but should avoid direct DB or network code.
- `infrastructure/` implements `domain` ports (e.g., repositories, DB access) and can depend on external crates like SeaORM. It may depend on `domain/` but not on `application/`.
- `grpc/` is the delivery layer. It wires protobuf types to application use cases and should not contain business rules.
- `generated/` is build output; do not edit by hand.


## Build, Test, and Development Commands
- `cargo build`: compile the API server.

## Coding Style

### General Guidance
- Follow Rust's convention

### Error
- gRPC-specific errors must be created and used only inside `src/grpc/`.
- Application-level errors live in `src/util/error.rs`.
- Fine-grained errors (e.g., "row not found") may be defined within each file as needed.
