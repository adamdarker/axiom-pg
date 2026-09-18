# Axiom Agent

The Axiom Agent is a lightweight daemon that manages PostgreSQL clusters using a state-reconciliation model.

## Project Structure

- `cmd/axiom-agent`: Main entry point.
- `internal/agent`: Core Agent logic and reconciliation loop.
- `internal/grpc`: gRPC server implementation.
- `pkg/api/v1`: gRPC service definitions (Protobuf).

## Getting Started

### Prerequisites

- Go 1.25+
- Protocol Buffers compiler (`protoc`)
- Go gRPC plugins

### Building

```bash
go build -o axiom-agent ./cmd/axiom-agent
```

### Running

```bash
./axiom-agent
```
