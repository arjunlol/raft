## Raft (sleep-consensus)

Raft is a small, educational Raft consensus implementation in Python, built on `asyncio`.
It focuses on clarity and close alignment with the original Raft paper terminology
(`term`, `commit_index`, `AppendEntries`, etc.).

This repository currently provides:

- **Core data types**: `Log`, `LogEntry`, RPC message dataclasses
- **In‑process transport**: `InMemoryTransport` using `asyncio.Queue`
- **State machine**: a simple `DictStateMachine` for experiments and tests
- **Cluster wiring**: `RaftCluster` to spin up multiple nodes in a single process

Consensus logic (leader election, log replication, etc.) will be added incrementally
in later commits; for now this is a clean, testable core.

---

## Requirements

- Python `>= 3.11` (the examples below assume a Unix-like shell)

---

## Setup and installation

From the project root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

You can use any supported Python 3.11+ interpreter; just replace `python3.12`
with your preferred version.

---

## Running the test suite

After activating the virtual environment:

```bash
pytest -v
```

This runs the core tests, which currently cover:

- Basic `Log` behavior (append, truncate, sentinel at index 0)
- `DictStateMachine` operations (`set`, `get`, `delete`)
- `RaftCluster` startup/shutdown behavior for a 3-node cluster

---

## Running a simple in-process cluster

There is no CLI yet; you use this library from your own Python code.
Here is a minimal example that boots a 3-node cluster and keeps it running
until you interrupt the process:

```python
import asyncio

from raft import RaftCluster


async def main() -> None:
    cluster = RaftCluster(["node-0", "node-1", "node-2"])
    await cluster.start()

    print("Cluster started with nodes:", list(cluster.nodes.keys()))
    print("Press Ctrl+C to stop.")

    try:
        # In a real application you would interact with the nodes here:
        # e.g. send client commands to a leader once leader election is implemented.
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await cluster.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `example_cluster.py` in the project root (next to `pyproject.toml`),
activate your virtual environment, and run:

```bash
python example_cluster.py
```

As the Raft logic evolves, this example is a good place to experiment with
client commands and state machine behavior.
