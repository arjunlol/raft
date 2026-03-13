## Raft Consensus

A Raft consensus implementation in Python, built on `asyncio`. No external dependencies.

The demo simulates a distributed feature flag store (like you'd use to gate rollouts of firmware features) where nodes must agree on flag state even through failures.

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Tests

```bash
pytest -v
```

### Demo

```bash
python demo.py
```

Boots a 3-node cluster with slow timeouts so you can watch elections and replication happen in real time. Walks through: leader election, flag replication, leader failure, re-election, and follower catch-up.

### Usage

```python
import asyncio
from raft import RaftCluster

async def main():
    cluster = RaftCluster(["node-0", "node-1", "node-2"])
    await cluster.start()

    # submit a command — routed to the leader, replicated to a majority
    result = await cluster.submit({"op": "set", "key": "x", "value": 1})

    value = cluster.read("x")  # 1

    await cluster.stop()

asyncio.run(main())
```

### Architecture

- `node.py` — core Raft node (election, replication, commitment)
- `log.py` — append-only replicated log
- `messages.py` — Raft RPC types (RequestVote, AppendEntries)
- `transport.py` — in-memory message bus with partition simulation
- `state_machine.py` — pluggable state machine interface (ships with a key-value store)
- `cluster.py` — wires up a multi-node cluster

### What's implemented so far

- Leader election with randomized timeouts
- Log replication with consistency checks
- Commitment via majority acknowledgment
- Leader step-down on higher term
- Follower catch-up after partition/restart
