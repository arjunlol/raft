"""Sleep feature flag store — powered by Raft consensus.

Simulates a distributed feature flag service where multiple nodes
must agree on which flags are enabled. Shows leader election,
replication, node failure, and catch-up recovery.

    python demo.py
"""

import asyncio
import logging
import sys

from raft import RaftCluster, Role
from raft.state_machine import DictStateMachine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)


async def wait_for_leader(cluster: RaftCluster, timeout: float = 10.0) -> str:
    """Poll until exactly one running node considers itself leader."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        for nid, node in cluster.node_map.items():
            if node._running and node.role == Role.LEADER:
                return nid
        await asyncio.sleep(0.2)
    raise TimeoutError("no leader elected in time")


async def main() -> None:
    cluster = RaftCluster(
        ["node-0", "node-1", "node-2"],
        election_timeout_range_seconds=(1.5, 3.0),
        heartbeat_interval_seconds=0.5,
    )

    # ── 1. Boot the cluster ──────────────────────────────────────
    print("\n--- Starting 3-node feature flag store ---\n")
    await cluster.start()

    leader_id = await wait_for_leader(cluster)
    leader = cluster.node_map[leader_id]
    print(f"Leader elected: {leader_id} (term={leader.current_term})")
    await asyncio.sleep(1)

    # ── 2. Set initial feature flags ─────────────────────────────
    print("\n--- Rolling out feature flags ---\n")

    flags = [
        ("adaptive_cooling", True),
        ("sleep_staging_v2", False),
        ("vibration_alarm", True),
        ("autopilot_temp", True),
    ]
    for name, enabled in flags:
        await asyncio.wait_for(
            leader.submit({"op": "set", "key": name, "value": enabled}),
            timeout=5.0,
        )
        print(f"  {name} -> {'enabled' if enabled else 'disabled'}")
        await asyncio.sleep(0.3)

    await asyncio.sleep(1)
    print("\nFlag state across all nodes:")
    for nid, node in cluster.node_map.items():
        sm: DictStateMachine = node.state_machine  # type: ignore[assignment]
        print(f"  {nid}: {sm.data}")
    await asyncio.sleep(1)

    # ── 3. Kill the leader ───────────────────────────────────────
    print("\n--- Simulating node failure ---\n")

    old_leader_id = leader_id
    old_leader = cluster.node_map[old_leader_id]
    await old_leader.stop()
    print(f"Stopped {old_leader_id}")

    await asyncio.sleep(0.5)
    print("Waiting for new leader...")

    new_leader_id = await wait_for_leader(cluster)
    new_leader = cluster.node_map[new_leader_id]
    print(f"New leader: {new_leader_id} (term={new_leader.current_term})")
    await asyncio.sleep(1)

    # ── 4. Update a flag on the new leader ───────────────────────
    print("\n--- Enabling sleep_staging_v2 on new leader ---\n")

    await asyncio.wait_for(
        new_leader.submit({"op": "set", "key": "sleep_staging_v2", "value": True}),
        timeout=5.0,
    )
    print("  sleep_staging_v2 -> enabled")
    await asyncio.sleep(1)

    print("\nFlag state on surviving nodes:")
    for nid, node in cluster.node_map.items():
        if not node._running:
            continue
        sm: DictStateMachine = node.state_machine  # type: ignore[assignment]
        print(f"  {nid}: {sm.data}")
    await asyncio.sleep(1)

    # ── 5. Restart the failed node ───────────────────────────────
    print("\n--- Restarting failed node ---\n")

    print(f"Restarting {old_leader_id}...")
    old_leader.role = Role.FOLLOWER
    await old_leader.start()
    await asyncio.sleep(3)

    print("Flag state after catch-up:")
    for nid, node in cluster.node_map.items():
        sm: DictStateMachine = node.state_machine  # type: ignore[assignment]
        print(f"  {nid}: {sm.data}")

    print("\n--- Done ---\n")
    await cluster.stop()


if __name__ == "__main__":
    asyncio.run(main())
