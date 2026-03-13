import asyncio

import pytest

from raft.cluster import RaftCluster
from raft.node import Role
from raft.state_machine import DictStateMachine


async def wait_for_leader(cluster: RaftCluster, timeout: float = 3.0) -> str:
    """Wait until exactly one leader is present and return its node id."""
    deadline = asyncio.get_event_loop().time() + timeout

    while True:
        leaders: list[str] = []
        for node_id, node in cluster.node_map.items():
            if node.role == Role.LEADER:
                leaders.append(node_id)

        if len(leaders) == 1:
            return leaders[0]

        if asyncio.get_event_loop().time() >= deadline:
            raise TimeoutError("leader did not stabilize in time")

        await asyncio.sleep(0.05)


def set_cmd(key: str, value: object) -> dict:
    """Shorthand for a DictStateMachine 'set' command."""
    return {"op": "set", "key": key, "value": value}


async def wait_for_state_machine_sync(
    cluster: RaftCluster,
    key: str,
    expected_value: object,
    timeout: float = 3.0,
) -> None:
    """Wait until every running node's state machine has the expected value."""
    deadline = asyncio.get_event_loop().time() + timeout

    while True:
        all_match = True
        for node in cluster.node_map.values():
            if not node._running:
                continue
            sm = node.state_machine
            assert isinstance(sm, DictStateMachine)
            if sm.data.get(key) != expected_value:
                all_match = False
                break

        if all_match:
            return

        if asyncio.get_event_loop().time() >= deadline:
            states = {}
            for nid, n in cluster.node_map.items():
                assert isinstance(n.state_machine, DictStateMachine)
                states[nid] = n.state_machine.data.copy()
            raise TimeoutError(
                f"state machines did not converge: {states}"
            )

        await asyncio.sleep(0.05)


class TestReplication:

    async def test_single_command_replicated(self) -> None:
        """Submit one command; verify all nodes end up with the same state."""
        cluster = RaftCluster(["n1", "n2", "n3"])
        await cluster.start()

        await wait_for_leader(cluster)
        result = await asyncio.wait_for(
            cluster.submit(set_cmd("x", 1)),
            timeout=3.0,
        )
        assert result == 1

        await wait_for_state_machine_sync(cluster, "x", 1)

        for node in cluster.node_map.values():
            sm = node.state_machine
            assert isinstance(sm, DictStateMachine)
            assert sm.data["x"] == 1

        await cluster.stop()

    async def test_multiple_commands_in_order(self) -> None:
        """Submit several commands; verify identical ordering on all nodes."""
        cluster = RaftCluster(["n1", "n2", "n3"])
        await cluster.start()

        await wait_for_leader(cluster)

        commands = [
            set_cmd("a", 1),
            set_cmd("b", 2),
            set_cmd("c", 3),
            set_cmd("a", 10),
        ]
        for cmd in commands:
            await asyncio.wait_for(cluster.submit(cmd), timeout=3.0)

        await wait_for_state_machine_sync(cluster, "a", 10)

        expected = {"a": 10, "b": 2, "c": 3}
        for node in cluster.node_map.values():
            sm = node.state_machine
            assert isinstance(sm, DictStateMachine)
            assert sm.data == expected

        await cluster.stop()

    async def test_leader_failure_mid_replication(self) -> None:
        """Commit a command, kill the leader, verify the new leader has
        the entry and the cluster still accepts new commands."""
        cluster = RaftCluster(["n1", "n2", "n3"])
        await cluster.start()

        leader_id = await wait_for_leader(cluster)

        await asyncio.wait_for(
            cluster.submit(set_cmd("x", 42)),
            timeout=3.0,
        )

        # Kill the current leader.
        old_leader = cluster.node_map[leader_id]
        await old_leader.stop()
        del cluster.node_map[leader_id]

        # Wait for a new leader among survivors.
        new_leader_id = await wait_for_leader(cluster, timeout=5.0)
        assert new_leader_id != leader_id

        new_leader = cluster.node_map[new_leader_id]
        sm = new_leader.state_machine
        assert isinstance(sm, DictStateMachine)
        assert sm.data.get("x") == 42

        # The cluster should still accept new commands.
        await asyncio.wait_for(
            cluster.submit(set_cmd("y", 99)),
            timeout=5.0,
        )

        await wait_for_state_machine_sync(cluster, "y", 99, timeout=3.0)

        await cluster.stop()

    async def test_follower_catches_up(self) -> None:
        """Stop a follower, submit commands, restart it, verify it
        converges to the same state as the rest of the cluster."""
        cluster = RaftCluster(["n1", "n2", "n3"])
        await cluster.start()

        leader_id = await wait_for_leader(cluster)

        # Pick a follower to pause.
        follower_id = None
        for nid in cluster.node_map:
            if nid != leader_id:
                follower_id = nid
                break
        assert follower_id is not None

        follower = cluster.node_map[follower_id]
        await follower.stop()

        # Submit commands while the follower is down.
        await asyncio.wait_for(
            cluster.submit(set_cmd("a", 1)),
            timeout=3.0,
        )
        await asyncio.wait_for(
            cluster.submit(set_cmd("b", 2)),
            timeout=3.0,
        )

        # Restart the follower.
        await follower.start()

        # Give it time to replicate the missed entries.
        await wait_for_state_machine_sync(cluster, "a", 1, timeout=5.0)
        await wait_for_state_machine_sync(cluster, "b", 2, timeout=5.0)

        sm = follower.state_machine
        assert isinstance(sm, DictStateMachine)
        assert sm.data == {"a": 1, "b": 2}

        await cluster.stop()

    async def test_uncommitted_entry_not_applied(self) -> None:
        """Partition the leader from all followers.  A submitted command
        must NOT be applied to the state machine (no majority)."""
        cluster = RaftCluster(["n1", "n2", "n3"])
        await cluster.start()

        leader_id = await wait_for_leader(cluster)

        # Partition the leader from every follower.
        for nid in cluster.node_map:
            if nid != leader_id:
                cluster.transport.disconnect(leader_id, nid)

        leader = cluster.node_map[leader_id]

        # submit() blocks until committed, which will never happen here.
        # We launch it as a background task and verify it does NOT resolve.
        submit_task = asyncio.create_task(
            leader.submit(set_cmd("x", 123))
        )

        # Wait long enough for several heartbeat rounds to pass.
        await asyncio.sleep(0.5)

        assert not submit_task.done(), (
            "submit should not resolve without a replication majority"
        )

        # The leader's state machine must NOT have applied the entry.
        sm = leader.state_machine
        assert isinstance(sm, DictStateMachine)
        assert "x" not in sm.data

        submit_task.cancel()

        # Reconnect so stop() can drain cleanly.
        for nid in cluster.node_map:
            if nid != leader_id:
                cluster.transport.reconnect(leader_id, nid)

        await cluster.stop()
