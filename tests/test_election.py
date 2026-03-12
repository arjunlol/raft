import asyncio
from collections import defaultdict

import pytest

from raft.cluster import RaftCluster
from raft.node import Role


async def wait_for_leader(cluster: RaftCluster, timeout: float = 3.0) -> tuple[str, int]:
    """Wait until exactly one leader is present, or raise TimeoutError."""
    deadline = asyncio.get_event_loop().time() + timeout

    while True:
        leaders: list[tuple[str, int]] = []
        for node_id, node in cluster.node_map.items():
            if node.role == Role.LEADER:
                leaders.append((node_id, node.current_term))

        if len(leaders) == 1:
            return leaders[0]

        if asyncio.get_event_loop().time() >= deadline:
            raise TimeoutError("leader did not stabilize in time")

        await asyncio.sleep(0.05)


class TestElection:
    async def test_leader_elected(self) -> None:
        cluster = RaftCluster(["n1", "n2", "n3"])
        await cluster.start()

        leader_id, term = await wait_for_leader(cluster)

        assert leader_id in cluster.node_map
        assert term >= 1

        followers = [
            node_id
            for node_id, node in cluster.node_map.items()
            if node.role == Role.FOLLOWER
        ]
        assert len(followers) == 2

        await cluster.stop()

    async def test_leader_reelected_after_failure(self) -> None:
        cluster = RaftCluster(["n1", "n2", "n3"])
        await cluster.start()

        first_leader_id, first_term = await wait_for_leader(cluster)

        # Stop the current leader.
        leader_node = cluster.node_map[first_leader_id]
        await leader_node.stop()

        # Remove from the cluster's view to simulate failure.
        del cluster.node_map[first_leader_id]

        # Wait for a new leader among the remaining nodes.
        new_leader_id, new_term = await wait_for_leader(cluster)

        assert new_leader_id != first_leader_id
        assert new_term > first_term

        await cluster.stop()

    async def test_follower_resets_timer_on_heartbeat(self) -> None:
        # Use small timeouts so the test completes quickly.
        node_ids = ["n1", "n2", "n3"]
        cluster = RaftCluster(node_ids)

        # Tighten timeouts on all nodes.
        for node in cluster.node_map.values():
            node._election_timeout_range_seconds = (0.2, 0.3)
            node._heartbeat_interval_seconds = 0.05

        await cluster.start()

        leader_id, _ = await wait_for_leader(cluster)

        # Record current roles.
        roles_before = {node_id: node.role for node_id, node in cluster.node_map.items()}

        # Let some time pass while heartbeats are flowing.
        await asyncio.sleep(0.6)

        # No follower should have started its own election (still exactly one leader).
        leaders = [
            node_id
            for node_id, node in cluster.node_map.items()
            if node.role == Role.LEADER
        ]
        assert len(leaders) == 1
        assert leaders[0] == leader_id

        # Followers remain followers.
        for node_id, role in roles_before.items():
            if node_id != leader_id:
                assert cluster.node_map[node_id].role == Role.FOLLOWER

        await cluster.stop()

    async def test_split_vote_resolves(self) -> None:
        cluster = RaftCluster(["n1", "n2", "n3", "n4", "n5"])
        await cluster.start()

        # Let elections run for a while; randomized timeouts should
        # eventually produce a single leader.
        leader_id, term = await wait_for_leader(cluster, timeout=5.0)

        assert leader_id in cluster.node_map
        assert term >= 1

        await cluster.stop()

    async def test_only_one_leader_per_term(self) -> None:
        cluster = RaftCluster(["n1", "n2", "n3", "n4", "n5"])
        await cluster.start()

        # Allow some elections and possible re-elections to occur.
        await asyncio.sleep(3.0)

        # Snapshot current roles and terms.
        leaders_by_term: dict[int, list[str]] = defaultdict(list)
        for node_id, node in cluster.node_map.items():
            if node.role == Role.LEADER:
                leaders_by_term[node.current_term].append(node_id)

        # No term should have more than one leader in this snapshot.
        for term, leader_ids in leaders_by_term.items():
            assert len(leader_ids) == 1, f"term {term} has leaders {leader_ids}"

        await cluster.stop()

