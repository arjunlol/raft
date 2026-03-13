import asyncio
from collections import defaultdict

import pytest

from raft.cluster import RaftCluster
from raft.log import LogEntry
from raft.messages import RequestVote, RequestVoteResponse
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

    async def test_follower_refuses_vote_when_candidate_log_less_up_to_date(self) -> None:
        """A follower must not grant a vote if the candidate's log is not at least as up-to-date as its own."""
        cluster = RaftCluster(["n1", "n2", "n3"])
        transport = cluster.transport

        # Give n1 (follower) a more up-to-date log: one entry at term 1, index 1.
        n1 = cluster.node_map["n1"]
        n1.log.append(LogEntry(term=1, index=1, command="x"))
        n1.current_term = 2
        n1.voted_for = None

        # Only start n1 so it runs the follower loop and we can inject a RequestVote.
        await n1.start()

        # Candidate n2 has empty log (last_log_index=0, last_log_term=0).
        request = RequestVote(
            term=2,
            candidate_id="n2",
            last_log_index=0,
            last_log_term=0,
            sender="n2",
        )
        await transport._node_queues_map["n1"].put(request)

        # Follower's response goes to the candidate's queue.
        response: RequestVoteResponse = await asyncio.wait_for(
            transport._node_queues_map["n2"].get(),
            timeout=1.0,
        )

        assert response.term == 2
        assert response.vote_granted is False, (
            "follower must not grant vote when candidate's log is less up-to-date"
        )

        await cluster.stop()

    async def test_candidate_does_not_count_duplicate_votes_from_same_peer(self) -> None:
        """A candidate must count each peer's vote at most once; duplicate responses must not be counted."""
        # 5 nodes -> majority = 3. So candidate needs self + 2 distinct peers to become leader.
        cluster = RaftCluster(["n1", "n2", "n3", "n4", "n5"])
        transport = cluster.transport

        # Only n1 runs (candidate). It will increment to term 1 and send RequestVotes; we inject responses.
        n1 = cluster.node_map["n1"]
        n1.role = Role.CANDIDATE

        await n1.start()

        # Grant from n2 only, but send the same vote three times (duplicate/retry). Use term 1 to match the election.
        for _ in range(3):
            await transport._node_queues_map["n1"].put(
                RequestVoteResponse(term=1, vote_granted=True, sender="n2")
            )

        # Give the candidate time to process all three messages.
        await asyncio.sleep(0.5)

        # With correct deduplication: n1 has 2 votes (self + n2) < majority 3 -> must not be leader.
        assert n1.role != Role.LEADER, (
            "candidate must not reach majority by counting the same peer's vote more than once"
        )

        await cluster.stop()

