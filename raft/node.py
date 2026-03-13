import asyncio
import enum
import random
from typing import Optional

from raft.messages import (
    AppendEntries,
    AppendEntriesResponse,
    Message,
    RequestVote,
    RequestVoteResponse,
)
from raft.log import Log
from raft.state_machine import StateMachine
from raft.transport import InMemoryTransport


class Role(enum.Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    """A single node in a Raft cluster.

    Holds all per-node Raft state.
    """

    def __init__(
        self,
        node_id: str,
        peer_ids: list[str],
        transport: InMemoryTransport,
        state_machine: StateMachine,
        *,
        # 150ms - 300ms timeout range is recommended by the Raft paper... most optimal 10x network latency
        election_timeout_range_seconds: tuple[float, float] = (0.15, 0.30),
        heartbeat_interval_seconds: float = 0.05,
    ) -> None:
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.transport = transport
        self.state_machine = state_machine

        # --- Persistent Raft state ---
        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log: Log = Log()

        # --- Volatile Raft state ---
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.role: Role = Role.FOLLOWER

        self._running: bool = False
        self._loop_task: Optional[asyncio.Task[None]] = None

        # Timeouts/intervals are expressed in seconds (asyncio APIs expect seconds for their timeout parameters)
        self._election_timeout_range_seconds = election_timeout_range_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the node (election timers, message loop, etc.)."""
        self._running = True
        if self._loop_task is None or self._loop_task.done():
            self._loop_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Gracefully shut the node down."""
        self._running = False
        if self._loop_task is not None:
            await self._loop_task

    # ------------------------------------------------------------------
    # Internal main loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        """Main role-dispatch loop.

        Each iteration runs the behavior for the current role.  Role
        transitions happen inside the role-specific methods.
        """

        while self._running:
            if self.role == Role.FOLLOWER:
                await self._run_follower_loop()
            elif self.role == Role.CANDIDATE:
                await self._run_candidate_loop()
            elif self.role == Role.LEADER:
                await self._run_leader_loop()
            else:
                # Defensive: should never happen.
                self.role = Role.FOLLOWER

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _new_election_timeout_seconds(self) -> float:
        """Pick a random election timeout in seconds."""
        low, high = self._election_timeout_range_seconds
        return random.uniform(low, high)

    def _update_term_from_message(self, message: Message) -> None:
        """Apply Raft's term rule based on an incoming message.

        If the message term is greater than current_term, update
        current_term, clear voted_for, and step down to follower.
        """
        if message.term > self.current_term:
            self.current_term = message.term
            self.voted_for = None
            self.role = Role.FOLLOWER

    def _majority(self) -> int:
        """Number of votes needed for a majority (including self)."""
        total_nodes = 1 + len(self.peer_ids)
        return (total_nodes // 2) + 1

    # ------------------------------------------------------------------
    # Follower behavior
    # ------------------------------------------------------------------

    async def _run_follower_loop(self) -> None:
        """Follower: wait for messages or election timeout."""
        while self._running and self.role == Role.FOLLOWER:
            timeout_seconds = self._new_election_timeout_seconds()
            deadline = asyncio.get_event_loop().time() + timeout_seconds

            while self._running and self.role == Role.FOLLOWER:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    self.role = Role.CANDIDATE
                    return

                try:
                    message = await asyncio.wait_for(
                        self.transport.receive(self.node_id),
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    # Did not hear from a valid leader in time: start election.
                    self.role = Role.CANDIDATE
                    return

                should_reset_timer = await self._handle_message_as_follower(message)
                if should_reset_timer:
                    break  # break inner loop -> outer loop picks a fresh timeout

    async def _handle_message_as_follower(self, message: Message) -> bool:
        """Handle a single incoming message while in follower role.

        Returns True if the election timer should be reset (valid
        AppendEntries from current leader, or vote granted to a candidate).
        """

        self._update_term_from_message(message)

        if isinstance(message, AppendEntries):
            # Heartbeat or replication request from a leader.
            if message.term < self.current_term:
                # Leader is from an older term: reject.
                response = AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    match_index=0,
                    sender=self.node_id,
                )
                await self.transport.send(message.leader_id, response)
                return False

            response = AppendEntriesResponse(
                term=self.current_term,
                success=True,
                match_index=self.log.last_index,
                sender=self.node_id,
            )
            await self.transport.send(message.leader_id, response)
            return True

        if isinstance(message, RequestVote):
            # A follower grants its vote to a candidate only if **all** of:
            # 1) the candidate's term is at least as large as ours,
            # 2) we have not yet voted in this term (or we vote again for the same candidate),
            # 3) the candidate's log is at least as up-to-date as our own log.
            vote_granted = False

            if message.term < self.current_term:
                vote_granted = False
            else:
                candidate_log_at_least_up_to_date_as_follower = (
                    message.last_log_term > self.log.last_term
                    or (
                        message.last_log_term == self.log.last_term
                        and message.last_log_index >= self.log.last_index
                    )
                )

                if (
                    candidate_log_at_least_up_to_date_as_follower
                    and (self.voted_for is None or self.voted_for == message.candidate_id)
                ):
                    self.voted_for = message.candidate_id
                    vote_granted = True

            response = RequestVoteResponse(
                term=self.current_term,
                vote_granted=vote_granted,
                sender=self.node_id,
            )
            await self.transport.send(message.candidate_id, response)
            return vote_granted

        return False



    # ------------------------------------------------------------------
    # Candidate behavior
    # ------------------------------------------------------------------

    async def _run_candidate_loop(self) -> None:
        """Candidate: start an election and wait for a majority."""
        while self._running and self.role == Role.CANDIDATE:
            # Start a new election.
            self.current_term += 1
            self.voted_for = self.node_id
            # Per Raft spec: count each peer's vote at most once (no duplicate votes).
            peers_that_granted_vote: set[str] = set()

            # Send RequestVote RPCs to all peers.
            request = RequestVote(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=self.log.last_index,
                last_log_term=self.log.last_term,
                sender=self.node_id,
            )

            for peer_id in self.peer_ids:
                await self.transport.send(peer_id, request)

            # Wait for responses or timeout.
            election_timeout_seconds = self._new_election_timeout_seconds()
            deadline = asyncio.get_event_loop().time() + election_timeout_seconds

            while self._running and self.role == Role.CANDIDATE:
                if (1 + len(peers_that_granted_vote)) >= self._majority():
                    # We have a majority (self + distinct peers): become leader.
                    self.role = Role.LEADER
                    return

                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break

                try:
                    message = await asyncio.wait_for(
                        self.transport.receive(self.node_id),
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    # Election timed out without a winner: start a new election.
                    break

                # Apply term rule first.
                self._update_term_from_message(message)

                # A higher term could have turned us into a follower.
                if self.role == Role.FOLLOWER:
                    return

                # Step down if we see a valid leader in the same term.
                if isinstance(message, AppendEntries) and message.term == self.current_term:
                    self.role = Role.FOLLOWER
                    # Let follower loop handle subsequent messages.
                    return

                if isinstance(message, RequestVoteResponse):
                    if message.term < self.current_term:
                        continue
                    if message.term > self.current_term:
                        # Term rule already handled; we are follower now.
                        return
                    if message.vote_granted and message.sender not in peers_that_granted_vote:
                        peers_that_granted_vote.add(message.sender)

                # Other messages are ignored here.

    # ------------------------------------------------------------------
    # Leader behavior
    # ------------------------------------------------------------------

    async def _run_leader_loop(self) -> None:
        """Leader: send periodic heartbeats and watch for higher terms."""
        while self._running and self.role == Role.LEADER:
            await self._send_heartbeats()

            # After sending heartbeats, listen for messages for a short time.
            try:
                message = await asyncio.wait_for(
                    self.transport.receive(self.node_id),
                    timeout=self._heartbeat_interval_seconds,
                )
            except asyncio.TimeoutError:
                # No messages; send another round of heartbeats.
                continue

            self._update_term_from_message(message)

            if self.role == Role.FOLLOWER:
                # Stepped down due to higher term.
                return

            # If we see an AppendEntries in the same term from a different
            # leader, step down defensively.
            if isinstance(message, AppendEntries) and message.term == self.current_term:
                if message.leader_id != self.node_id:
                    self.role = Role.FOLLOWER
                    return

            if isinstance(message, RequestVote):
                # As leader for this term, we should not grant new votes.
                response = RequestVoteResponse(
                    term=self.current_term,
                    vote_granted=False,
                    sender=self.node_id,
                )
                await self.transport.send(message.candidate_id, response)

    async def _send_heartbeats(self) -> None:
        """Send an AppendEntries heartbeat to all peers."""
        for peer_id in self.peer_ids:
            heartbeat = AppendEntries(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=self.log.last_index,
                prev_log_term=self.log.last_term,
                entries=[],
                leader_commit=self.commit_index,
                sender=self.node_id,
            )
            await self.transport.send(peer_id, heartbeat)
