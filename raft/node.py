import asyncio
import enum
import random
from typing import Any, Optional

from raft.messages import (
    AppendEntries,
    AppendEntriesResponse,
    Message,
    RequestVote,
    RequestVoteResponse,
)
from raft.log import Log, LogEntry
from raft.state_machine import StateMachine
from raft.transport import InMemoryTransport


class Role(enum.Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class NotLeaderError(Exception):
    """Raised when a command is submitted to a non-leader node."""


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

        # --- Volatile Raft state (all nodes) ---
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.role: Role = Role.FOLLOWER

        # --- Volatile Raft state (leader only, initialized on election) ---
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # --- Internal bookkeeping ---
        self._pending_futures: dict[int, asyncio.Future[Any]] = {}
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
    # Command submission (leader only)
    # ------------------------------------------------------------------

    async def submit(self, command: Any) -> Any:
        """Submit a command to the replicated log.

        Only the leader may accept commands.  Returns the state-machine
        result once the entry has been committed by a majority.
        """
        if self.role != Role.LEADER:
            raise NotLeaderError(f"{self.node_id} is not the leader")

        entry = LogEntry(
            term=self.current_term,
            index=self.log.last_index + 1,
            command=command,
        )
        self.log.append(entry)

        future: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._pending_futures[entry.index] = future

        return await future

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

    def _apply_committed_entries(self) -> None:
        """Apply all committed-but-not-yet-applied entries to the state machine.

        On the leader, resolves any pending client futures with the
        state-machine result.
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            result = self.state_machine.apply(entry.command)

            future = self._pending_futures.pop(self.last_applied, None)
            if future is not None and not future.done():
                future.set_result(result)

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

        # ---- AppendEntries (heartbeat or log replication) ----
        if isinstance(message, AppendEntries):
            return await self._handle_append_entries(message)

        # ---- RequestVote ----
        if isinstance(message, RequestVote):
            # A follower grants its vote to a candidate only if **all** of:
            # 1) the candidate's term is at least as large as ours,
            # 2) we have not yet voted in this term (or we vote again for the same candidate),
            # 3) the candidate's log is at least as up-to-date as our own log.
            vote_granted = False

            if message.term >= self.current_term:
                candidate_log_ok = (
                    message.last_log_term > self.log.last_term
                    or (
                        message.last_log_term == self.log.last_term
                        and message.last_log_index >= self.log.last_index
                    )
                )
                not_yet_voted = (
                    self.voted_for is None
                    or self.voted_for == message.candidate_id
                )

                if candidate_log_ok and not_yet_voted:
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

    async def _handle_append_entries(self, message: AppendEntries) -> bool:
        """Process an AppendEntries RPC as a follower.

        Implements the receiver logic from Figure 2 of the Raft paper.
        Returns True if the election timer should be reset.
        """
        # Step 1: Reject if the leader's term is stale.
        if message.term < self.current_term:
            response = AppendEntriesResponse(
                term=self.current_term,
                success=False,
                match_index=0,
                sender=self.node_id,
            )
            await self.transport.send(message.leader_id, response)
            return False

        # Step 2: Check that our log contains an entry at prev_log_index
        # with a term matching prev_log_term.
        if message.prev_log_index > self.log.last_index:
            response = AppendEntriesResponse(
                term=self.current_term,
                success=False,
                match_index=self.log.last_index,
                sender=self.node_id,
            )
            await self.transport.send(message.leader_id, response)
            return True  # valid leader, reset election timer

        if self.log.get(message.prev_log_index).term != message.prev_log_term:
            response = AppendEntriesResponse(
                term=self.current_term,
                success=False,
                match_index=0,
                sender=self.node_id,
            )
            await self.transport.send(message.leader_id, response)
            return True  # valid leader, reset election timer

        # Step 3: Walk through new entries one by one.  If we have a
        # conflicting entry (same index, different term), truncate from
        # that point, then append the new entry.
        insert_index = message.prev_log_index + 1
        for entry in message.entries:
            if insert_index <= self.log.last_index:
                existing = self.log.get(insert_index)
                if existing.term != entry.term:
                    self.log.truncate_from(insert_index)
                    self.log.append(entry)
                # else: already have this entry, no action needed
            else:
                self.log.append(entry)
            insert_index += 1

        # Step 4: Advance commit_index based on leader's commit_index.
        if message.leader_commit > self.commit_index:
            self.commit_index = min(message.leader_commit, self.log.last_index)

        # Step 5: Apply newly committed entries.
        self._apply_committed_entries()

        # Step 6: Respond with success.
        response = AppendEntriesResponse(
            term=self.current_term,
            success=True,
            match_index=self.log.last_index,
            sender=self.node_id,
        )
        await self.transport.send(message.leader_id, response)
        return True

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
        """Leader: replicate log entries and maintain authority."""

        # Initialize volatile leader state (Raft paper, Figure 2).
        # next_index: optimistic — assume every peer is up to date.
        # match_index: conservative — we don't know what they have yet.
        for peer_id in self.peer_ids:
            self.next_index[peer_id] = self.log.last_index + 1
            self.match_index[peer_id] = 0

        while self._running and self.role == Role.LEADER:
            await self._send_append_entries()

            # Process messages until the next heartbeat is due.
            deadline = asyncio.get_event_loop().time() + self._heartbeat_interval_seconds

            while self._running and self.role == Role.LEADER:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break

                try:
                    message = await asyncio.wait_for(
                        self.transport.receive(self.node_id),
                        timeout=remaining,
                    )
                except asyncio.TimeoutError:
                    break

                self._update_term_from_message(message)
                if self.role == Role.FOLLOWER:
                    return

                if isinstance(message, AppendEntries) and message.term == self.current_term:
                    if message.leader_id != self.node_id:
                        self.role = Role.FOLLOWER
                        return

                if isinstance(message, AppendEntriesResponse):
                    self._handle_append_entries_response(message)

                if isinstance(message, RequestVote):
                    response = RequestVoteResponse(
                        term=self.current_term,
                        vote_granted=False,
                        sender=self.node_id,
                    )
                    await self.transport.send(message.candidate_id, response)

            # Apply anything that was committed during this round.
            self._apply_committed_entries()

    async def _send_append_entries(self) -> None:
        """Send AppendEntries RPCs to all peers.

        For each peer, sends all log entries from next_index[peer]
        onward (empty list acts as a heartbeat).
        """
        for peer_id in self.peer_ids:
            next_idx = self.next_index.get(peer_id, self.log.last_index + 1)
            prev_log_index = next_idx - 1
            prev_log_term = self.log.get(prev_log_index).term
            entries = self.log.entries_from(next_idx)

            message = AppendEntries(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index,
                sender=self.node_id,
            )
            await self.transport.send(peer_id, message)

    def _handle_append_entries_response(self, message: AppendEntriesResponse) -> None:
        """Process a follower's response to our AppendEntries."""
        peer_id = message.sender

        if message.success:
            self.next_index[peer_id] = message.match_index + 1
            self.match_index[peer_id] = message.match_index
            self._advance_commit_index()
        else:
            # Log inconsistency: back off by one and retry next round.
            self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)

    def _advance_commit_index(self) -> None:
        """Check whether commit_index can advance (leader only).

        Find the highest N where N > commit_index, a majority of
        match_index[peer] >= N, and log[N].term == current_term.
        The current_term check is critical — Raft leaders only commit
        entries from their own term (§5.4.2).
        """
        for n in range(self.commit_index + 1, self.log.last_index + 1):
            if self.log.get(n).term != self.current_term:
                continue

            # Count how many nodes have this entry (including self).
            replication_count = 1
            for peer_id in self.peer_ids:
                if self.match_index.get(peer_id, 0) >= n:
                    replication_count += 1

            if replication_count >= self._majority():
                self.commit_index = n
