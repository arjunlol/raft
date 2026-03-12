import enum

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

    async def start(self) -> None:
        """Start the node (election timers, message loop, etc.)."""
        self._running = True

    async def stop(self) -> None:
        """Gracefully shut the node down."""
        self._running = False
