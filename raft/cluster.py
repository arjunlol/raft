from typing import Any, Callable

from raft.node import RaftNode, Role
from raft.state_machine import DictStateMachine, StateMachine
from raft.transport import InMemoryTransport


class RaftCluster:
    """Convenience wrapper that wires up a cluster"""

    def __init__(
        self,
        node_ids: list[str],
        state_machine_factory: Callable[[], StateMachine] | None = None,
    ) -> None:
        self.transport = InMemoryTransport()
        self.node_map: dict[str, RaftNode] = {}

        if len(node_ids) % 2 == 0:
            msg = "RaftCluster requires an odd number of nodes"
            raise ValueError(msg)

        if state_machine_factory is None:
            state_machine_factory = DictStateMachine

        for node_id in node_ids:
            self.transport.register(node_id)

            peer_ids = []
            for nid in node_ids:
                if nid != node_id:
                    peer_ids.append(nid)

            state_machine = state_machine_factory()
            node = RaftNode(node_id, peer_ids, self.transport, state_machine)
            self.node_map[node_id] = node

    async def start(self) -> None:
        """Start every node in the cluster."""
        for node in self.node_map.values():
            await node.start()

    async def stop(self) -> None:
        """Stop every node in the cluster."""
        for node in self.node_map.values():
            await node.stop()

    def get_leader(self) -> RaftNode:
        """Return the current leader node, or raise if there is none."""
        for node in self.node_map.values():
            if node.role == Role.LEADER:
                return node
        raise RuntimeError("no leader available")

    async def submit(self, command: Any) -> Any:
        """Submit a command to the current leader."""
        leader = self.get_leader()
        return await leader.submit(command)

    def read(self, key: str) -> Any:
        """Read a key from the leader's state machine (testing convenience)."""
        leader = self.get_leader()
        return leader.state_machine.apply({"op": "get", "key": key})
