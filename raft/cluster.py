from typing import Callable

from raft.node import RaftNode
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
