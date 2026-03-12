import asyncio
from typing import Any


class InMemoryTransport:
    """Message bus that shuttles messages between nodes via asyncio queues.

    No real networking — everything stays in-process.
    A set of broken links lets tests simulate network partitions.
    """

    def __init__(self) -> None:
        self._node_queues_map: dict[str, asyncio.Queue[Any]] = {}
        self._disconnected_node_pairs: set[tuple[str, str]] = set()

    def register(self, node_id: str) -> None:
        """Create a receive queue for a node."""
        self._node_queues_map[node_id] = asyncio.Queue()

    async def send(self, to: str, message: Any) -> None:
        """Deliver a message to a node's queue (dropped if link is broken)."""
        link = set({message.sender, to})
        if link in self._disconnected_node_pairs:
            return

        queue = self._node_queues_map.get(to)
        if queue is not None:
            await queue.put(message)

    async def receive(self, node_id: str) -> Any:
        """Wait for the next message addressed to this node."""
        return await self._node_queues_map[node_id].get()

    def disconnect(self, a: str, b: str) -> None:
        """Break the link between two nodes (messages silently dropped)."""
        self._disconnected_node_pairs.add(set({a, b}))

    def reconnect(self, a: str, b: str) -> None:
        """Restore a previously broken link."""
        self._disconnected_node_pairs.discard(set({a, b}))
