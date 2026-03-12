from abc import ABC, abstractmethod
from typing import Any


class StateMachine(ABC):
    """Interface that consumers implement to define application behavior."""

    @abstractmethod
    def apply(self, command: Any) -> Any:
        """Apply a committed command and return a result."""
        ...


class DictStateMachine(StateMachine):
    """Simple key-value store"""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def apply(self, command: Any) -> Any:
        """Apply a dict command like {"op": "set", "key": "x", "value": 1}."""
        op = command["op"]

        if op == "set":
            self.data[command["key"]] = command["value"]
            return command["value"]

        if op == "get":
            return self.data.get(command["key"])

        if op == "delete":
            return self.data.pop(command["key"], None)

        raise ValueError(f"Unknown operation: {op}")
