from dataclasses import dataclass
from typing import Any


@dataclass
class LogEntry:
    """A single entry in the Raft log."""

    term: int
    index: int
    command: Any


class Log:
    """1-indexed Raft log backed by a plain list.

    Index 0 is a sentinel so that prev_log_index/prev_log_term
    always have something to point at, even on an empty log.
    """

    def __init__(self) -> None:
        sentinel = LogEntry(term=0, index=0, command=None)
        self._entries: list[LogEntry] = [sentinel]

    def append(self, entry: LogEntry) -> None:
        """Add an entry to the end of the log."""
        self._entries.append(entry)

    def get(self, index: int) -> LogEntry:
        """Return the entry at the given 1-based index."""
        return self._entries[index]

    @property
    def last_index(self) -> int:
        """Index of the most recent entry (0 if log is empty)."""
        return len(self._entries) - 1

    @property
    def last_term(self) -> int:
        """Term of the most recent entry (0 if log is empty)."""
        return self._entries[-1].term

    def entries_from(self, index: int) -> list[LogEntry]:
        """Return all entries from the given index onward (inclusive)."""
        return self._entries[index:]

    def truncate_from(self, index: int) -> None:
        """Remove all entries from the given index onward (inclusive)."""
        del self._entries[index:]
