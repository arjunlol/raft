from dataclasses import dataclass

from raft.log import LogEntry


@dataclass
class RequestVote:
    """Sent by candidates to gather votes during an election."""

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    sender: str


@dataclass
class RequestVoteResponse:
    """Reply to a RequestVote RPC."""

    term: int
    vote_granted: bool
    sender: str


@dataclass
class AppendEntries:
    """Sent by the leader to replicate log entries and serve as heartbeat."""

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int
    sender: str


@dataclass
class AppendEntriesResponse:
    """Reply to an AppendEntries RPC."""

    term: int
    success: bool
    match_index: int
    sender: str


Message = RequestVote | RequestVoteResponse | AppendEntries | AppendEntriesResponse
