import pytest

from raft.cluster import RaftCluster
from raft.log import Log, LogEntry
from raft.node import Role
from raft.state_machine import DictStateMachine


# ── Log ──────────────────────────────────────────────────────────────


class TestLog:
    def test_empty_log_has_sentinel(self) -> None:
        log = Log()
        assert log.last_index == 0
        assert log.last_term == 0

    def test_append_and_get(self) -> None:
        log = Log()
        entry = LogEntry(term=1, index=1, command="x")
        log.append(entry)

        assert log.get(1) is entry
        assert log.last_index == 1
        assert log.last_term == 1

    def test_multiple_appends(self) -> None:
        log = Log()
        log.append(LogEntry(term=1, index=1, command="a"))
        log.append(LogEntry(term=1, index=2, command="b"))
        log.append(LogEntry(term=2, index=3, command="c"))

        assert log.last_index == 3
        assert log.last_term == 2

    def test_entries_from(self) -> None:
        log = Log()
        log.append(LogEntry(term=1, index=1, command="a"))
        log.append(LogEntry(term=1, index=2, command="b"))
        log.append(LogEntry(term=2, index=3, command="c"))

        entries = log.entries_from(2)
        assert len(entries) == 2
        assert entries[0].command == "b"
        assert entries[1].command == "c"

    def test_truncate_from(self) -> None:
        log = Log()
        log.append(LogEntry(term=1, index=1, command="a"))
        log.append(LogEntry(term=1, index=2, command="b"))
        log.append(LogEntry(term=2, index=3, command="c"))

        log.truncate_from(2)

        assert log.last_index == 1
        assert log.get(1).command == "a"


# ── DictStateMachine ────────────────────────────────────────────────


class TestDictStateMachine:
    def test_set_and_get(self) -> None:
        sm = DictStateMachine()
        result = sm.apply({"op": "set", "key": "x", "value": 42})
        assert result == 42
        assert sm.apply({"op": "get", "key": "x"}) == 42

    def test_get_missing_key(self) -> None:
        sm = DictStateMachine()
        assert sm.apply({"op": "get", "key": "nope"}) is None

    def test_delete(self) -> None:
        sm = DictStateMachine()
        sm.apply({"op": "set", "key": "x", "value": 1})
        deleted = sm.apply({"op": "delete", "key": "x"})
        assert deleted == 1
        assert sm.apply({"op": "get", "key": "x"}) is None

    def test_unknown_op_raises(self) -> None:
        sm = DictStateMachine()
        with pytest.raises(ValueError, match="Unknown operation"):
            sm.apply({"op": "explode"})


# ── Cluster ─────────────────────────────────────────────────────────


class TestCluster:
    async def test_boot_and_shutdown(self, cluster: RaftCluster) -> None:
        await cluster.start()

        for node in cluster.node_map.values():
            assert node._running is True
            assert node.role == Role.FOLLOWER
            assert node.current_term == 0

        await cluster.stop()

        for node in cluster.node_map.values():
            assert node._running is False

    def test_peer_wiring(self, cluster: RaftCluster) -> None:
        for node_id, node in cluster.node_map.items():
            assert node_id not in node.peer_ids
            assert len(node.peer_ids) == 2

