import pytest

from raft.cluster import RaftCluster


@pytest.fixture
def cluster() -> RaftCluster:
    """A 3-node cluster using the default DictStateMachine."""
    return RaftCluster(["node-0", "node-1", "node-2"])
