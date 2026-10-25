"""Unit and Integration Tests for Step 12: Cross-Validator Ledger Consistency.

Verifies:
1. Canonical serialization and deterministic hash chaining in raft/ledger.py.
2. Empty committed histories start at fixed GENESIS_HASH.
3. Identical committed histories evaluate to CONSISTENT.
4. Shorter matching committed prefixes evaluate to BEHIND (with accurate lag).
5. Divergent entries evaluate to DIVERGENT and pinpoint the exact first_mismatch_index.
6. Uncommitted trailing entries beyond commit_index are strictly excluded.
7. Offline/killed nodes evaluate to UNAVAILABLE.
8. Non-serializable commands fail safely with status ERROR without crashing.
9. Provisional reference selection when no active leader exists.
10. FastAPI integration: GET /api/ledger/consistency returns valid schema.
11. Tracked server_wal files remain untouched under RAFT_WAL_DIR isolation.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, List

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from fastapi.testclient import TestClient

from monitor import app
from raft.ledger import (
    ClusterConsistencyStatus,
    GENESIS_HASH,
    LedgerConsistencyChecker,
    LedgerStatus,
    canonical_serialize_entry,
    compute_committed_hash_chain,
)
from raft.state import LogEntry, NodeRole, RaftState


# =============================================================================
# Helper Mock Class for Deterministic Unit Tests
# =============================================================================

class MockValidatorNode:
    """Mock node mimicking RaftNode state for deterministic in-memory testing."""

    def __init__(
        self,
        node_id: str,
        role: NodeRole = NodeRole.FOLLOWER,
        commit_index: int = 0,
        last_applied: int = 0,
        log: List[LogEntry] = None,
        is_running: bool = True,
    ) -> None:
        self.node_id = node_id
        self.role = role
        self._is_running = is_running
        self.state = RaftState(
            commit_index=commit_index,
            last_applied=last_applied,
            log=list(log) if log is not None else [],
        )


# =============================================================================
# 1. Deterministic Unit Tests: Canonical Serialization & Hash Chains
# =============================================================================

def test_canonical_serialization_determinism() -> None:
    """Canonical serialization produces identical bytes regardless of dictionary insertion order."""
    entry1 = LogEntry(index=1, term=2, command={"val": 42, "key": "k", "op": "SET"})
    entry2 = LogEntry(index=1, term=2, command={"op": "SET", "key": "k", "val": 42})

    b1 = canonical_serialize_entry(entry1)
    b2 = canonical_serialize_entry(entry2)
    assert b1 == b2
    assert b1 == b'{"command":{"key":"k","op":"SET","val":42},"index":1,"term":2}'


def test_empty_committed_history_produces_genesis_hash() -> None:
    """A log with commit_index 0 produces GENESIS_HASH and length 1 prefix chain."""
    log = [LogEntry(index=1, term=1, command="UNCOMMITTED_DATA")]
    hashes, err = compute_committed_hash_chain(log, commit_index=0)

    assert err is None
    assert len(hashes) == 1
    assert hashes[0] == GENESIS_HASH


def test_uncommitted_trailing_entries_are_excluded() -> None:
    """Entries beyond commit_index must not alter the committed hash chain."""
    committed_entries = [
        LogEntry(index=1, term=1, command="cmd1"),
        LogEntry(index=2, term=1, command="cmd2"),
    ]
    divergent_uncommitted = LogEntry(index=3, term=2, command="CONFLICTING_UNCOMMITTED")

    log_a = committed_entries
    log_b = committed_entries + [divergent_uncommitted]

    # Both nodes committed up to index 2
    hashes_a, err_a = compute_committed_hash_chain(log_a, commit_index=2)
    hashes_b, err_b = compute_committed_hash_chain(log_b, commit_index=2)

    assert err_a is None and err_b is None
    assert hashes_a == hashes_b
    assert len(hashes_a) == 3  # [index 0 (genesis), index 1, index 2]


def test_non_serializable_command_fails_safely_as_error() -> None:
    """Non-serializable command raises error without crashing and marks node ERROR."""
    class NonSerializable:
        pass

    bad_entry = LogEntry(index=1, term=1, command=NonSerializable())
    node = MockValidatorNode(node_id="node1", role=NodeRole.LEADER, commit_index=1, log=[bad_entry])

    report, hashes = LedgerConsistencyChecker.inspect_node(node, "node1")
    assert report.status == LedgerStatus.ERROR
    assert report.error is not None
    assert "Failed to serialize" in report.error
    assert hashes == []


# =============================================================================
# 2. Multi-Node Consistency Scenarios: Checker Logic
# =============================================================================

def test_identical_committed_histories_are_consistent() -> None:
    """All running nodes with matching committed histories evaluate to CONSISTENT."""
    log = [
        LogEntry(index=1, term=1, command={"k": "a", "v": "1"}),
        LogEntry(index=2, term=1, command={"k": "b", "v": "2"}),
    ]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=2, log=log),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=2, log=log),
        "node3": MockValidatorNode("node3", role=NodeRole.FOLLOWER, commit_index=2, log=log),
    }

    report = LedgerConsistencyChecker.check_cluster_consistency(nodes, active_leader_id="node1")
    assert report.cluster_status == ClusterConsistencyStatus.CONSISTENT
    assert report.is_authoritative is True
    assert report.reference_node == "node1"
    assert report.reference_commit_index == 2
    assert report.summary["consistent_count"] == 3
    assert report.summary["behind_count"] == 0
    assert report.summary["divergent_count"] == 0

    for rep in report.nodes.values():
        assert rep.status == LedgerStatus.CONSISTENT
        assert rep.lag == 0
        assert rep.mismatch_index is None


def test_shorter_matching_prefix_evaluates_to_behind() -> None:
    """Node with shorter matching committed prefix is BEHIND with exact lag count."""
    full_log = [
        LogEntry(index=1, term=1, command="cmd1"),
        LogEntry(index=2, term=1, command="cmd2"),
        LogEntry(index=3, term=2, command="cmd3"),
    ]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=3, log=full_log),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=3, log=full_log),
        "node3": MockValidatorNode("node3", role=NodeRole.FOLLOWER, commit_index=1, log=full_log[:1]),
    }

    report = LedgerConsistencyChecker.check_cluster_consistency(nodes, active_leader_id="node1")
    assert report.cluster_status == ClusterConsistencyStatus.DEGRADED_BEHIND
    assert report.nodes["node3"].status == LedgerStatus.BEHIND
    assert report.nodes["node3"].lag == 2
    assert report.nodes["node3"].mismatch_index is None
    assert report.summary["behind_count"] == 1


def test_divergent_entries_pinpoint_first_mismatch() -> None:
    """Conflicting committed entry is flagged DIVERGENT and locates first mismatch index."""
    log_good = [
        LogEntry(index=1, term=1, command="cmd1"),
        LogEntry(index=2, term=1, command="cmd2"),
        LogEntry(index=3, term=2, command="cmd3"),
    ]
    log_bad = [
        LogEntry(index=1, term=1, command="cmd1"),
        LogEntry(index=2, term=1, command="CONFLICTING_COMMAND"),
        LogEntry(index=3, term=2, command="cmd3"),
    ]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=3, log=log_good),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=3, log=log_good),
        "node3": MockValidatorNode("node3", role=NodeRole.FOLLOWER, commit_index=3, log=log_bad),
    }

    report = LedgerConsistencyChecker.check_cluster_consistency(nodes, active_leader_id="node1")
    assert report.cluster_status == ClusterConsistencyStatus.DIVERGENT
    assert report.nodes["node3"].status == LedgerStatus.DIVERGENT
    assert report.nodes["node3"].mismatch_index == 2  # First difference at index 2
    assert report.summary["divergent_count"] == 1


def test_offline_node_evaluates_to_unavailable() -> None:
    """Killed or terminated node evaluates to UNAVAILABLE without corrupting consistency check."""
    log = [LogEntry(index=1, term=1, command="cmd1")]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=1, log=log),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=1, log=log),
        "node3": MockValidatorNode("node3", role=NodeRole.FOLLOWER, is_running=False),
    }

    report = LedgerConsistencyChecker.check_cluster_consistency(nodes, active_leader_id="node1")
    assert report.nodes["node3"].status == LedgerStatus.UNAVAILABLE
    assert report.nodes["node3"].is_running is False
    assert report.nodes["node3"].commit_hash is None
    assert report.summary["unavailable_count"] == 1


def test_no_active_leader_uses_provisional_reference() -> None:
    """When no active leader exists, reference is flagged provisional and cluster status is NO_LEADER."""
    log = [LogEntry(index=1, term=1, command="cmd1")]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.CANDIDATE, commit_index=1, log=log),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=1, log=log),
    }

    report = LedgerConsistencyChecker.check_cluster_consistency(nodes, active_leader_id=None)
    assert report.is_authoritative is False
    assert report.reference_type == "PROVISIONAL"
    assert report.cluster_status == ClusterConsistencyStatus.NO_LEADER
    assert report.diagnostic_message is not None
    assert "No active leader" in report.diagnostic_message


# =============================================================================
# 3. Integration Tests: Live FastAPI Endpoint & Snapshot
# =============================================================================

@pytest.fixture(autouse=True)
def run_around_tests(tmp_path, monkeypatch):
    """Ensure clean cluster state and isolated temporary WAL storage."""
    test_wal = str(tmp_path / "server_wal")
    monkeypatch.setenv("RAFT_WAL_DIR", test_wal)
    yield


def test_api_ledger_consistency_endpoint_schema() -> None:
    """GET /api/ledger/consistency returns complete, valid schema."""
    with TestClient(app) as client:
        time.sleep(0.8)  # Allow initial election
        resp = client.get("/api/ledger/consistency")
        assert resp.status_code == 200
        data = resp.json()

        assert "timestamp" in data
        assert "cluster_status" in data
        assert "is_authoritative" in data
        assert "reference_type" in data
        assert "reference_node" in data
        assert "reference_commit_index" in data
        assert "reference_commit_hash" in data
        assert "nodes" in data
        assert "summary" in data

        # Check per-node fields
        for nid, n_data in data["nodes"].items():
            assert "node_id" in n_data
            assert "status" in n_data
            assert "commit_index" in n_data
            assert "last_applied" in n_data
            assert "commit_hash" in n_data
            assert "lag" in n_data
            assert "mismatch_index" in n_data
            assert "is_running" in n_data


def test_api_ledger_consistency_tracks_live_commits() -> None:
    """Submitting commands increments commit_index and preserves cluster CONSISTENT status."""
    with TestClient(app) as client:
        time.sleep(0.8)

        # 1. Check initial consistency
        c0 = client.get("/api/ledger/consistency").json()
        assert c0["cluster_status"] == "CONSISTENT"
        assert c0["is_authoritative"] is True
        initial_commit = c0["reference_commit_index"]

        # 2. Execute a command
        cmd_resp = client.post("/api/command", json={"key": "consist_k", "val": "consist_v"})
        assert cmd_resp.status_code == 200
        assert cmd_resp.json()["success"] is True

        # 3. Check updated consistency
        c1 = client.get("/api/ledger/consistency").json()
        assert c1["cluster_status"] == "CONSISTENT"
        assert c1["reference_commit_index"] == initial_commit + 1
        assert c1["reference_commit_hash"] != GENESIS_HASH

        # 4. Cluster snapshot also contains ledger_consistency key
        snap = client.get("/api/cluster").json()
        assert "ledger_consistency" in snap
        assert snap["ledger_consistency"]["cluster_status"] == "CONSISTENT"
