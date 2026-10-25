"""Unit and Integration Tests for Step 13: Independent State-Transition Verification.

Verifies:
1. Empty state produces deterministic genesis fingerprint (GENESIS_STATE_HASH).
2. Exact parity with raft.node.apply_command_to_kv_store across all supported command types.
3. Fully applied consistent nodes evaluate to STATE_CONSISTENT.
4. Application lag is classified as APPLICATION_LAG with lag count, not divergence.
5. State mismatch correctly flags STATE_DIVERGENT and lists differing/missing/unexpected keys.
6. Offline nodes evaluate to UNAVAILABLE.
7. Missing leader results in non-authoritative NO_LEADER status.
8. Non-serializable state machine payloads fail safely with ERROR.
9. last_applied > commit_index produces an explicit invariant violation ERROR.
10. Applied progress exceeding available log length produces an ERROR.
11. Replay deep-copies mutable values, protecting original command and log objects from mutation.
12. Committed ledger divergence at or before last_applied flags LEDGER_DIVERGENT.
13. Identical final KV states cannot override a committed history divergence (reports LEDGER_DIVERGENT).
14. GET /api/state/verification returns valid schema and HTTP 200.
15. Integration tests use temporary RAFT_WAL_DIR isolation.
"""

from __future__ import annotations

import copy
import os
import sys
import time
from typing import Any, Dict, List

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from fastapi.testclient import TestClient

from monitor import app
from raft.node import apply_command_to_kv_store
from raft.state import LogEntry, NodeRole, RaftState
from raft.verifier import (
    ClusterStateStatus,
    GENESIS_STATE_HASH,
    NodeStateStatus,
    StateTransitionVerifier,
    apply_command_to_state,
    compute_state_hash,
    diff_states,
)


class MockValidatorNode:
    """Mock node mimicking RaftNode state for deterministic state machine testing."""

    def __init__(
        self,
        node_id: str,
        role: NodeRole = NodeRole.FOLLOWER,
        commit_index: int = 0,
        last_applied: int = 0,
        log: List[LogEntry] = None,
        kv_store: Dict[str, Any] = None,
        is_running: bool = True,
    ) -> None:
        self.node_id = node_id
        self.role = role
        self._is_running = is_running
        self.state = RaftState(
            commit_index=commit_index,
            last_applied=last_applied,
            log=list(log) if log is not None else [],
            kv_store=dict(kv_store) if kv_store is not None else {},
        )


# =============================================================================
# 1. Deterministic Unit Tests: Genesis, Hashes & Parity
# =============================================================================

def test_empty_state_and_deterministic_genesis_fingerprint() -> None:
    """Genesis empty state produces exact fixed SHA-256 fingerprint."""
    empty_state: Dict[str, Any] = {}
    h, err = compute_state_hash(empty_state)
    assert err is None
    assert h == GENESIS_STATE_HASH
    assert h == "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"


def test_parity_with_existing_command_semantics_all_formats() -> None:
    """Verifier command transitions produce 100% identical outputs to raft.node for all formats."""
    test_commands = [
        # Dict SET variants
        {"op": "SET", "key": "k1", "val": "v1"},
        {"op": "set", "k": "k2", "value": 123},
        {"key": "k3", "val": {"nested": "data"}},  # Default op is SET
        # Dict DELETE variants
        {"op": "DELETE", "key": "k1"},
        {"op": "DEL", "k": "k2"},
        # Dict NOOP
        {"op": "NOOP"},
        # String SET/PUT with '='
        "SET str_k1 = str_v1",
        "PUT str_k2=str_v2",
        # String SET/PUT with whitespace
        "SET str_k3 str_v3",
        "PUT str_k4 str_v4",
        # Bare 'key=value'
        "bare_key = bare_val",
        # Unsupported / unknown commands (should be no-op)
        "UNKNOWN_OP test",
        {"op": "UNKNOWN_OP", "key": "ignore_me"},
        12345,
        None,
    ]

    kv_node: Dict[str, Any] = {}
    kv_verifier: Dict[str, Any] = {}

    for cmd in test_commands:
        apply_command_to_kv_store(kv_node, cmd)
        apply_command_to_state(kv_verifier, cmd)
        assert kv_verifier == kv_node, f"Mismatch on command: {cmd}"


def test_deep_copy_prevents_command_and_log_mutation() -> None:
    """Replay deep-copies mutable values, protecting original command and log from mutation."""
    original_dict = {"nested_field": "initial_value"}
    cmd = {"op": "SET", "key": "data", "val": original_dict}
    entry = LogEntry(index=1, term=1, command=cmd)

    states, hashes, err = StateTransitionVerifier.replay_committed_log([entry], up_to_index=1)
    assert err is None
    replayed_state = states[1]

    # Mutate the replayed state dictionary
    replayed_state["data"]["nested_field"] = "mutated_value"

    # Original command object inside entry must remain pristine
    assert entry.command["val"]["nested_field"] == "initial_value"
    assert original_dict["nested_field"] == "initial_value"


# =============================================================================
# 2. State Comparison & Diagnostic Tests
# =============================================================================

def test_nodes_fully_applied_are_state_consistent() -> None:
    """Running nodes with matching log and last_applied == commit_index are STATE_CONSISTENT."""
    log = [
        LogEntry(index=1, term=1, command={"key": "k1", "val": "v1"}),
        LogEntry(index=2, term=1, command={"key": "k2", "val": "v2"}),
    ]
    expected_kv = {"k1": "v1", "k2": "v2"}

    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=2, last_applied=2, log=log, kv_store=expected_kv),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=2, last_applied=2, log=log, kv_store=expected_kv),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    assert report.cluster_state_status == ClusterStateStatus.VERIFIED
    assert report.is_authoritative is True
    assert report.nodes["node1"].status == NodeStateStatus.STATE_CONSISTENT
    assert report.nodes["node2"].status == NodeStateStatus.STATE_CONSISTENT
    assert report.summary["consistent_count"] == 2
    assert report.summary["divergent_count"] == 0


def test_application_lag_is_not_misclassified_as_divergence() -> None:
    """Node that applied only prefix entries is APPLICATION_LAG with correct lag, not divergent."""
    log = [
        LogEntry(index=1, term=1, command={"key": "k1", "val": "v1"}),
        LogEntry(index=2, term=1, command={"key": "k2", "val": "v2"}),
    ]
    # Leader has applied both entries
    kv_leader = {"k1": "v1", "k2": "v2"}
    # Follower has committed both, but only applied entry 1
    kv_lagging = {"k1": "v1"}

    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=2, last_applied=2, log=log, kv_store=kv_leader),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=2, last_applied=1, log=log, kv_store=kv_lagging),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    assert report.cluster_state_status == ClusterStateStatus.DEGRADED_LAG
    n2_rep = report.nodes["node2"]
    assert n2_rep.status == NodeStateStatus.APPLICATION_LAG
    assert n2_rep.application_lag == 1
    assert n2_rep.differing_keys == []
    assert report.summary["lagging_count"] == 1
    assert report.summary["divergent_count"] == 0


def test_state_mismatch_with_differing_key_diagnostics() -> None:
    """State discrepancy flags STATE_DIVERGENT and pinpoints differing keys."""
    log = [LogEntry(index=1, term=1, command={"key": "k1", "val": "v1"})]
    kv_corrupted = {"k1": "TAMPERED_VAL", "extra_key": "unexpected"}

    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=1, last_applied=1, log=log, kv_store={"k1": "v1"}),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=1, last_applied=1, log=log, kv_store=kv_corrupted),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    assert report.cluster_state_status == ClusterStateStatus.DIVERGENT
    n2 = report.nodes["node2"]
    assert n2.status == NodeStateStatus.STATE_DIVERGENT
    assert "k1" in n2.differing_keys
    assert "extra_key" in n2.unexpected_keys


def test_offline_node_is_unavailable() -> None:
    """Killed node reports UNAVAILABLE without failing cluster verification."""
    log = [LogEntry(index=1, term=1, command="k=v")]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=1, last_applied=1, log=log, kv_store={"k": "v"}),
        "node2": MockValidatorNode("node2", is_running=False),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    assert report.nodes["node2"].status == NodeStateStatus.UNAVAILABLE
    assert report.summary["unavailable_count"] == 1


def test_missing_leader_results_in_non_authoritative_no_leader() -> None:
    """When no active leader exists, cluster_state_status is NO_LEADER and is_authoritative is False."""
    log = [LogEntry(index=1, term=1, command="k=v")]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.CANDIDATE, commit_index=1, last_applied=1, log=log, kv_store={"k": "v"}),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=1, last_applied=1, log=log, kv_store={"k": "v"}),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id=None)
    assert report.cluster_state_status == ClusterStateStatus.NO_LEADER
    assert report.is_authoritative is False
    assert report.reference_type == "PROVISIONAL"


def test_non_serializable_state_fails_safely_as_error() -> None:
    """State containing non-serializable object returns ERROR without crashing."""
    class BadObject:
        pass

    log = [LogEntry(index=1, term=1, command="k=v")]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=1, last_applied=1, log=log, kv_store={"bad": BadObject()}),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    assert report.cluster_state_status == ClusterStateStatus.ERROR
    assert report.nodes["node1"].status == NodeStateStatus.ERROR
    assert "serialization failure" in report.nodes["node1"].error


def test_last_applied_greater_than_commit_index_invariant_error() -> None:
    """Node with last_applied > commit_index violates invariant and reports ERROR."""
    log = [LogEntry(index=1, term=1, command="k=v")]
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=1, last_applied=1, log=log, kv_store={"k": "v"}),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=0, last_applied=1, log=log, kv_store={"k": "v"}),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    assert report.nodes["node2"].status == NodeStateStatus.ERROR
    assert "last_applied (1) exceeds commit_index (0)" in report.nodes["node2"].error


def test_applied_progress_exceeding_available_log_error() -> None:
    """Node reporting last_applied > len(log) reports invariant ERROR."""
    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=0, last_applied=0, log=[], kv_store={}),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=1, last_applied=1, log=[], kv_store={}),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    assert report.nodes["node2"].status == NodeStateStatus.ERROR
    assert "exceeds available log length" in report.nodes["node2"].error


def test_ledger_divergence_flags_ledger_divergent() -> None:
    """Committed ledger conflict at or before last_applied flags LEDGER_DIVERGENT."""
    log_leader = [
        LogEntry(index=1, term=1, command="k=v1"),
        LogEntry(index=2, term=1, command="k=v2"),
    ]
    log_divergent = [
        LogEntry(index=1, term=1, command="k=v1"),
        LogEntry(index=2, term=2, command="k=CONFLICT"),
    ]

    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=2, last_applied=2, log=log_leader, kv_store={"k": "v2"}),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=2, last_applied=2, log=log_divergent, kv_store={"k": "CONFLICT"}),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    assert report.cluster_state_status == ClusterStateStatus.DIVERGENT
    assert report.nodes["node2"].status == NodeStateStatus.LEDGER_DIVERGENT
    assert "Ledger conflict" in report.nodes["node2"].error


def test_identical_final_kv_with_divergent_history_cannot_override_ledger_divergence() -> None:
    """Even if final KV store is identical, committed log divergence forces LEDGER_DIVERGENT."""
    # Leader overwrites k=final in entry 2
    log_leader = [
        LogEntry(index=1, term=1, command={"op": "SET", "key": "k", "val": "temp_a"}),
        LogEntry(index=2, term=1, command={"op": "SET", "key": "k", "val": "final"}),
    ]
    # Follower had a different command at index 1, but entry 2 set k to the same final value
    log_divergent = [
        LogEntry(index=1, term=2, command={"op": "SET", "key": "k", "val": "temp_b"}),
        LogEntry(index=2, term=2, command={"op": "SET", "key": "k", "val": "final"}),
    ]

    nodes = {
        "node1": MockValidatorNode("node1", role=NodeRole.LEADER, commit_index=2, last_applied=2, log=log_leader, kv_store={"k": "final"}),
        "node2": MockValidatorNode("node2", role=NodeRole.FOLLOWER, commit_index=2, last_applied=2, log=log_divergent, kv_store={"k": "final"}),
    }

    report = StateTransitionVerifier.verify_cluster_state(nodes, active_leader_id="node1")
    # Must be DIVERGENT at cluster level, and node2 must be LEDGER_DIVERGENT
    assert report.cluster_state_status == ClusterStateStatus.DIVERGENT
    assert report.nodes["node2"].status == NodeStateStatus.LEDGER_DIVERGENT


# =============================================================================
# 3. Integration Tests: Live FastAPI Endpoint & WAL Isolation
# =============================================================================

@pytest.fixture(autouse=True)
def run_around_tests(tmp_path, monkeypatch):
    """Ensure clean cluster state and isolated temporary WAL storage."""
    test_wal = str(tmp_path / "server_wal")
    monkeypatch.setenv("RAFT_WAL_DIR", test_wal)
    yield


def test_api_state_verification_endpoint_schema_and_http_success() -> None:
    """GET /api/state/verification returns HTTP 200 and complete schema."""
    with TestClient(app) as client:
        time.sleep(0.8)  # Allow leader election
        resp = client.get("/api/state/verification")
        assert resp.status_code == 200
        data = resp.json()

        assert "timestamp" in data
        assert "cluster_state_status" in data
        assert "is_authoritative" in data
        assert "reference_type" in data
        assert "reference_node" in data
        assert "reference_commit_index" in data
        assert "reference_applied_index" in data
        assert "reference_state_hash" in data
        assert "nodes" in data
        assert "summary" in data

        for nid, n_data in data["nodes"].items():
            assert "node_id" in n_data
            assert "status" in n_data
            assert "commit_index" in n_data
            assert "last_applied" in n_data
            assert "state_hash" in n_data
            assert "commit_lag" in n_data
            assert "application_lag" in n_data
            assert "differing_keys" in n_data
            assert "is_running" in n_data


def test_api_state_verification_tracks_live_commands() -> None:
    """Submitting state machine commands updates reference state hash and verifies state."""
    with TestClient(app) as client:
        time.sleep(0.8)

        # 1. Read initial state verification
        v0 = client.get("/api/state/verification").json()
        assert v0["cluster_state_status"] == "VERIFIED"
        assert v0["is_authoritative"] is True
        initial_hash = v0["reference_state_hash"]

        # 2. Submit a write command
        cmd_resp = client.post("/api/command", json={"key": "state_test_k", "val": "state_test_v"})
        assert cmd_resp.status_code == 200
        assert cmd_resp.json()["success"] is True

        # 3. Read updated state verification
        v1 = client.get("/api/state/verification").json()
        assert v1["cluster_state_status"] == "VERIFIED"
        assert v1["reference_state_hash"] != initial_hash

        # 4. Snapshot also includes state_verification
        snap = client.get("/api/cluster").json()
        assert "state_verification" in snap
        assert snap["state_verification"]["cluster_state_status"] == "VERIFIED"
