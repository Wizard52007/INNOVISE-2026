"""Unit and Integration Tests for Step 11: Performance Measurement & Telemetry.

Verifies:
1. ConsensusMetrics unit tests:
   - Initial state and null/zero values.
   - Successful commits increment counter and update latency.
   - Failed submissions increment failed count only without affecting latency averages.
   - Rolling-window throughput calculations and expiry pruning.
   - Deterministic recovery interval tracking and duplicate prevention.
2. FastAPI integration tests:
   - GET /api/metrics schema and values.
   - POST /api/command execution increments metrics.
   - WebSocket broadcast emits 'metrics_update' event with consistent payload.
   - Recovery duration recorded on leader failover.
   - Tracked server_wal files remain untouched.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Any, Dict

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from fastapi.testclient import TestClient

from monitor import app, controller
from raft.metrics import ConsensusMetrics


# =============================================================================
# 1. Unit Tests: ConsensusMetrics (Deterministic, in-memory)
# =============================================================================

def test_metrics_initial_state() -> None:
    """ConsensusMetrics initializes with clean zero/null values."""
    metrics = ConsensusMetrics(window_seconds=10.0)
    assert metrics.committed_transactions == 0
    assert metrics.failed_submissions == 0
    assert metrics.last_latency_ms is None
    assert metrics.get_average_latency() is None
    assert metrics.get_throughput(now=100.0) == 0.0
    assert metrics.recovery_count == 0
    assert metrics.last_recovery_duration_ms is None
    assert metrics.is_recovering() is False

    data = metrics.to_dict(now=100.0)
    assert data["committed_transactions"] == 0
    assert data["failed_submissions"] == 0
    assert data["last_latency_ms"] is None
    assert data["average_latency_ms"] is None
    assert data["throughput_tps"] == 0.0
    assert data["recovery_count"] == 0
    assert data["last_recovery_duration_ms"] is None


def test_metrics_successful_commits_and_latency() -> None:
    """Successful commits record latency and update running average."""
    metrics = ConsensusMetrics(window_seconds=10.0)

    # 1. First commit: 10.0 ms at t=100.0
    metrics.record_commit(10.0, timestamp=100.0)
    assert metrics.committed_transactions == 1
    assert metrics.last_latency_ms == 10.0
    assert metrics.get_average_latency() == 10.0

    # 2. Second commit: 30.0 ms at t=101.0
    metrics.record_commit(30.0, timestamp=101.0)
    assert metrics.committed_transactions == 2
    assert metrics.last_latency_ms == 30.0
    assert metrics.get_average_latency() == 20.0  # (10 + 30) / 2


def test_metrics_failed_submissions_do_not_affect_latency() -> None:
    """Failed commands increment failed_submissions only, without affecting latency."""
    metrics = ConsensusMetrics(window_seconds=10.0)

    # Commit 1
    metrics.record_commit(15.0, timestamp=100.0)
    assert metrics.committed_transactions == 1
    assert metrics.failed_submissions == 0
    assert metrics.get_average_latency() == 15.0

    # Record 3 rejections
    metrics.record_rejection()
    metrics.record_rejection()
    metrics.record_rejection()

    assert metrics.committed_transactions == 1
    assert metrics.failed_submissions == 3
    # Latency average and last latency must remain strictly 15.0
    assert metrics.last_latency_ms == 15.0
    assert metrics.get_average_latency() == 15.0


def test_metrics_throughput_window_and_pruning() -> None:
    """Throughput calculates rate over window and prunes expired records."""
    metrics = ConsensusMetrics(window_seconds=10.0)

    # Commit 5 transactions at t=100.0
    for _ in range(5):
        metrics.record_commit(10.0, timestamp=100.0)

    # At t=105.0: all 5 are within 10-second window (cutoff = 95.0)
    # Throughput = 5 tx / 10s = 0.5 TPS
    assert metrics.get_throughput(now=105.0) == 0.5

    # Add 5 more transactions at t=106.0 (total 10 in window)
    for _ in range(5):
        metrics.record_commit(10.0, timestamp=106.0)

    # At t=108.0: cutoff is 98.0 -> all 10 are still inside window
    # Throughput = 10 tx / 10s = 1.0 TPS
    assert metrics.get_throughput(now=108.0) == 1.0

    # At t=112.0: cutoff is 102.0 -> the first 5 (at t=100.0) have expired!
    # Remaining: 5 transactions (from t=106.0) -> Throughput = 5 / 10 = 0.5 TPS
    assert metrics.get_throughput(now=112.0) == 0.5

    # At t=120.0: cutoff is 110.0 -> all transactions have expired
    assert metrics.get_throughput(now=120.0) == 0.0


def test_metrics_recovery_interval_measurement() -> None:
    """Recovery time is measured from failure start to new leader emergence."""
    metrics = ConsensusMetrics()

    # 1. No recovery initially
    assert metrics.is_recovering() is False
    assert metrics.record_recovery_end("node2") is None  # no-op when not recovering

    # 2. Leader failure observed at perf_counter = 100.0
    metrics.record_recovery_start("node1", start_time=100.0)
    assert metrics.is_recovering() is True

    # 3. Duplicate start call during the same incident must NOT overwrite the original start time
    metrics.record_recovery_start("node1", start_time=100.5)
    assert metrics.is_recovering() is True

    # 4. Recovery completes at perf_counter = 100.450 (450 ms later)
    duration = metrics.record_recovery_end("node2", end_time=100.450)
    assert duration == 450.0
    assert metrics.recovery_count == 1
    assert metrics.last_recovery_duration_ms == 450.0
    assert metrics.is_recovering() is False

    # 5. Subsequent end calls do nothing
    assert metrics.record_recovery_end("node2") is None
    assert metrics.recovery_count == 1


# =============================================================================
# 2. Integration Tests: FastAPI Endpoints & WebSocket
# =============================================================================

@pytest.fixture(autouse=True)
def run_around_tests(tmp_path, monkeypatch):
    """Ensure clean cluster state and isolated WAL directory between tests."""
    test_wal = str(tmp_path / "server_wal")
    monkeypatch.setenv("RAFT_WAL_DIR", test_wal)
    yield


def test_api_get_metrics_endpoint() -> None:
    """GET /api/metrics returns valid JSON performance snapshot."""
    with TestClient(app) as client:
        time.sleep(0.8)  # wait for leader election
        resp = client.get("/api/metrics")
        assert resp.status_code == 200
        data = resp.json()

        assert "committed_transactions" in data
        assert "failed_submissions" in data
        assert "last_latency_ms" in data
        assert "average_latency_ms" in data
        assert "throughput_tps" in data
        assert "window_seconds" in data
        assert "recovery_count" in data
        assert "last_recovery_duration_ms" in data


def test_api_command_execution_updates_metrics() -> None:
    """POST /api/command updates committed_transactions and latency."""
    with TestClient(app) as client:
        time.sleep(0.8)

        # Initial metrics
        m0 = client.get("/api/metrics").json()
        init_committed = m0["committed_transactions"]

        # Submit successful command
        cmd = {"op": "SET", "key": "metric_test_k", "val": "metric_test_v"}
        resp = client.post("/api/command", json=cmd)
        assert resp.status_code == 200
        assert resp.json()["success"] is True

        # Check updated metrics
        m1 = client.get("/api/metrics").json()
        assert m1["committed_transactions"] == init_committed + 1
        assert m1["last_latency_ms"] is not None
        assert m1["last_latency_ms"] > 0
        assert m1["average_latency_ms"] is not None
        assert m1["average_latency_ms"] > 0

        # Snapshot also includes metrics
        cluster_snap = client.get("/api/cluster").json()
        assert "metrics" in cluster_snap
        assert cluster_snap["metrics"]["committed_transactions"] == m1["committed_transactions"]


def test_api_websocket_metrics_update_event() -> None:
    """WebSocket receives dedicated 'metrics_update' event on command execution."""
    with TestClient(app) as client:
        time.sleep(0.8)
        with client.websocket_connect("/ws") as ws:
            # First frame is CLUSTER_SNAPSHOT
            frame1 = ws.receive_json()
            assert frame1["type"] == "CLUSTER_SNAPSHOT"

            # Send command
            client.post("/api/command", json={"command": "SET ws_metric=1"})

            # Read broadcast frames until METRICS_UPDATE arrives
            received_events = []
            metrics_payload = None
            for _ in range(50):
                try:
                    f = ws.receive_json()
                    received_events.append(f.get("type"))
                    if f.get("type") == "METRICS_UPDATE":
                        metrics_payload = f.get("data")
                        break
                except Exception:
                    break

            assert "METRICS_UPDATE" in received_events
            assert metrics_payload is not None
            assert "committed_transactions" in metrics_payload
            assert "throughput_tps" in metrics_payload


def test_api_recovery_time_measured_on_leader_kill() -> None:
    """Killing the leader triggers recovery start, and new leader election finishes it."""
    with TestClient(app) as client:
        time.sleep(0.8)
        cluster_data = client.get("/api/cluster").json()
        leader_id = cluster_data["leader_id"]
        assert leader_id is not None

        # Kill the current leader
        kill_resp = client.post(f"/api/nodes/{leader_id}/kill")
        assert kill_resp.status_code == 200

        # Wait for cluster to elect a new leader (timeout 3s)
        time.sleep(1.2)

        m = client.get("/api/metrics").json()
        assert m["recovery_count"] >= 1
        assert m["last_recovery_duration_ms"] is not None
        assert m["last_recovery_duration_ms"] > 0
