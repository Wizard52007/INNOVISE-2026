"""Performance Metrics Engine for ConsensusLab.

Tracks:
- Confirmed committed transaction counts and confirmation latency (ms).
- Failed / rejected command submissions.
- Rolling-window transaction throughput (TPS).
- Failover recovery count and duration (ms).
"""

from __future__ import annotations

import collections
import time
from typing import Any, Deque, Dict, Optional, Tuple


class ConsensusMetrics:
    """In-memory telemetry and performance measurement collector."""

    def __init__(self, window_seconds: float = 10.0) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds: float = window_seconds

        # Transaction Counters
        self.committed_transactions: int = 0
        self.failed_submissions: int = 0

        # Latency Tracking
        self.total_latency_ms: float = 0.0
        self.last_latency_ms: Optional[float] = None

        # Rolling Window History: deque of (monotonic_timestamp, latency_ms)
        self._commits: Deque[Tuple[float, float]] = collections.deque()

        # Recovery Tracking
        self.recovery_count: int = 0
        self.last_recovery_duration_ms: Optional[float] = None
        self._recovery_start_perf: Optional[float] = None
        self._lost_leader_id: Optional[str] = None

    def record_commit(self, latency_ms: float, timestamp: Optional[float] = None) -> None:
        """Record a confirmed committed transaction and its latency."""
        ts = timestamp if timestamp is not None else time.monotonic()
        self.committed_transactions += 1
        self.total_latency_ms += latency_ms
        self.last_latency_ms = round(latency_ms, 2)
        self._commits.append((ts, latency_ms))

    def record_rejection(self) -> None:
        """Record a failed or rejected transaction submission."""
        self.failed_submissions += 1

    def get_throughput(self, now: Optional[float] = None) -> float:
        """Calculate throughput (TPS) across the rolling measurement window."""
        current_time = now if now is not None else time.monotonic()
        cutoff = current_time - self.window_seconds

        # Prune expired commits older than window
        while self._commits and self._commits[0][0] < cutoff:
            self._commits.popleft()

        if not self._commits:
            return 0.0

        return round(len(self._commits) / self.window_seconds, 2)

    def get_average_latency(self) -> Optional[float]:
        """Return average confirmation latency of committed transactions in ms."""
        if self.committed_transactions == 0:
            return None
        return round(self.total_latency_ms / self.committed_transactions, 2)

    def is_recovering(self) -> bool:
        """Return True if an unresolved leader loss interval is actively being measured."""
        return self._recovery_start_perf is not None

    def record_recovery_start(self, leader_id: str, start_time: Optional[float] = None) -> None:
        """Mark the beginning of a leader failure and recovery interval."""
        if self._recovery_start_perf is None:
            self._recovery_start_perf = start_time if start_time is not None else time.perf_counter()
            self._lost_leader_id = leader_id

    def record_recovery_end(self, new_leader_id: str, end_time: Optional[float] = None) -> Optional[float]:
        """Mark the completion of leader recovery when a new leader is recognized."""
        if self._recovery_start_perf is None:
            return None

        t_end = end_time if end_time is not None else time.perf_counter()
        duration_ms = max(0.0, (t_end - self._recovery_start_perf) * 1000.0)
        self.recovery_count += 1
        self.last_recovery_duration_ms = round(duration_ms, 2)

        # Clear recovery state
        self._recovery_start_perf = None
        self._lost_leader_id = None
        return self.last_recovery_duration_ms

    def reset(self) -> None:
        """Reset all metrics to initial state."""
        self.committed_transactions = 0
        self.failed_submissions = 0
        self.total_latency_ms = 0.0
        self.last_latency_ms = None
        self._commits.clear()
        self.recovery_count = 0
        self.last_recovery_duration_ms = None
        self._recovery_start_perf = None
        self._lost_leader_id = None

    def to_dict(self, now: Optional[float] = None) -> Dict[str, Any]:
        """Return serializable dictionary of all current metrics."""
        return {
            "committed_transactions": self.committed_transactions,
            "failed_submissions": self.failed_submissions,
            "last_latency_ms": self.last_latency_ms,
            "average_latency_ms": self.get_average_latency(),
            "throughput_tps": self.get_throughput(now=now),
            "window_seconds": self.window_seconds,
            "recovery_count": self.recovery_count,
            "last_recovery_duration_ms": self.last_recovery_duration_ms,
        }
