"""Committed Ledger Consistency and Hash-Chain Verification Engine.

Provides:
- Deterministic canonical serialization of committed Raft log entries.
- SHA-256 recursive hash chain calculation for committed log entries up to commit_index.
- Multi-node cross-validator consistency checker distinguishing:
  CONSISTENT, BEHIND, DIVERGENT, AHEAD, UNAVAILABLE, and ERROR.
- Authoritative vs. provisional reference selection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from raft.state import LogEntry, NodeRole

logger = logging.getLogger(__name__)

# Fixed genesis hash for an empty committed ledger history (commit_index == 0)
GENESIS_HASH = hashlib.sha256(b"GENESIS:ConsensusLab:PS3").hexdigest()


class LedgerStatus(str, Enum):
    """Validation status of an individual validator node's committed ledger."""

    CONSISTENT = "CONSISTENT"
    BEHIND = "BEHIND"
    DIVERGENT = "DIVERGENT"
    AHEAD = "AHEAD"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


class ClusterConsistencyStatus(str, Enum):
    """Overall consistency status across the cluster."""

    CONSISTENT = "CONSISTENT"
    DEGRADED_BEHIND = "DEGRADED_BEHIND"
    DIVERGENT = "DIVERGENT"
    ANOMALY_AHEAD = "ANOMALY_AHEAD"
    NO_LEADER = "NO_LEADER"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


def canonical_serialize_entry(entry: LogEntry) -> bytes:
    """Deterministically serialize a LogEntry to UTF-8 bytes.

    Includes index, term, and command using sorted keys and compact separators.
    Raises TypeError if command cannot be serialized to JSON.
    """
    payload = {
        "command": entry.command,
        "index": entry.index,
        "term": entry.term,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def compute_committed_hash_chain(
    log: List[LogEntry],
    commit_index: int,
) -> Tuple[List[str], Optional[str]]:
    """Compute the recursive SHA-256 hash chain over committed entries up to commit_index.

    Returns:
        (prefix_hashes, error_message)
        prefix_hashes[0] is GENESIS_HASH (commit_index 0).
        prefix_hashes[i] is the cumulative hash at committed index i.
        If serialization fails, prefix_hashes will be empty and error_message will be populated.
    """
    safe_commit_limit = max(0, min(commit_index, len(log)))
    hashes: List[str] = [GENESIS_HASH]
    current_hash = GENESIS_HASH

    for i in range(safe_commit_limit):
        entry = log[i]
        try:
            entry_bytes = canonical_serialize_entry(entry)
        except Exception as exc:
            err = f"Failed to serialize entry #{entry.index} (term {entry.term}): {exc}"
            logger.warning(err)
            return [], err

        current_hash = hashlib.sha256(current_hash.encode("ascii") + entry_bytes).hexdigest()
        hashes.append(current_hash)

    return hashes, None


@dataclass
class NodeConsistencyReport:
    """Consistency evaluation of an individual validator node."""

    node_id: str
    status: LedgerStatus
    is_running: bool
    commit_index: int
    last_applied: int
    commit_hash: Optional[str] = None
    lag: Optional[int] = None
    mismatch_index: Optional[int] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "is_running": self.is_running,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "commit_hash": self.commit_hash,
            "lag": self.lag,
            "mismatch_index": self.mismatch_index,
            "error": self.error,
        }


@dataclass
class ClusterConsistencyReport:
    """Full cluster consistency report across all validators."""

    timestamp: float
    cluster_status: ClusterConsistencyStatus
    is_authoritative: bool
    reference_type: str  # "AUTHORITATIVE", "PROVISIONAL", or "NONE"
    reference_node: Optional[str]
    reference_commit_index: Optional[int]
    reference_commit_hash: Optional[str]
    nodes: Dict[str, NodeConsistencyReport] = field(default_factory=dict)
    summary: Dict[str, int] = field(default_factory=dict)
    diagnostic_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "cluster_status": self.cluster_status.value,
            "is_authoritative": self.is_authoritative,
            "reference_type": self.reference_type,
            "reference_node": self.reference_node,
            "reference_commit_index": self.reference_commit_index,
            "reference_commit_hash": self.reference_commit_hash,
            "nodes": {nid: rep.to_dict() for nid, rep in self.nodes.items()},
            "summary": self.summary,
            "diagnostic_message": self.diagnostic_message,
        }


class LedgerConsistencyChecker:
    """Cross-validator committed-ledger consistency verification engine."""

    @staticmethod
    def inspect_node(node: Any, node_id: str) -> Tuple[NodeConsistencyReport, List[str]]:
        """Inspect a single node's state and return its report and prefix hash chain.

        Handles live RaftNode instances, persistent states, or mock test objects.
        """
        is_running = getattr(node, "_is_running", True) if node is not None else False
        if not is_running or node is None:
            return (
                NodeConsistencyReport(
                    node_id=node_id,
                    status=LedgerStatus.UNAVAILABLE,
                    is_running=False,
                    commit_index=0,
                    last_applied=0,
                    commit_hash=None,
                    lag=None,
                    mismatch_index=None,
                    error="Node is offline or terminated",
                ),
                [],
            )

        state = getattr(node, "state", None)
        if state is None:
            return (
                NodeConsistencyReport(
                    node_id=node_id,
                    status=LedgerStatus.ERROR,
                    is_running=True,
                    commit_index=0,
                    last_applied=0,
                    error="Node state object missing",
                ),
                [],
            )

        commit_index = max(0, getattr(state, "commit_index", 0))
        last_applied = max(0, getattr(state, "last_applied", 0))
        log = getattr(state, "log", [])

        prefix_hashes, err = compute_committed_hash_chain(log, commit_index)
        if err is not None:
            return (
                NodeConsistencyReport(
                    node_id=node_id,
                    status=LedgerStatus.ERROR,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    error=err,
                ),
                [],
            )

        final_hash = prefix_hashes[-1] if prefix_hashes else GENESIS_HASH
        report = NodeConsistencyReport(
            node_id=node_id,
            status=LedgerStatus.CONSISTENT,  # Provisional, will be compared against reference
            is_running=True,
            commit_index=commit_index,
            last_applied=last_applied,
            commit_hash=final_hash,
            lag=0,
            mismatch_index=None,
            error=None,
        )
        return report, prefix_hashes

    @classmethod
    def check_cluster_consistency(
        cls,
        nodes: Dict[str, Any],
        active_leader_id: Optional[str] = None,
        now: Optional[float] = None,
    ) -> ClusterConsistencyReport:
        """Inspect and compare committed ledger histories across all cluster validators."""
        ts = now if now is not None else time.time()

        # Step 1: Collect reports and hash chains for all nodes
        node_reports: Dict[str, NodeConsistencyReport] = {}
        node_hash_chains: Dict[str, List[str]] = {}

        for nid, node in nodes.items():
            report, hashes = cls.inspect_node(node, nid)
            node_reports[nid] = report
            node_hash_chains[nid] = hashes

        # Step 2: Reference Selection
        # Prefer a running active leader
        reference_node: Optional[str] = None
        is_authoritative = False
        reference_type = "NONE"
        diag_msg: Optional[str] = None

        if (
            active_leader_id
            and active_leader_id in node_reports
            and node_reports[active_leader_id].is_running
            and node_reports[active_leader_id].status != LedgerStatus.ERROR
        ):
            reference_node = active_leader_id
            is_authoritative = True
            reference_type = "AUTHORITATIVE"
        else:
            # Fallback to provisional reference for diagnostics among running nodes with clean states
            running_candidates = [
                (nid, rep.commit_index)
                for nid, rep in node_reports.items()
                if rep.is_running and rep.status != LedgerStatus.ERROR
            ]
            if running_candidates:
                # Pick node with highest commit_index as provisional baseline
                running_candidates.sort(key=lambda x: x[1], reverse=True)
                reference_node = running_candidates[0][0]
                is_authoritative = False
                reference_type = "PROVISIONAL"
                diag_msg = f"No active leader; provisional comparison against {reference_node}"
            else:
                reference_node = None
                is_authoritative = False
                reference_type = "NONE"
                diag_msg = "No operational validators available for consistency check"

        # If no reference can be determined, all nodes are either unavailable or in error
        if reference_node is None:
            summary = {
                "total_nodes": len(node_reports),
                "consistent_count": 0,
                "behind_count": 0,
                "divergent_count": 0,
                "ahead_count": 0,
                "unavailable_count": sum(1 for r in node_reports.values() if r.status == LedgerStatus.UNAVAILABLE),
                "error_count": sum(1 for r in node_reports.values() if r.status == LedgerStatus.ERROR),
            }
            status = (
                ClusterConsistencyStatus.ERROR
                if summary["error_count"] > 0
                else ClusterConsistencyStatus.UNAVAILABLE
            )
            return ClusterConsistencyReport(
                timestamp=ts,
                cluster_status=status,
                is_authoritative=False,
                reference_type=reference_type,
                reference_node=None,
                reference_commit_index=None,
                reference_commit_hash=None,
                nodes=node_reports,
                summary=summary,
                diagnostic_message=diag_msg,
            )

        ref_report = node_reports[reference_node]
        ref_hashes = node_hash_chains[reference_node]
        ref_commit_index = ref_report.commit_index
        ref_commit_hash = ref_report.commit_hash

        # Step 3: Compare each node against the chosen reference
        counts = {
            "consistent": 0,
            "behind": 0,
            "divergent": 0,
            "ahead": 0,
            "unavailable": 0,
            "error": 0,
        }

        for nid, report in node_reports.items():
            if report.status in (LedgerStatus.UNAVAILABLE, LedgerStatus.ERROR):
                if report.status == LedgerStatus.UNAVAILABLE:
                    counts["unavailable"] += 1
                else:
                    counts["error"] += 1
                continue

            if nid == reference_node:
                report.status = LedgerStatus.CONSISTENT
                report.lag = 0
                report.mismatch_index = None
                counts["consistent"] += 1
                continue

            target_hashes = node_hash_chains[nid]
            target_commit_index = report.commit_index

            # Compare up to common prefix
            shared_len = min(target_commit_index, ref_commit_index)
            first_mismatch: Optional[int] = None

            for idx in range(1, shared_len + 1):
                if target_hashes[idx] != ref_hashes[idx]:
                    first_mismatch = idx
                    break

            if first_mismatch is not None:
                report.status = LedgerStatus.DIVERGENT
                report.mismatch_index = first_mismatch
                report.lag = max(0, ref_commit_index - target_commit_index)
                counts["divergent"] += 1
            elif target_commit_index < ref_commit_index:
                report.status = LedgerStatus.BEHIND
                report.lag = ref_commit_index - target_commit_index
                report.mismatch_index = None
                counts["behind"] += 1
            elif target_commit_index > ref_commit_index:
                report.status = LedgerStatus.AHEAD
                report.lag = 0
                report.mismatch_index = None
                counts["ahead"] += 1
            else:
                report.status = LedgerStatus.CONSISTENT
                report.lag = 0
                report.mismatch_index = None
                counts["consistent"] += 1

        summary = {
            "total_nodes": len(node_reports),
            "consistent_count": counts["consistent"],
            "behind_count": counts["behind"],
            "divergent_count": counts["divergent"],
            "ahead_count": counts["ahead"],
            "unavailable_count": counts["unavailable"],
            "error_count": counts["error"],
        }

        # Step 4: Determine Cluster-Wide Consistency Status
        if counts["error"] > 0:
            cluster_status = ClusterConsistencyStatus.ERROR
        elif counts["divergent"] > 0:
            cluster_status = ClusterConsistencyStatus.DIVERGENT
        elif not is_authoritative:
            # When reference is provisional, never declare definitive cluster-wide consistency
            cluster_status = ClusterConsistencyStatus.NO_LEADER
        elif counts["ahead"] > 0:
            cluster_status = ClusterConsistencyStatus.ANOMALY_AHEAD
        elif counts["behind"] > 0:
            cluster_status = ClusterConsistencyStatus.DEGRADED_BEHIND
        else:
            cluster_status = ClusterConsistencyStatus.CONSISTENT

        return ClusterConsistencyReport(
            timestamp=ts,
            cluster_status=cluster_status,
            is_authoritative=is_authoritative,
            reference_type=reference_type,
            reference_node=reference_node,
            reference_commit_index=ref_commit_index,
            reference_commit_hash=ref_commit_hash,
            nodes=node_reports,
            summary=summary,
            diagnostic_message=diag_msg,
        )
