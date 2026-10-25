"""Independent State-Transition Verification Engine for ConsensusLab.

Provides:
- Independent reference state machine replay of committed commands.
- Exact parity with the existing Raft node state machine transition semantics.
- Evaluation of each validator's application state at its own applied index.
- Differentiation between log consistency, state consistency, application lag, and divergence.
- Deterministic canonical state fingerprinting and differing-key diagnostics.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from raft.ledger import LedgerConsistencyChecker, LedgerStatus
from raft.state import LogEntry

logger = logging.getLogger(__name__)

# Fixed genesis state fingerprint for an empty state dictionary ({})
GENESIS_STATE_HASH = hashlib.sha256(b"{}").hexdigest()


class NodeStateStatus(str, Enum):
    """Evaluation status of an individual node's state machine state."""

    STATE_CONSISTENT = "STATE_CONSISTENT"
    APPLICATION_LAG = "APPLICATION_LAG"
    STATE_DIVERGENT = "STATE_DIVERGENT"
    LEDGER_DIVERGENT = "LEDGER_DIVERGENT"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


class ClusterStateStatus(str, Enum):
    """Aggregate cluster-wide state machine verification status."""

    VERIFIED = "VERIFIED"
    DEGRADED_LAG = "DEGRADED_LAG"
    DIVERGENT = "DIVERGENT"
    NO_LEADER = "NO_LEADER"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


def apply_command_to_state(kv_store: Dict[str, Any], command: Any) -> None:
    """Faithfully apply a state machine command to a target KV dictionary in-place.

    Matches the exact semantics of raft.node.apply_command_to_kv_store.
    Enforces deep-copy of mutable payloads to protect reference independence.
    """
    if isinstance(command, dict):
        op = command.get("op", "SET")
        op_str = "SET" if op is None else str(op).upper()

        if op_str == "SET":
            key = command.get("key") or command.get("k")
            val = command.get("val") if "val" in command else command.get("value")
            if key is not None:
                kv_store[str(key)] = copy.deepcopy(val)
        elif op_str in ("DELETE", "DEL"):
            key = command.get("key") or command.get("k")
            if key is not None:
                kv_store.pop(str(key), None)
        elif op_str == "NOOP":
            # Explicit no-op used by ReadIndex linearizable reads
            pass
        # Any other op is a no-op matching raft.node
    elif isinstance(command, str):
        parts = command.strip().split()
        if len(parts) >= 2 and parts[0].upper() in ("SET", "PUT"):
            rest = " ".join(parts[1:])
            if "=" in rest:
                k, v = rest.split("=", 1)
                kv_store[k.strip()] = v.strip()
            elif len(parts) == 3:
                kv_store[parts[1].strip()] = parts[2].strip()
        elif "=" in command:
            k, v = command.split("=", 1)
            kv_store[k.strip()] = v.strip()


def compute_state_hash(state: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Compute a deterministic SHA-256 fingerprint using canonical JSON.

    Returns:
        (hash_hex, error_message)
    """
    try:
        serialized = json.dumps(state, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest(), None
    except Exception as exc:
        return None, f"State serialization failure: {exc}"


def diff_states(actual: Dict[str, Any], expected: Dict[str, Any]) -> Dict[str, List[str]]:
    """Compare two state dictionaries and return missing, unexpected, and differing keys."""
    missing_keys: List[str] = []
    unexpected_keys: List[str] = []
    differing_keys: List[str] = []

    for k in expected:
        if k not in actual:
            missing_keys.append(k)
        else:
            v_actual = actual[k]
            v_expected = expected[k]
            if v_actual != v_expected:
                differing_keys.append(k)

    for k in actual:
        if k not in expected:
            unexpected_keys.append(k)

    all_differing = sorted(list(set(missing_keys + unexpected_keys + differing_keys)))
    return {
        "missing_keys": sorted(missing_keys),
        "unexpected_keys": sorted(unexpected_keys),
        "differing_keys": all_differing,
    }


@dataclass
class NodeStateVerificationReport:
    """State machine verification report for an individual validator node."""

    node_id: str
    status: NodeStateStatus
    is_running: bool
    commit_index: int
    last_applied: int
    state_hash: Optional[str] = None
    expected_state_hash: Optional[str] = None
    commit_lag: Optional[int] = None
    application_lag: Optional[int] = None
    differing_keys: List[str] = field(default_factory=list)
    missing_keys: List[str] = field(default_factory=list)
    unexpected_keys: List[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "is_running": self.is_running,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "state_hash": self.state_hash,
            "expected_state_hash": self.expected_state_hash,
            "commit_lag": self.commit_lag,
            "application_lag": self.application_lag,
            "differing_keys": self.differing_keys,
            "missing_keys": self.missing_keys,
            "unexpected_keys": self.unexpected_keys,
            "error": self.error,
        }


@dataclass
class ClusterStateVerificationReport:
    """Full cluster state machine verification report."""

    timestamp: float
    cluster_state_status: ClusterStateStatus
    is_authoritative: bool
    reference_type: str  # "AUTHORITATIVE", "PROVISIONAL", or "NONE"
    reference_node: Optional[str]
    reference_commit_index: Optional[int]
    reference_applied_index: Optional[int]
    reference_state_hash: Optional[str]
    nodes: Dict[str, NodeStateVerificationReport] = field(default_factory=dict)
    summary: Dict[str, int] = field(default_factory=dict)
    diagnostic_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "cluster_state_status": self.cluster_state_status.value,
            "is_authoritative": self.is_authoritative,
            "reference_type": self.reference_type,
            "reference_node": self.reference_node,
            "reference_commit_index": self.reference_commit_index,
            "reference_applied_index": self.reference_applied_index,
            "reference_state_hash": self.reference_state_hash,
            "nodes": {nid: rep.to_dict() for nid, rep in self.nodes.items()},
            "summary": self.summary,
            "diagnostic_message": self.diagnostic_message,
        }


class StateTransitionVerifier:
    """Independent reference state machine and cross-validator state verifier."""

    @classmethod
    def replay_committed_log(
        cls,
        committed_log: List[LogEntry],
        up_to_index: int,
    ) -> Tuple[List[Dict[str, Any]], List[str], Optional[str]]:
        """Replay committed log entries sequentially from genesis state.

        Returns:
            (states_by_index, hashes_by_index, error_message)
            Index 0 corresponds to the genesis empty state ({}).
            Index i corresponds to the state after applying entry at index i.
        """
        states: List[Dict[str, Any]] = [{}]
        hashes: List[str] = [GENESIS_STATE_HASH]
        current_state: Dict[str, Any] = {}

        safe_limit = max(0, min(up_to_index, len(committed_log)))

        for i in range(safe_limit):
            entry = committed_log[i]
            # Deep-copy current state to form next immutable state snapshot
            next_state = copy.deepcopy(current_state)
            try:
                apply_command_to_state(next_state, entry.command)
            except Exception as exc:
                err = f"Failed to replay command at index {entry.index}: {exc}"
                logger.warning(err)
                return [], [], err

            h, err_h = compute_state_hash(next_state)
            if err_h is not None:
                return [], [], err_h

            current_state = next_state
            states.append(current_state)
            hashes.append(h)

        return states, hashes, None

    @classmethod
    def verify_cluster_state(
        cls,
        nodes: Dict[str, Any],
        active_leader_id: Optional[str] = None,
        now: Optional[float] = None,
    ) -> ClusterStateVerificationReport:
        """Independently verify state transitions across all cluster nodes."""
        ts = now if now is not None else time.time()

        # Step 1: Run Phase 2B Ledger Consistency Check for reference & divergence facts
        ledger_report = LedgerConsistencyChecker.check_cluster_consistency(
            nodes=nodes,
            active_leader_id=active_leader_id,
            now=ts,
        )

        reference_node = ledger_report.reference_node
        is_authoritative = ledger_report.is_authoritative
        reference_type = ledger_report.reference_type
        ref_commit_index = ledger_report.reference_commit_index or 0
        diag_msg = ledger_report.diagnostic_message

        # If no reference node is available at all
        if reference_node is None:
            node_reports: Dict[str, NodeStateVerificationReport] = {}
            for nid, node in nodes.items():
                is_running = getattr(node, "_is_running", True) if node else False
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.UNAVAILABLE if not is_running else NodeStateStatus.ERROR,
                    is_running=is_running,
                    commit_index=0,
                    last_applied=0,
                    error="Node unavailable" if not is_running else "No reference node available",
                )
            summary = {
                "total_nodes": len(nodes),
                "consistent_count": 0,
                "lagging_count": 0,
                "divergent_count": 0,
                "unavailable_count": sum(1 for r in node_reports.values() if r.status == NodeStateStatus.UNAVAILABLE),
                "error_count": sum(1 for r in node_reports.values() if r.status == NodeStateStatus.ERROR),
            }
            return ClusterStateVerificationReport(
                timestamp=ts,
                cluster_state_status=ClusterStateStatus.UNAVAILABLE,
                is_authoritative=False,
                reference_type="NONE",
                reference_node=None,
                reference_commit_index=None,
                reference_applied_index=None,
                reference_state_hash=None,
                nodes=node_reports,
                summary=summary,
                diagnostic_message=diag_msg or "No operational nodes available",
            )

        # Step 2: Build Independent Reference State Machine from Reference Node's committed log
        ref_node_obj = nodes[reference_node]
        ref_state_obj = getattr(ref_node_obj, "state", None)
        ref_log = getattr(ref_state_obj, "log", []) if ref_state_obj else []
        ref_applied_index = getattr(ref_state_obj, "last_applied", 0) if ref_state_obj else 0

        ref_states, ref_hashes, replay_err = cls.replay_committed_log(ref_log, ref_commit_index)

        if replay_err is not None:
            # Replay failed due to corrupted or non-serializable payload on reference
            node_reports = {
                nid: NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.ERROR,
                    is_running=getattr(node, "_is_running", True),
                    commit_index=0,
                    last_applied=0,
                    error=replay_err,
                )
                for nid, node in nodes.items()
            }
            return ClusterStateVerificationReport(
                timestamp=ts,
                cluster_state_status=ClusterStateStatus.ERROR,
                is_authoritative=is_authoritative,
                reference_type=reference_type,
                reference_node=reference_node,
                reference_commit_index=ref_commit_index,
                reference_applied_index=ref_applied_index,
                reference_state_hash=None,
                nodes=node_reports,
                summary={"total_nodes": len(nodes), "error_count": len(nodes)},
                diagnostic_message=f"Reference log replay error: {replay_err}",
            )

        reference_state_hash = ref_hashes[ref_commit_index] if ref_commit_index < len(ref_hashes) else GENESIS_STATE_HASH

        # Step 3: Evaluate each node at its own applied index
        node_reports = {}
        counts = {
            "consistent": 0,
            "lagging": 0,
            "divergent": 0,
            "unavailable": 0,
            "error": 0,
        }

        for nid, node in nodes.items():
            is_running = getattr(node, "_is_running", True) if node is not None else False
            if not is_running or node is None:
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.UNAVAILABLE,
                    is_running=False,
                    commit_index=0,
                    last_applied=0,
                    error="Node is offline or terminated",
                )
                counts["unavailable"] += 1
                continue

            state = getattr(node, "state", None)
            if state is None:
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.ERROR,
                    is_running=True,
                    commit_index=0,
                    last_applied=0,
                    error="Node state object missing",
                )
                counts["error"] += 1
                continue

            commit_index = max(0, getattr(state, "commit_index", 0))
            last_applied = max(0, getattr(state, "last_applied", 0))
            node_log = getattr(state, "log", [])
            actual_kv = getattr(state, "kv_store", {})

            commit_lag = max(0, ref_commit_index - commit_index)
            application_lag = max(0, commit_index - last_applied)

            # Validate index invariants
            if last_applied > commit_index:
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.ERROR,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    commit_lag=commit_lag,
                    application_lag=application_lag,
                    error=f"Index invariant violated: last_applied ({last_applied}) exceeds commit_index ({commit_index})",
                )
                counts["error"] += 1
                continue

            if last_applied > len(node_log):
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.ERROR,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    commit_lag=commit_lag,
                    application_lag=application_lag,
                    error=f"Index invariant violated: last_applied ({last_applied}) exceeds available log length ({len(node_log)})",
                )
                counts["error"] += 1
                continue

            if commit_index > len(node_log):
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.ERROR,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    commit_lag=commit_lag,
                    application_lag=application_lag,
                    error=f"Index invariant violated: commit_index ({commit_index}) exceeds available log length ({len(node_log)})",
                )
                counts["error"] += 1
                continue

            # Compute actual state fingerprint
            actual_hash, hash_err = compute_state_hash(actual_kv)
            if hash_err is not None:
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.ERROR,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    commit_lag=commit_lag,
                    application_lag=application_lag,
                    error=hash_err,
                )
                counts["error"] += 1
                continue

            # Check if ledger history is divergent at or before the node's applied index
            node_ledger_info = ledger_report.nodes.get(nid)
            has_ledger_conflict_applied = False
            if node_ledger_info and node_ledger_info.status == LedgerStatus.DIVERGENT.value:
                mismatch_idx = node_ledger_info.mismatch_index
                if mismatch_idx is not None and mismatch_idx <= last_applied:
                    has_ledger_conflict_applied = True

            if has_ledger_conflict_applied:
                # Even if final KV matches by coincidence, committed history conflict takes precedence
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.LEDGER_DIVERGENT,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    state_hash=actual_hash,
                    expected_state_hash=None,
                    commit_lag=commit_lag,
                    application_lag=application_lag,
                    error=f"Ledger conflict at committed index {node_ledger_info.mismatch_index} <= applied index {last_applied}",
                )
                counts["divergent"] += 1
                continue

            # Compare at the node's own last_applied index
            if last_applied < len(ref_states):
                expected_state = ref_states[last_applied]
                expected_hash = ref_hashes[last_applied]
            else:
                # Node applied ahead of reference commit index
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.ERROR,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    commit_lag=commit_lag,
                    application_lag=application_lag,
                    state_hash=actual_hash,
                    error=f"Node applied index {last_applied} exceeds reference commit index {ref_commit_index}",
                )
                counts["error"] += 1
                continue

            # State comparison
            diffs = diff_states(actual_kv, expected_state)
            is_matching = (actual_hash == expected_hash) and (actual_kv == expected_state)

            if not is_matching:
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.STATE_DIVERGENT,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    state_hash=actual_hash,
                    expected_state_hash=expected_hash,
                    commit_lag=commit_lag,
                    application_lag=application_lag,
                    differing_keys=diffs["differing_keys"],
                    missing_keys=diffs["missing_keys"],
                    unexpected_keys=diffs["unexpected_keys"],
                )
                counts["divergent"] += 1
            elif (last_applied < ref_commit_index) or (last_applied < commit_index):
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.APPLICATION_LAG,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    state_hash=actual_hash,
                    expected_state_hash=expected_hash,
                    commit_lag=commit_lag,
                    application_lag=application_lag,
                    differing_keys=[],
                )
                counts["lagging"] += 1
            else:
                node_reports[nid] = NodeStateVerificationReport(
                    node_id=nid,
                    status=NodeStateStatus.STATE_CONSISTENT,
                    is_running=True,
                    commit_index=commit_index,
                    last_applied=last_applied,
                    state_hash=actual_hash,
                    expected_state_hash=expected_hash,
                    commit_lag=0,
                    application_lag=0,
                    differing_keys=[],
                )
                counts["consistent"] += 1

        summary = {
            "total_nodes": len(nodes),
            "consistent_count": counts["consistent"],
            "lagging_count": counts["lagging"],
            "divergent_count": counts["divergent"],
            "unavailable_count": counts["unavailable"],
            "error_count": counts["error"],
        }

        # Step 4: Cluster Status Determination (Strict Precedence)
        if counts["error"] > 0:
            cluster_status = ClusterStateStatus.ERROR
        elif counts["divergent"] > 0:
            cluster_status = ClusterStateStatus.DIVERGENT
        elif not is_authoritative:
            cluster_status = ClusterStateStatus.NO_LEADER
        elif counts["lagging"] > 0:
            cluster_status = ClusterStateStatus.DEGRADED_LAG
        elif counts["consistent"] > 0:
            cluster_status = ClusterStateStatus.VERIFIED
        else:
            cluster_status = ClusterStateStatus.UNAVAILABLE

        return ClusterStateVerificationReport(
            timestamp=ts,
            cluster_state_status=cluster_status,
            is_authoritative=is_authoritative,
            reference_type=reference_type,
            reference_node=reference_node,
            reference_commit_index=ref_commit_index,
            reference_applied_index=ref_applied_index,
            reference_state_hash=reference_state_hash,
            nodes=node_reports,
            summary=summary,
            diagnostic_message=diag_msg,
        )
