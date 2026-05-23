from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from itertools import pairwise
from pathlib import Path

from musubito.models import ExecutionNode, NodeStatus
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage

EXECUTED_AT = datetime(2026, 5, 23, 10, 0, tzinfo=timezone.utc)
DEPTH_GUARD_TIMEOUT_SECONDS = 2.0


def test_invalidate_downstream_respects_depth_guard_on_two_hundred_node_chain(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_1.db"
    node_ids = [f"N{index:04d}" for index in range(200)]

    with SQLiteStorage(str(db_path)) as storage:
        for node_id in node_ids:
            storage.save_node(_make_success_node(node_id))

        for parent_node_id, child_node_id in pairwise(node_ids):
            storage.save_edge(parent_node_id, child_node_id)

        storage.invalidate_downstream(node_ids[0])

    statuses = _node_statuses_from_sql(db_path)

    assert statuses["N0000"] == NodeStatus.SUCCESS.value
    assert statuses["N0001"] == NodeStatus.STALE.value
    assert statuses["N0050"] == NodeStatus.STALE.value
    assert statuses["N0167"] == NodeStatus.STALE.value
    assert statuses["N0168"] == NodeStatus.SUCCESS.value
    assert statuses["N0199"] == NodeStatus.SUCCESS.value


def test_invalidate_downstream_terminates_within_timeout_on_manual_cycle(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_2.db"

    with SQLiteStorage(str(db_path)) as storage:
        for node_id in ("A", "B", "C"):
            storage.save_node(_make_success_node(node_id))

        storage.save_edge("A", "B")
        storage.save_edge("B", "C")

    _insert_edge_with_sql(db_path, "C", "A")
    _invalidate_with_timeout(db_path, "A", DEPTH_GUARD_TIMEOUT_SECONDS)

    assert _node_statuses_from_sql(db_path) == {
        "A": NodeStatus.SUCCESS.value,
        "B": NodeStatus.STALE.value,
        "C": NodeStatus.STALE.value,
    }


def _make_success_node(node_id: str) -> ExecutionNode:
    return ExecutionNode(
        node_id=node_id,
        input_hash=f"input-hash-{node_id}",
        operation_name=f"operation-{node_id}",
        status=NodeStatus.SUCCESS,
        semantics=StepConfiguration(step_type=StepType.DETERMINISTIC),
        executed_at=EXECUTED_AT,
        output_artifact_id=None,
    )


def _insert_edge_with_sql(db_path: Path, parent_node_id: str, child_node_id: str) -> None:
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            """
            INSERT INTO edges (parent_node_id, child_node_id)
            VALUES (?, ?)
            """,
            (parent_node_id, child_node_id),
        )


def _invalidate_with_timeout(
    db_path: Path,
    node_id: str,
    timeout_seconds: float,
) -> None:
    exceptions: list[BaseException] = []
    timeout_expired = threading.Event()

    def run_invalidation() -> None:
        try:
            with SQLiteStorage(str(db_path)) as storage:
                storage.invalidate_downstream(node_id)
        except BaseException as exc:
            exceptions.append(exc)

    worker = threading.Thread(target=run_invalidation, daemon=True)
    timer = threading.Timer(timeout_seconds, timeout_expired.set)

    timer.start()
    worker.start()
    worker.join(timeout_seconds)
    timer.cancel()

    assert not worker.is_alive()
    assert not timeout_expired.is_set()

    if exceptions:
        raise exceptions[0]


def _node_statuses_from_sql(db_path: Path) -> dict[str, str]:
    with sqlite3.connect(str(db_path)) as connection:
        rows = connection.execute(
            """
            SELECT node_id, status
            FROM nodes
            ORDER BY node_id
            """
        ).fetchall()

    return {str(node_id): str(status) for node_id, status in rows}
