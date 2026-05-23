from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from musubito.models import ExecutionNode, NodeStatus
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage

NODE_IDS = ("A", "B", "C", "D")
CHAIN_EDGES = (("A", "B"), ("B", "C"), ("C", "D"))
EXECUTED_AT = datetime(2026, 5, 23, 10, 0, tzinfo=timezone.utc)


def test_invalidate_downstream_from_a_marks_b_c_d_stale_and_keeps_a_success(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_1.db"

    with SQLiteStorage(str(db_path)) as storage:
        _insert_success_chain(storage)
        storage.invalidate_downstream("A")

    assert _node_statuses_from_sql(db_path) == {
        "A": NodeStatus.SUCCESS.value,
        "B": NodeStatus.STALE.value,
        "C": NodeStatus.STALE.value,
        "D": NodeStatus.STALE.value,
    }


def test_invalidate_downstream_from_c_marks_only_d_stale_and_keeps_a_b_c_success(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_2.db"

    with SQLiteStorage(str(db_path)) as storage:
        _insert_success_chain(storage)
        storage.invalidate_downstream("C")

    assert _node_statuses_from_sql(db_path) == {
        "A": NodeStatus.SUCCESS.value,
        "B": NodeStatus.SUCCESS.value,
        "C": NodeStatus.SUCCESS.value,
        "D": NodeStatus.STALE.value,
    }


def test_invalidate_downstream_from_a_is_idempotent_for_linear_chain(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_3.db"

    with SQLiteStorage(str(db_path)) as storage:
        _insert_success_chain(storage)
        storage.invalidate_downstream("A")
        storage.invalidate_downstream("A")

    assert _node_statuses_from_sql(db_path) == {
        "A": NodeStatus.SUCCESS.value,
        "B": NodeStatus.STALE.value,
        "C": NodeStatus.STALE.value,
        "D": NodeStatus.STALE.value,
    }


def _insert_success_chain(storage: SQLiteStorage) -> None:
    for node_id in NODE_IDS:
        storage.save_node(_make_success_node(node_id))

    for parent_node_id, child_node_id in CHAIN_EDGES:
        storage.save_edge(parent_node_id, child_node_id)


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
