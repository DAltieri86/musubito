from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from musubito.models import Artifact, ArtifactType, ExecutionNode, NodeStatus
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage


def test_retention_days_purges_expired_nodes_and_cleans_related_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention.db"
    old_time = datetime.now(timezone.utc) - timedelta(days=30)
    recent_time = datetime.now(timezone.utc)
    old_parent = _make_node("old-parent", old_time, output_artifact_id="artifact-old-parent")
    old_child = _make_node(
        "old-child",
        old_time,
        status=NodeStatus.STALE,
        output_artifact_id="artifact-old-child",
    )
    recent_stale = _make_node(
        "recent-stale",
        recent_time,
        status=NodeStatus.STALE,
        output_artifact_id="artifact-recent-stale",
    )

    with SQLiteStorage(str(db_path)) as storage:
        storage.save_execution(
            old_parent,
            _make_artifact(old_parent.output_artifact_id),
            frozenset(),
        )
        storage.save_execution(
            old_child,
            _make_artifact(old_child.output_artifact_id),
            frozenset({old_parent.node_id}),
        )
        storage.save_execution(
            recent_stale,
            _make_artifact(recent_stale.output_artifact_id),
            frozenset(),
        )

    with SQLiteStorage(str(db_path), retention_days=1):
        pass

    assert _node_ids(db_path) == {"recent-stale"}
    assert _artifact_ids(db_path) == {"artifact-recent-stale"}
    assert _count_rows(db_path, "edges") == 0


def test_max_size_mb_evicts_oldest_nodes_when_threshold_is_exceeded(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "size-cap.db"
    inserted_count = 12

    with SQLiteStorage(str(db_path), max_size_mb=0.001) as storage:
        for index in range(inserted_count):
            storage.save_execution(
                _make_node(
                    f"node-{index:02d}",
                    datetime.now(timezone.utc) + timedelta(seconds=index),
                    output_artifact_id=f"artifact-{index:02d}",
                ),
                _make_artifact(f"artifact-{index:02d}", payload_size=4096),
                frozenset(),
            )

        stats = storage.storage_stats()

    estimated_size_mb = stats["estimated_size_mb"]
    node_count = stats["node_count"]
    artifact_count = stats["artifact_count"]

    assert isinstance(estimated_size_mb, float)
    assert isinstance(node_count, int)
    assert isinstance(artifact_count, int)
    assert 0 <= node_count < inserted_count
    assert artifact_count == node_count


def test_auto_vacuum_runs_incremental_vacuum_after_eviction(tmp_path: Path) -> None:
    db_path = tmp_path / "auto-vacuum.db"
    traced_statements: list[str] = []

    with SQLiteStorage(str(db_path), max_size_mb=0.001, auto_vacuum=True) as storage:
        connection = storage._connection
        connection.set_trace_callback(traced_statements.append)
        storage.save_execution(
            _make_node(
                "evicted-node",
                datetime.now(timezone.utc),
                output_artifact_id="artifact-evicted-node",
            ),
            _make_artifact("artifact-evicted-node", payload_size=8192),
            frozenset(),
        )
        connection.set_trace_callback(None)

    assert any("PRAGMA incremental_vacuum" in statement for statement in traced_statements)


def test_storage_stats_returns_path_size_and_counts(tmp_path: Path) -> None:
    db_path = tmp_path / "stats.db"
    expected_count = 2

    with SQLiteStorage(str(db_path)) as storage:
        storage.save_execution(
            _make_node("node-a", datetime.now(timezone.utc), output_artifact_id="artifact-a"),
            _make_artifact("artifact-a"),
            frozenset(),
        )
        storage.save_execution(
            _make_node("node-b", datetime.now(timezone.utc), output_artifact_id="artifact-b"),
            _make_artifact("artifact-b"),
            frozenset(),
        )

        stats = storage.storage_stats()

    assert stats["db_path"] == str(db_path)
    assert isinstance(stats["estimated_size_mb"], float)
    assert stats["estimated_size_mb"] > 0.0
    assert stats["node_count"] == expected_count
    assert stats["artifact_count"] == expected_count


def test_default_constructor_keeps_existing_default_path_and_empty_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    with SQLiteStorage() as storage:
        stats = storage.storage_stats()

    assert storage.db_path == ".musubito/musubito.db"
    assert (tmp_path / ".musubito" / "musubito.db").exists()
    assert stats["node_count"] == 0
    assert stats["artifact_count"] == 0


def _make_node(
    node_id: str,
    executed_at: datetime,
    *,
    status: NodeStatus = NodeStatus.SUCCESS,
    output_artifact_id: str | None = None,
) -> ExecutionNode:
    return ExecutionNode(
        node_id=node_id,
        input_hash=f"input-hash-{node_id}",
        operation_name=f"operation-{node_id}",
        status=status,
        semantics=StepConfiguration(step_type=StepType.DETERMINISTIC),
        executed_at=executed_at,
        output_artifact_id=output_artifact_id,
    )


def _make_artifact(artifact_id: str | None, *, payload_size: int = 32) -> Artifact:
    if artifact_id is None:
        raise ValueError("artifact_id is required")

    return Artifact(
        artifact_id=artifact_id,
        type=ArtifactType.RESPONSE,
        payload={"text": "x" * payload_size},
    )


def _node_ids(db_path: Path) -> set[str]:
    with sqlite3.connect(str(db_path)) as connection:
        rows = connection.execute("SELECT node_id FROM nodes").fetchall()

    return {str(row[0]) for row in rows}


def _artifact_ids(db_path: Path) -> set[str]:
    with sqlite3.connect(str(db_path)) as connection:
        rows = connection.execute("SELECT artifact_id FROM artifacts").fetchall()

    return {str(row[0]) for row in rows}


def _count_rows(db_path: Path, table_name: str) -> int:
    if table_name not in {"artifacts", "edges", "nodes"}:
        raise ValueError(f"unsupported table name: {table_name}")

    with sqlite3.connect(str(db_path)) as connection:
        row: Any = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()

    return int(row[0])
