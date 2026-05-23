from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from musubito.hashing import compute_node_id, generate_deterministic_hash
from musubito.models import Artifact, ArtifactType, ExecutionNode, NodeStatus
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage


def test_storage_creates_tables_indexes_and_wal_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"

    with SQLiteStorage(str(db_path)):
        pass

    with sqlite3.connect(str(db_path)) as connection:
        tables = _sqlite_names(connection, "table")
        indexes = _sqlite_names(connection, "index")
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()

    assert {"artifacts", "nodes", "edges"}.issubset(tables)
    assert {
        "idx_nodes_input_hash",
        "idx_edges_parent_node_id",
        "idx_edges_child_node_id",
    }.issubset(indexes)
    assert journal_mode == "wal"


def test_storage_persists_artifacts_nodes_and_edges(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"
    artifact = Artifact(
        artifact_id="artifact-response",
        type=ArtifactType.RESPONSE,
        payload={"text": "done", "usage": {"tokens": 7}},
    )
    node = _make_node("node-a", output_artifact_id=artifact.artifact_id)
    child = _make_node("node-b")

    with SQLiteStorage(str(db_path)) as storage:
        storage.save_artifact(artifact)
        storage.save_node(node)
        storage.save_node(child)
        storage.save_edge(node.node_id, child.node_id)

        loaded_artifact = storage.get_artifact(artifact.artifact_id)
        loaded_node = storage.get_node(node.node_id)

    assert loaded_artifact == artifact
    assert loaded_node == node


def test_invalidate_downstream_marks_descendants_stale_and_handles_cycles(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"
    root = _make_node("root")
    child = _make_node("child")
    grandchild = _make_node("grandchild")
    sibling = _make_node("sibling")
    unrelated = _make_node("unrelated")

    with SQLiteStorage(str(db_path)) as storage:
        for node in (root, child, grandchild, sibling, unrelated):
            storage.save_node(node)

        storage.save_edge(root.node_id, child.node_id)
        storage.save_edge(child.node_id, grandchild.node_id)
        storage.save_edge(grandchild.node_id, child.node_id)
        storage.save_edge(root.node_id, sibling.node_id)

        storage.invalidate_downstream(root.node_id)

        statuses = {
            node.node_id: storage.get_node_status(node.node_id)
            for node in (root, child, grandchild, sibling, unrelated)
        }

    assert statuses == {
        root.node_id: NodeStatus.SUCCESS,
        child.node_id: NodeStatus.STALE,
        grandchild.node_id: NodeStatus.STALE,
        sibling.node_id: NodeStatus.STALE,
        unrelated.node_id: NodeStatus.SUCCESS,
    }


def test_concurrent_short_writes_complete_without_transient_lock_failures(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"
    worker_count = 8
    writes_per_worker = 12
    barrier = threading.Barrier(worker_count)

    with SQLiteStorage(str(db_path)):
        pass

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                _write_nodes_concurrently,
                str(db_path),
                worker_index,
                writes_per_worker,
                barrier,
            )
            for worker_index in range(worker_count)
        ]

    failures = [failure for future in futures for failure in future.result()]

    with SQLiteStorage(str(db_path)) as storage:
        persisted_count = _count_nodes(storage.db_path)

    assert failures == []
    assert persisted_count == worker_count * writes_per_worker


def _sqlite_names(connection: sqlite3.Connection, object_type: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = ?
        """,
        (object_type,),
    ).fetchall()
    return {str(row[0]) for row in rows}


def _make_node(
    node_seed: str,
    *,
    status: NodeStatus = NodeStatus.SUCCESS,
    output_artifact_id: str | None = None,
) -> ExecutionNode:
    input_hash = generate_deterministic_hash({"node_seed": node_seed})
    node_id = compute_node_id(
        operation_name=f"operation-{node_seed}",
        input_hash=input_hash,
        upstream_ids=frozenset(),
    )
    return ExecutionNode(
        node_id=node_id,
        input_hash=input_hash,
        operation_name=f"operation-{node_seed}",
        status=status,
        semantics=StepConfiguration(step_type=StepType.DETERMINISTIC),
        executed_at=datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc),
        output_artifact_id=output_artifact_id,
    )


def _write_nodes_concurrently(
    db_path: str,
    worker_index: int,
    writes_per_worker: int,
    barrier: threading.Barrier,
) -> list[str]:
    failures: list[str] = []

    with SQLiteStorage(db_path) as storage:
        try:
            barrier.wait(timeout=5.0)
        except threading.BrokenBarrierError as exc:
            return [repr(exc)]

        for write_index in range(writes_per_worker):
            try:
                storage.save_node(_make_node(f"worker-{worker_index}-{write_index}"))
            except sqlite3.OperationalError as exc:
                failures.append(str(exc))

    return failures


def _count_nodes(db_path: str) -> int:
    with sqlite3.connect(db_path) as connection:
        row: Any = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()

    return int(row[0])
