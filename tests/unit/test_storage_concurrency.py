from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import pairwise
from pathlib import Path
from typing import Any

from musubito.models import Artifact, ArtifactType, ExecutionNode, NodeStatus
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage

EXECUTED_AT = datetime(2026, 5, 23, 10, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class ReaderWriterContext:
    writer_count: int
    seed_node_ids: list[str]
    writer_node_ids: list[str]


def test_concurrent_save_execution_on_distinct_nodes_completes_without_errors(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "save_execution_concurrency.db"
    worker_count = 10
    _initialize_database(db_path)
    storages = _open_storages(db_path, worker_count)

    try:
        errors = _run_simultaneously(
            worker_count,
            lambda worker_index, barrier: _save_execution_worker(
                storages[worker_index],
                worker_index,
                barrier,
            ),
        )
    finally:
        _close_storages(storages)

    assert _format_thread_errors(errors) == []
    assert _count_nodes(db_path) == worker_count
    assert _node_statuses_from_sql(db_path) == {
        f"save-node-{worker_index}": NodeStatus.SUCCESS.value
        for worker_index in range(worker_count)
    }


def test_concurrent_invalidate_downstream_on_independent_chains_avoids_database_locks(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "invalidate_concurrency.db"
    chain_count = 4
    chain_length = 5
    invalidation_targets = [
        _chain_node_id(chain_index, node_index)
        for chain_index in range(chain_count)
        for node_index in range(chain_length)
    ]

    with SQLiteStorage(str(db_path)) as storage:
        _insert_success_chains(storage, chain_count, chain_length)

    storages = _open_storages(db_path, len(invalidation_targets))
    try:
        errors = _run_simultaneously(
            len(invalidation_targets),
            lambda worker_index, barrier: _invalidate_worker(
                storages[worker_index],
                invalidation_targets[worker_index],
                barrier,
            ),
        )
    finally:
        _close_storages(storages)

    assert _format_thread_errors(errors) == []
    assert all("database is locked" not in str(error) for error in errors)
    assert _node_statuses_from_sql(db_path) == {
        _chain_node_id(chain_index, node_index): (
            NodeStatus.SUCCESS.value if node_index == 0 else NodeStatus.STALE.value
        )
        for chain_index in range(chain_count)
        for node_index in range(chain_length)
    }


def test_concurrent_writers_and_readers_complete_without_crashes_or_corruption(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reader_writer_concurrency.db"
    writer_count = 5
    reader_count = 5
    worker_count = writer_count + reader_count
    context = ReaderWriterContext(
        writer_count=writer_count,
        seed_node_ids=[f"seed-node-{index}" for index in range(reader_count)],
        writer_node_ids=[f"writer-node-{index}" for index in range(writer_count)],
    )

    with SQLiteStorage(str(db_path)) as storage:
        for node_id in context.seed_node_ids:
            storage.save_execution(
                _make_execution_node(node_id),
                _make_artifact(node_id),
                frozenset(),
            )

    storages = _open_storages(db_path, worker_count)
    try:
        errors = _run_simultaneously(
            worker_count,
            lambda worker_index, barrier: _reader_writer_worker(
                storages[worker_index],
                worker_index,
                context,
                barrier,
            ),
        )
    finally:
        _close_storages(storages)

    assert _format_thread_errors(errors) == []
    assert _count_nodes(db_path) == writer_count + reader_count
    assert _node_statuses_from_sql(db_path) == {
        **{node_id: NodeStatus.SUCCESS.value for node_id in context.seed_node_ids},
        **{node_id: NodeStatus.SUCCESS.value for node_id in context.writer_node_ids},
    }


def _run_simultaneously(
    worker_count: int,
    worker: Callable[[int, threading.Barrier], None],
) -> list[BaseException]:
    barrier = threading.Barrier(worker_count)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def run_worker(worker_index: int) -> None:
        try:
            worker(worker_index, barrier)
        except BaseException as exc:
            with errors_lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=run_worker, args=(worker_index,))
        for worker_index in range(worker_count)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    return errors


def _save_execution_worker(
    storage: SQLiteStorage,
    worker_index: int,
    barrier: threading.Barrier,
) -> None:
    barrier.wait(timeout=5.0)
    node_id = f"save-node-{worker_index}"
    storage.save_execution(
        _make_execution_node(node_id),
        _make_artifact(node_id),
        frozenset(),
    )


def _invalidate_worker(
    storage: SQLiteStorage,
    node_id: str,
    barrier: threading.Barrier,
) -> None:
    barrier.wait(timeout=5.0)
    storage.invalidate_downstream(node_id)


def _reader_writer_worker(
    storage: SQLiteStorage,
    worker_index: int,
    context: ReaderWriterContext,
    barrier: threading.Barrier,
) -> None:
    barrier.wait(timeout=5.0)
    if worker_index < context.writer_count:
        node_id = context.writer_node_ids[worker_index]
        storage.save_execution(
            _make_execution_node(node_id),
            _make_artifact(node_id),
            frozenset(),
        )
        return

    for node_id in context.seed_node_ids + context.writer_node_ids:
        loaded_node = storage.get_node(node_id)
        if loaded_node is not None:
            assert loaded_node.node_id == node_id
            assert loaded_node.status is NodeStatus.SUCCESS


def _initialize_database(db_path: Path) -> None:
    with SQLiteStorage(str(db_path)):
        pass


def _open_storages(db_path: Path, count: int) -> list[SQLiteStorage]:
    return [SQLiteStorage(str(db_path)) for _ in range(count)]


def _close_storages(storages: list[SQLiteStorage]) -> None:
    for storage in storages:
        storage.close()


def _insert_success_chains(
    storage: SQLiteStorage,
    chain_count: int,
    chain_length: int,
) -> None:
    for chain_index in range(chain_count):
        node_ids = [_chain_node_id(chain_index, node_index) for node_index in range(chain_length)]
        for node_id in node_ids:
            storage.save_node(_make_success_node(node_id))

        for parent_node_id, child_node_id in pairwise(node_ids):
            storage.save_edge(parent_node_id, child_node_id)


def _chain_node_id(chain_index: int, node_index: int) -> str:
    return f"chain-{chain_index}-node-{node_index}"


def _make_execution_node(node_id: str) -> ExecutionNode:
    artifact_id = _artifact_id_for_node(node_id)
    return ExecutionNode(
        node_id=node_id,
        input_hash=f"input-hash-{node_id}",
        operation_name=f"operation-{node_id}",
        status=NodeStatus.SUCCESS,
        semantics=StepConfiguration(step_type=StepType.DETERMINISTIC),
        executed_at=EXECUTED_AT,
        output_artifact_id=artifact_id,
    )


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


def _make_artifact(node_id: str) -> Artifact:
    return Artifact(
        artifact_id=_artifact_id_for_node(node_id),
        type=ArtifactType.RESPONSE,
        payload={"node_id": node_id},
    )


def _artifact_id_for_node(node_id: str) -> str:
    return f"artifact-{node_id}"


def _count_nodes(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as connection:
        row: Any = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()

    return int(row[0])


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


def _format_thread_errors(errors: list[BaseException]) -> list[str]:
    return [f"{type(error).__name__}: {error}" for error in errors]
