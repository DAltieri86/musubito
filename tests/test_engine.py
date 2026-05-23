from __future__ import annotations

import asyncio
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from musubito.engine import (
    MusubitoEngine,
    current_execution_parents,
    musubito_merge,
    override_upstream_parents,
)
from musubito.models import MusubitoResult, NodeStatus
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage


class NoStandaloneInvalidationStorage(SQLiteStorage):
    def invalidate_downstream(self, node_id: str) -> None:
        msg = "Engine must not call standalone invalidation for output changes."
        raise AssertionError(msg)


def test_execute_replays_successful_node_without_rerunning_function(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"
    call_count = 0

    def operation(inputs: Any) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return {"echo": inputs["value"]}

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

        first_result = engine.execute("echo", {"value": "stable"}, operation, semantics)
        second_result = engine.execute("echo", {"value": "stable"}, operation, semantics)

    assert call_count == 1
    assert second_result == first_result


def test_nested_execute_materializes_current_parent_before_child_edges(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"
    inner_result: MusubitoResult[Any] | None = None
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)

        def outer_operation(inputs: Any) -> dict[str, Any]:
            nonlocal inner_result
            inner_result = engine.execute(
                "inner",
                {"value": inputs["value"]},
                lambda child_inputs: child_inputs["value"] + 1,
                semantics,
            )
            return {"inner_value": inner_result.value}

        outer_result = engine.execute(
            "outer",
            {"value": 41},
            outer_operation,
            semantics,
        )

    assert inner_result is not None
    assert outer_result.value == {"inner_value": 42}
    assert _parent_node_ids(str(db_path), inner_result.node_id) == {outer_result.node_id}


def test_failed_outer_execution_resets_context_after_nested_child(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)

        def failing_outer_operation(inputs: Any) -> None:
            engine.execute(
                "inner-before-failure",
                {"value": inputs["value"]},
                lambda child_inputs: child_inputs["value"] + 1,
                semantics,
            )
            raise RuntimeError("outer failed")

        try:
            engine.execute("outer-failure", {"value": 1}, failing_outer_operation, semantics)
        except RuntimeError as exc:
            assert str(exc) == "outer failed"

    assert current_execution_parents.get() == frozenset()


def test_musubito_merge_uses_only_musubito_result_producer_node_ids() -> None:
    dependency = MusubitoResult[str](
        value="dependency",
        artifact_id="artifact-dependency",
        producer_node_id="producer-node",
        node_id="current-node",
    )
    ignored_dependency = object()

    with musubito_merge(dependency, ignored_dependency):
        assert override_upstream_parents.get() == frozenset({"producer-node"})

    assert override_upstream_parents.get() is None


def test_musubito_merge_creates_multi_parent_edges(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        left = engine.execute("left", {"value": "left"}, lambda inputs: inputs["value"], semantics)
        right = engine.execute(
            "right", {"value": "right"}, lambda inputs: inputs["value"], semantics
        )

        with musubito_merge(left, right):
            merged = engine.execute(
                "merged",
                {"value": "merged"},
                lambda inputs: inputs["value"],
                semantics,
            )

    assert _parent_node_ids(str(db_path), merged.node_id) == {
        left.producer_node_id,
        right.producer_node_id,
    }


def test_force_reexecution_invalidates_downstream_only_when_output_changes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"
    deterministic_semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)
    forced_semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
    )

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        parent = engine.execute(
            "parent",
            {"value": "same-input"},
            lambda inputs: {"version": "old", "input": inputs["value"]},
            deterministic_semantics,
        )

        with musubito_merge(parent):
            child = engine.execute(
                "child",
                {"value": "child"},
                lambda inputs: {"child": inputs["value"]},
                deterministic_semantics,
            )

        reexecuted_parent = engine.execute(
            "parent",
            {"value": "same-input"},
            lambda inputs: {"version": "new", "input": inputs["value"]},
            forced_semantics,
        )

        child_status = storage.get_node_status(child.node_id)

    assert reexecuted_parent.node_id == parent.node_id
    assert reexecuted_parent.artifact_id != parent.artifact_id
    assert child_status is NodeStatus.STALE


def test_output_change_invalidation_is_persisted_atomically_by_storage(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"
    deterministic_semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)
    forced_semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
    )

    with NoStandaloneInvalidationStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        parent = engine.execute(
            "parent",
            {"value": "same-input"},
            lambda inputs: {"version": "old", "input": inputs["value"]},
            deterministic_semantics,
        )

        with musubito_merge(parent):
            child = engine.execute(
                "child",
                {"value": "child"},
                lambda inputs: {"child": inputs["value"]},
                deterministic_semantics,
            )

        engine.execute(
            "parent",
            {"value": "same-input"},
            lambda inputs: {"version": "new", "input": inputs["value"]},
            forced_semantics,
        )
        child_status = storage.get_node_status(child.node_id)

    assert child_status is NodeStatus.STALE


def test_force_reexecution_keeps_downstream_fresh_when_output_is_unchanged(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"
    deterministic_semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)
    forced_semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
    )

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        parent = engine.execute(
            "parent",
            {"value": "same-input"},
            lambda inputs: {"version": "stable", "input": inputs["value"]},
            deterministic_semantics,
        )

        with musubito_merge(parent):
            child = engine.execute(
                "child",
                {"value": "child"},
                lambda inputs: {"child": inputs["value"]},
                deterministic_semantics,
            )

        reexecuted_parent = engine.execute(
            "parent",
            {"value": "same-input"},
            lambda inputs: {"version": "stable", "input": inputs["value"]},
            forced_semantics,
        )

        child_status = storage.get_node_status(child.node_id)

    assert reexecuted_parent.node_id == parent.node_id
    assert reexecuted_parent.artifact_id == parent.artifact_id
    assert child_status is NodeStatus.SUCCESS


def test_forced_reexecution_failure_marks_node_failed_and_stales_children(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"
    deterministic_semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)
    forced_semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
    )

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        parent = engine.execute(
            "parent",
            {"value": "same-input"},
            lambda inputs: {"version": "old", "input": inputs["value"]},
            deterministic_semantics,
        )

        with musubito_merge(parent):
            child = engine.execute(
                "child",
                {"value": "child"},
                lambda inputs: {"child": inputs["value"]},
                deterministic_semantics,
            )

        def fail_reexecution(inputs: Any) -> dict[str, Any]:
            raise RuntimeError(f"cannot rebuild {inputs['value']}")

        try:
            engine.execute("parent", {"value": "same-input"}, fail_reexecution, forced_semantics)
        except RuntimeError as exc:
            assert str(exc) == "cannot rebuild same-input"

        failed_parent = storage.get_node(parent.node_id)
        child_status = storage.get_node_status(child.node_id)

    assert failed_parent is not None
    assert failed_parent.status is NodeStatus.FAILED
    assert failed_parent.output_artifact_id is None
    assert child_status is NodeStatus.STALE


def test_shared_engine_executes_safely_across_threads(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"
    worker_count = 8
    writes_per_worker = 20
    barrier = threading.Barrier(worker_count)
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
    )

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(
                    _execute_nodes_concurrently,
                    engine,
                    worker_index,
                    writes_per_worker,
                    barrier,
                    semantics,
                )
                for worker_index in range(worker_count)
            ]

        failures = [failure for future in futures for failure in future.result()]

    assert failures == []
    assert _count_nodes(str(db_path)) == worker_count * writes_per_worker


def test_async_parallel_executions_do_not_leak_contextvars(tmp_path: Path) -> None:
    asyncio.run(_assert_async_parallel_context_isolation(tmp_path))


async def _assert_async_parallel_context_isolation(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"
    worker_count = 6
    ready_count = 0
    ready_lock = asyncio.Lock()
    all_workers_ready = asyncio.Event()
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC, force_reexecution=True)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)

        async def operation(inputs: Any) -> dict[str, Any]:
            nonlocal ready_count

            parents_at_entry = current_execution_parents.get()
            async with ready_lock:
                ready_count += 1
                if ready_count == worker_count:
                    all_workers_ready.set()

            await asyncio.wait_for(all_workers_ready.wait(), timeout=2.0)
            parents_after_wait = current_execution_parents.get()

            return {
                "worker": inputs["worker"],
                "parents_at_entry": sorted(parents_at_entry),
                "parents_after_wait": sorted(parents_after_wait),
            }

        results = await asyncio.gather(
            *[
                engine.execute_async(
                    f"async-worker-{worker_index}",
                    {"worker": worker_index},
                    operation,
                    semantics,
                )
                for worker_index in range(worker_count)
            ]
        )

    seen_node_ids = {result.node_id for result in results}
    assert len(seen_node_ids) == worker_count

    for result in results:
        assert result.value["parents_at_entry"] == [result.node_id]
        assert result.value["parents_after_wait"] == [result.node_id]

    assert current_execution_parents.get() == frozenset()
    assert override_upstream_parents.get() is None


def _parent_node_ids(db_path: str, child_node_id: str) -> set[str]:
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT parent_node_id
            FROM edges
            WHERE child_node_id = ?
            """,
            (child_node_id,),
        ).fetchall()

    return {str(row[0]) for row in rows}


def _execute_nodes_concurrently(
    engine: MusubitoEngine,
    worker_index: int,
    writes_per_worker: int,
    barrier: threading.Barrier,
    semantics: StepConfiguration,
) -> list[str]:
    failures: list[str] = []
    try:
        barrier.wait(timeout=5.0)
    except threading.BrokenBarrierError as exc:
        return [repr(exc)]

    for write_index in range(writes_per_worker):
        try:
            engine.execute(
                f"thread-worker-{worker_index}-{write_index}",
                {"worker": worker_index, "write": write_index},
                lambda inputs: {"worker": inputs["worker"], "write": inputs["write"]},
                semantics,
            )
        except (RuntimeError, sqlite3.DatabaseError, sqlite3.OperationalError) as exc:
            failures.append(f"{type(exc).__name__}: {exc}")

    return failures


def _count_nodes(db_path: str) -> int:
    with sqlite3.connect(db_path) as connection:
        row: Any = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()

    return int(row[0])
