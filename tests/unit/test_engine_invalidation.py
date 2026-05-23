from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from musubito.engine import MusubitoEngine, musubito_merge
from musubito.models import NodeStatus
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage


def test_pipeline_downstream_remains_success_when_parent_output_is_unchanged(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_1.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)
    forced_semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
    )

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        parent = engine.execute(
            "parent-same-output",
            {"value": "stable-input"},
            lambda inputs: "foo",
            semantics,
        )

        with musubito_merge(parent):
            child = engine.execute(
                "child-of-same-output",
                {"value": "child-input"},
                lambda inputs: f"child:{inputs['value']}",
                semantics,
            )

        reexecuted_parent = engine.execute(
            "parent-same-output",
            {"value": "stable-input"},
            lambda inputs: "foo",
            forced_semantics,
        )

    assert reexecuted_parent.node_id == parent.node_id
    assert reexecuted_parent.artifact_id == parent.artifact_id
    assert _node_statuses_from_sql(db_path, (parent.node_id, child.node_id)) == {
        parent.node_id: NodeStatus.SUCCESS.value,
        child.node_id: NodeStatus.SUCCESS.value,
    }


def test_pipeline_downstream_becomes_stale_when_parent_output_changes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_2.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)
    forced_semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
    )

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        parent = engine.execute(
            "parent-changed-output",
            {"value": "stable-input"},
            lambda inputs: "foo",
            semantics,
        )

        with musubito_merge(parent):
            child = engine.execute(
                "child-of-changed-output",
                {"value": "child-input"},
                lambda inputs: f"child:{inputs['value']}",
                semantics,
            )

        reexecuted_parent = engine.execute(
            "parent-changed-output",
            {"value": "stable-input"},
            lambda inputs: "bar",
            forced_semantics,
        )

    assert reexecuted_parent.node_id == parent.node_id
    assert reexecuted_parent.artifact_id != parent.artifact_id
    assert _node_statuses_from_sql(db_path, (parent.node_id, child.node_id)) == {
        parent.node_id: NodeStatus.SUCCESS.value,
        child.node_id: NodeStatus.STALE.value,
    }


def test_terminal_node_reexecution_with_changed_output_updates_without_downstream(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_3.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)
    forced_semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
    )

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        first_result = engine.execute(
            "terminal-node",
            {"value": "stable-input"},
            lambda inputs: "foo",
            semantics,
        )
        second_result = engine.execute(
            "terminal-node",
            {"value": "stable-input"},
            lambda inputs: "bar",
            forced_semantics,
        )

    assert second_result.node_id == first_result.node_id
    assert second_result.artifact_id != first_result.artifact_id
    assert second_result.value == "bar"
    assert _node_statuses_from_sql(db_path, (second_result.node_id,)) == {
        second_result.node_id: NodeStatus.SUCCESS.value,
    }
    assert _count_nodes_from_sql(db_path) == 1


def test_failed_parent_reexecution_success_stales_existing_downstream(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_4.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)

        def failing_parent(inputs: Any) -> str:
            engine.execute(
                "downstream-created-before-parent-failure",
                {"value": inputs["value"]},
                lambda child_inputs: f"child:{child_inputs['value']}",
                semantics,
            )
            raise RuntimeError("parent failed")

        try:
            engine.execute(
                "failed-then-success-parent",
                {"value": "stable-input"},
                failing_parent,
                semantics,
            )
        except RuntimeError as exc:
            assert str(exc) == "parent failed"

        parent_node_id = _single_node_id_for_operation(db_path, "failed-then-success-parent")
        parent_after_failure_status = _node_statuses_from_sql(db_path, (parent_node_id,))[
            parent_node_id
        ]
        child_node_id = _single_node_id_for_operation(
            db_path,
            "downstream-created-before-parent-failure",
        )

        successful_parent = engine.execute(
            "failed-then-success-parent",
            {"value": "stable-input"},
            lambda inputs: "new-output",
            semantics,
        )

    assert parent_after_failure_status == NodeStatus.FAILED.value
    assert _node_statuses_from_sql(db_path, (successful_parent.node_id, child_node_id)) == {
        successful_parent.node_id: NodeStatus.SUCCESS.value,
        child_node_id: NodeStatus.STALE.value,
    }


def _node_statuses_from_sql(db_path: Path, node_ids: tuple[str, ...]) -> dict[str, str]:
    placeholders = ",".join("?" for _ in node_ids)
    with sqlite3.connect(str(db_path)) as connection:
        rows = connection.execute(
            f"""
            SELECT node_id, status
            FROM nodes
            WHERE node_id IN ({placeholders})
            """,
            node_ids,
        ).fetchall()

    return {str(node_id): str(status) for node_id, status in rows}


def _count_nodes_from_sql(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as connection:
        row: Any = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()

    return int(row[0])


def _single_node_id_for_operation(db_path: Path, operation_name: str) -> str:
    with sqlite3.connect(str(db_path)) as connection:
        rows = connection.execute(
            """
            SELECT node_id
            FROM nodes
            WHERE operation_name = ?
            """,
            (operation_name,),
        ).fetchall()

    assert len(rows) == 1
    return str(rows[0][0])
