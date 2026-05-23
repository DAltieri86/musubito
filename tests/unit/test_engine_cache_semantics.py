from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from musubito.engine import MusubitoEngine, musubito_merge
from musubito.models import NodeStatus
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage

EXPECTED_SHARED_STEP_NODE_COUNT_AFTER_REPLAY = 2


def test_cache_miss_returns_current_node_as_producer_and_persists_success(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_1.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        result = engine.execute(
            "cache-miss",
            {"value": "alpha"},
            lambda inputs: {"echo": inputs["value"]},
            semantics,
        )

    assert result.node_id == result.producer_node_id
    assert _node_status(db_path, result.node_id) == NodeStatus.SUCCESS.value


def test_cache_hit_for_same_input_reuses_existing_node_without_duplicate(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_2.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        first_result = engine.execute(
            "cache-hit",
            {"value": "stable"},
            lambda inputs: {"echo": inputs["value"]},
            semantics,
        )
        second_result = engine.execute(
            "cache-hit",
            {"value": "stable"},
            lambda inputs: {"echo": inputs["value"]},
            semantics,
        )

    assert second_result.node_id == first_result.producer_node_id
    assert second_result.producer_node_id == first_result.producer_node_id
    assert _count_nodes(db_path) == 1
    assert _node_status(db_path, first_result.node_id) == NodeStatus.SUCCESS.value


def test_historical_replay_uses_historical_producer_without_staling_downstream(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_3.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        historical_parent = engine.execute(
            "historical-parent",
            {"value": "old-parent"},
            lambda inputs: {"parent": inputs["value"]},
            semantics,
        )
        current_parent = engine.execute(
            "current-parent",
            {"value": "new-parent"},
            lambda inputs: {"parent": inputs["value"]},
            semantics,
        )

        with musubito_merge(historical_parent):
            historical_result = engine.execute(
                "shared-deterministic-step",
                {"prompt": "same-input"},
                lambda inputs: {"answer": inputs["prompt"]},
                semantics,
            )

        with musubito_merge(historical_result):
            historical_child = engine.execute(
                "historical-child",
                {"value": "depends-on-historical"},
                lambda inputs: {"child": inputs["value"]},
                semantics,
            )

        with musubito_merge(current_parent):
            replayed_result = engine.execute(
                "shared-deterministic-step",
                {"prompt": "same-input"},
                lambda inputs: {"answer": f"not-used-{inputs['prompt']}"},
                semantics,
            )

    assert replayed_result.node_id != replayed_result.producer_node_id
    assert replayed_result.producer_node_id == historical_result.node_id
    assert replayed_result.artifact_id == historical_result.artifact_id
    assert replayed_result.value == historical_result.value
    assert _node_status(db_path, replayed_result.node_id) == NodeStatus.SUCCESS.value
    assert _node_status(db_path, historical_child.node_id) == NodeStatus.SUCCESS.value
    assert (
        _count_nodes_for_operation(db_path, "shared-deterministic-step")
        == EXPECTED_SHARED_STEP_NODE_COUNT_AFTER_REPLAY
    )


def test_cache_miss_after_input_change_creates_new_node_and_stales_old_downstream(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_4.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        first_result = engine.execute(
            "superseded-parent",
            {"value": "old"},
            lambda inputs: {"parent": inputs["value"]},
            semantics,
        )

        with musubito_merge(first_result):
            old_downstream = engine.execute(
                "old-downstream",
                {"value": "child"},
                lambda inputs: {"child": inputs["value"]},
                semantics,
            )

        second_result = engine.execute(
            "superseded-parent",
            {"value": "new"},
            lambda inputs: {"parent": inputs["value"]},
            semantics,
        )

    assert second_result.node_id != first_result.node_id
    assert second_result.producer_node_id == second_result.node_id
    assert second_result.artifact_id != first_result.artifact_id
    assert _node_status(db_path, first_result.node_id) == NodeStatus.SUCCESS.value
    assert _node_status(db_path, second_result.node_id) == NodeStatus.SUCCESS.value
    assert _node_status(db_path, old_downstream.node_id) == NodeStatus.STALE.value


def _node_status(db_path: Path, node_id: str) -> str | None:
    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            """
            SELECT status
            FROM nodes
            WHERE node_id = ?
            """,
            (node_id,),
        ).fetchone()

    if row is None:
        return None

    return str(row[0])


def _count_nodes(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as connection:
        row: Any = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()

    return int(row[0])


def _count_nodes_for_operation(db_path: Path, operation_name: str) -> int:
    with sqlite3.connect(str(db_path)) as connection:
        row: Any = connection.execute(
            """
            SELECT COUNT(*)
            FROM nodes
            WHERE operation_name = ?
            """,
            (operation_name,),
        ).fetchone()

    return int(row[0])
