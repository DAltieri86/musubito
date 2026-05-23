from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from musubito.decorators import musubito_step, use_musubito_engine
from musubito.engine import MusubitoEngine, musubito_merge
from musubito.models import MusubitoResult, NodeStatus
from musubito.storage import SQLiteStorage

EXPECTED_INITIAL_NODE_COUNT = 3
EXPECTED_INITIAL_EDGE_COUNT = 2
EXPECTED_CASCADE_NODE_COUNT = 6
EXPECTED_CASCADE_EDGE_COUNT = 4


def test_replay_persists_across_engine_sessions_and_cascades_after_input_change(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "persist.db"
    execution_counts = {"step_a": 0, "step_b": 0, "step_c": 0}

    @musubito_step()
    def step_a(seed: int) -> dict[str, int | str]:
        execution_counts["step_a"] += 1
        return {"step": "a", "value": seed + 1}

    @musubito_step()
    def step_b(result_a: MusubitoResult[dict[str, int | str]]) -> dict[str, int | str]:
        execution_counts["step_b"] += 1
        return {
            "step": "b",
            "value": int(result_a.value["value"]) * 2,
        }

    @musubito_step()
    def step_c(result_b: MusubitoResult[dict[str, int | str]]) -> dict[str, int | str]:
        execution_counts["step_c"] += 1
        return {
            "step": "c",
            "value": int(result_b.value["value"]) + 3,
        }

    with SQLiteStorage(str(db_path)) as storage:
        session_1_engine = MusubitoEngine(storage)
        with use_musubito_engine(session_1_engine):
            session_1_results = _run_pipeline(step_a, step_b, step_c, seed=10)

    session_1_counts = dict(execution_counts)

    with SQLiteStorage(str(db_path)) as storage:
        session_2_engine = MusubitoEngine(storage)
        with use_musubito_engine(session_2_engine):
            session_2_results = _run_pipeline(step_a, step_b, step_c, seed=10)

            assert _result_node_ids(session_2_results) == _result_node_ids(session_1_results)
            assert execution_counts == session_1_counts
            assert _result_values(session_2_results) == _result_values(session_1_results)
            assert _node_count(db_path) == EXPECTED_INITIAL_NODE_COUNT
            assert _edge_count(db_path) == EXPECTED_INITIAL_EDGE_COUNT

            changed_results = _run_pipeline(step_a, step_b, step_c, seed=20)

    assert execution_counts == {
        "step_a": session_1_counts["step_a"] + 1,
        "step_b": session_1_counts["step_b"] + 1,
        "step_c": session_1_counts["step_c"] + 1,
    }
    assert changed_results.step_a.node_id != session_1_results.step_a.node_id
    assert changed_results.step_b.node_id != session_1_results.step_b.node_id
    assert changed_results.step_c.node_id != session_1_results.step_c.node_id
    assert changed_results.step_c.value == {"step": "c", "value": 45}
    assert _node_statuses(
        db_path,
        (
            session_1_results.step_a.node_id,
            session_1_results.step_b.node_id,
            session_1_results.step_c.node_id,
            changed_results.step_a.node_id,
            changed_results.step_b.node_id,
            changed_results.step_c.node_id,
        ),
    ) == {
        session_1_results.step_a.node_id: NodeStatus.SUCCESS.value,
        session_1_results.step_b.node_id: NodeStatus.STALE.value,
        session_1_results.step_c.node_id: NodeStatus.STALE.value,
        changed_results.step_a.node_id: NodeStatus.SUCCESS.value,
        changed_results.step_b.node_id: NodeStatus.SUCCESS.value,
        changed_results.step_c.node_id: NodeStatus.SUCCESS.value,
    }
    assert _node_count(db_path) == EXPECTED_CASCADE_NODE_COUNT
    assert _edge_count(db_path) == EXPECTED_CASCADE_EDGE_COUNT


class PipelineResults:
    def __init__(
        self,
        step_a: MusubitoResult[dict[str, int | str]],
        step_b: MusubitoResult[dict[str, int | str]],
        step_c: MusubitoResult[dict[str, int | str]],
    ) -> None:
        self.step_a = step_a
        self.step_b = step_b
        self.step_c = step_c


def _run_pipeline(
    step_a: Any,
    step_b: Any,
    step_c: Any,
    *,
    seed: int,
) -> PipelineResults:
    result_a = step_a(seed)
    with musubito_merge(result_a):
        result_b = step_b(result_a)

    with musubito_merge(result_b):
        result_c = step_c(result_b)

    return PipelineResults(result_a, result_b, result_c)


def _result_node_ids(results: PipelineResults) -> tuple[str, str, str]:
    return (
        results.step_a.node_id,
        results.step_b.node_id,
        results.step_c.node_id,
    )


def _result_values(
    results: PipelineResults,
) -> tuple[dict[str, int | str], dict[str, int | str], dict[str, int | str]]:
    return (
        results.step_a.value,
        results.step_b.value,
        results.step_c.value,
    )


def _node_count(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()

    return int(row[0])


def _edge_count(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute("SELECT COUNT(*) FROM edges").fetchone()

    return int(row[0])


def _node_statuses(db_path: Path, node_ids: tuple[str, ...]) -> dict[str, str]:
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
