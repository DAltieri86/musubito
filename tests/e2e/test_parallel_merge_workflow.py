from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from musubito.decorators import musubito_step, use_musubito_engine
from musubito.engine import MusubitoEngine, musubito_merge
from musubito.models import MusubitoResult, NodeStatus
from musubito.storage import SQLiteStorage

EXPECTED_EDGE_COUNT = 2


@pytest.mark.asyncio
async def test_parallel_async_branches_merge_and_replay_from_cache(tmp_path: Path) -> None:
    db_path = tmp_path / "parallel_merge.db"
    execution_counts = {"step_a": 0, "step_b": 0, "step_c": 0}

    @musubito_step()
    async def step_a(seed: int) -> dict[str, int | str]:
        execution_counts["step_a"] += 1
        await asyncio.sleep(0)
        return {"branch": "a", "value": seed + 1}

    @musubito_step()
    async def step_b(seed: int) -> dict[str, int | str]:
        execution_counts["step_b"] += 1
        await asyncio.sleep(0)
        return {"branch": "b", "value": seed + 2}

    @musubito_step()
    def step_c(
        result_a: MusubitoResult[dict[str, int | str]],
        result_b: MusubitoResult[dict[str, int | str]],
    ) -> dict[str, Any]:
        execution_counts["step_c"] += 1
        return {
            "branches": [result_a.value["branch"], result_b.value["branch"]],
            "total": int(result_a.value["value"]) + int(result_b.value["value"]),
        }

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            first_a, first_b, first_c = await _run_pipeline(step_a, step_b, step_c)
            first_execution_counts = dict(execution_counts)
            second_a, second_b, second_c = await _run_pipeline(step_a, step_b, step_c)

    assert _node_status_from_sql(db_path, first_c.node_id) == NodeStatus.SUCCESS.value
    assert _parent_node_ids_from_sql(db_path, first_c.node_id) == {
        first_a.producer_node_id,
        first_b.producer_node_id,
    }
    assert first_c.value == {"branches": ["a", "b"], "total": 33}
    assert _edge_count_from_sql(db_path) == EXPECTED_EDGE_COUNT
    assert execution_counts == first_execution_counts
    assert second_a == first_a
    assert second_b == first_b
    assert second_c == first_c


async def _run_pipeline(
    step_a: Any,
    step_b: Any,
    step_c: Any,
) -> tuple[
    MusubitoResult[dict[str, int | str]],
    MusubitoResult[dict[str, int | str]],
    MusubitoResult[dict[str, Any]],
]:
    result_a, result_b = await asyncio.gather(step_a(10), step_b(20))

    with musubito_merge(result_a, result_b):
        result_c = step_c(result_a, result_b)

    return result_a, result_b, result_c


def _node_status_from_sql(db_path: Path, node_id: str) -> str | None:
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


def _parent_node_ids_from_sql(db_path: Path, child_node_id: str) -> set[str]:
    with sqlite3.connect(str(db_path)) as connection:
        rows = connection.execute(
            """
            SELECT parent_node_id
            FROM edges
            WHERE child_node_id = ?
            """,
            (child_node_id,),
        ).fetchall()

    return {str(row[0]) for row in rows}


def _edge_count_from_sql(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute("SELECT COUNT(*) FROM edges").fetchone()

    return int(row[0])
