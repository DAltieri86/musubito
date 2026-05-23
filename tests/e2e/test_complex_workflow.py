from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from musubito.decorators import musubito_step, use_musubito_engine
from musubito.engine import MusubitoEngine, musubito_merge
from musubito.models import MusubitoResult, NodeStatus
from musubito.storage import SQLiteStorage

EXPECTED_NODE_COUNT = 3
EXPECTED_EDGE_COUNT = 2


def test_complex_async_workflow_with_merge_and_db_verification(tmp_path: Path) -> None:
    asyncio.run(_assert_complex_async_workflow(tmp_path))


async def _assert_complex_async_workflow(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"

    @musubito_step()
    async def fetch_left(seed: int) -> dict[str, int | str]:
        await asyncio.sleep(0)
        return {"branch": "left", "value": seed + 1}

    @musubito_step()
    async def fetch_right(seed: int) -> dict[str, int | str]:
        await asyncio.sleep(0)
        return {"branch": "right", "value": seed + 2}

    @musubito_step()
    def aggregate(
        left: MusubitoResult[dict[str, int | str]],
        right: MusubitoResult[dict[str, int | str]],
    ) -> dict[str, Any]:
        return {
            "branches": [left.value["branch"], right.value["branch"]],
            "total": int(left.value["value"]) + int(right.value["value"]),
        }

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            left_result, right_result = await asyncio.gather(fetch_left(10), fetch_right(20))

            with musubito_merge(left_result, right_result):
                aggregate_result = aggregate(left_result, right_result)

        stored_artifact = storage.get_artifact(aggregate_result.artifact_id)
        aggregate_node = storage.get_node(aggregate_result.node_id)

    assert aggregate_result.value == {"branches": ["left", "right"], "total": 33}
    assert stored_artifact is not None
    assert stored_artifact.payload == aggregate_result.value
    assert aggregate_node is not None
    assert aggregate_node.status is NodeStatus.SUCCESS
    assert _parent_node_ids(str(db_path), aggregate_result.node_id) == {
        left_result.producer_node_id,
        right_result.producer_node_id,
    }
    assert _node_count(str(db_path)) == EXPECTED_NODE_COUNT
    assert _edge_count(str(db_path)) == EXPECTED_EDGE_COUNT


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


def _node_count(db_path: str) -> int:
    with sqlite3.connect(db_path) as connection:
        row = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()

    return int(row[0])


def _edge_count(db_path: str) -> int:
    with sqlite3.connect(db_path) as connection:
        row = connection.execute("SELECT COUNT(*) FROM edges").fetchone()

    return int(row[0])
