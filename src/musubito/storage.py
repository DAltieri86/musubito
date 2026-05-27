# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 Domenico Altieri <softwaretamrsv@gmail.com>
# This file is part of Musubito — https://github.com/daltieri86/musubito
"""SQLite persistence for the relational execution DAG."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from musubito.models import Artifact, ArtifactType, ExecutionNode, NodeStatus
from musubito.semantics import StepConfiguration

MAX_RECURSIVE_INVALIDATION_DEPTH = 10_000


class SQLiteStorage:
    """SQLite-backed persistence layer for artifacts, nodes, and DAG edges."""

    def __init__(self, db_path: str = ".musubito/musubito.db") -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection_lock = threading.RLock()
        self._connection = sqlite3.connect(
            str(self._db_path),
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._configure_connection()
        self._initialize_schema()

    def __enter__(self) -> SQLiteStorage:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @property
    def db_path(self) -> str:
        """Return the filesystem path backing this storage instance."""

        return str(self._db_path)

    def close(self) -> None:
        """Close the underlying SQLite connection."""

        with self._connection_lock:
            self._connection.close()

    def save_artifact(self, artifact: Artifact) -> None:
        """Persist an artifact by immutable identifier."""

        payload_json = _serialize_json(artifact.payload)
        artifact_type = artifact.type.value

        with self._write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO artifacts (artifact_id, type, payload_json)
                VALUES (?, ?, ?)
                ON CONFLICT(artifact_id) DO UPDATE SET
                    type = excluded.type,
                    payload_json = excluded.payload_json
                """,
                (artifact.artifact_id, artifact_type, payload_json),
            )

    def get_artifact(self, artifact_id: str) -> Artifact | None:
        """Return an artifact by identifier when present."""

        with self._connection_lock:
            row = self._connection.execute(
                """
                SELECT artifact_id, type, payload_json
                FROM artifacts
                WHERE artifact_id = ?
                """,
                (artifact_id,),
            ).fetchone()

        if row is None:
            return None

        return Artifact(
            artifact_id=str(row["artifact_id"]),
            type=ArtifactType(str(row["type"])),
            payload=_deserialize_json(str(row["payload_json"])),
        )

    def save_node(self, node: ExecutionNode) -> None:
        """Persist an execution node by deterministic node identifier."""

        semantics_json = _serialize_json(node.semantics.model_dump(mode="json"))
        executed_at = node.executed_at.isoformat()
        status = node.status.value

        with self._write_transaction() as connection:
            _upsert_node(
                connection,
                node,
                semantics_json=semantics_json,
                executed_at=executed_at,
                status=status,
            )

    def save_failed_execution(
        self,
        node: ExecutionNode,
        *,
        invalidate_downstream_if_previously_successful: bool = False,
    ) -> None:
        """Persist a failed execution and optionally stale old dependents atomically."""

        semantics_json = _serialize_json(node.semantics.model_dump(mode="json"))
        executed_at = node.executed_at.isoformat()
        status = node.status.value

        with self._write_transaction() as connection:
            previous_output_state = _get_node_output_state(connection, node.node_id)
            _upsert_node(
                connection,
                node,
                semantics_json=semantics_json,
                executed_at=executed_at,
                status=status,
            )
            if _should_invalidate_after_failure(
                previous_output_state,
                invalidate_downstream_if_previously_successful,
            ):
                _invalidate_downstream(connection, node.node_id)

    def save_execution(
        self,
        node: ExecutionNode,
        artifact: Artifact,
        upstream_node_ids: frozenset[str],
        *,
        invalidate_downstream_if_output_changed: bool = False,
        supersede_previous_successful_nodes: bool = False,
    ) -> None:
        """Persist an execution output, node, and parent edges atomically."""

        payload_json = _serialize_json(artifact.payload)
        artifact_type = artifact.type.value
        semantics_json = _serialize_json(node.semantics.model_dump(mode="json"))
        executed_at = node.executed_at.isoformat()
        status = node.status.value
        sorted_upstream_node_ids = sorted(upstream_node_ids)
        edge_rows = [
            (upstream_node_id, node.node_id) for upstream_node_id in sorted_upstream_node_ids
        ]

        with self._write_transaction() as connection:
            previous_output_state = _get_node_output_state(connection, node.node_id)
            superseded_output_states = _get_superseded_output_states(
                connection,
                node,
                upstream_node_ids,
                supersede_previous_successful_nodes,
            )
            connection.execute(
                """
                INSERT INTO artifacts (artifact_id, type, payload_json)
                VALUES (?, ?, ?)
                ON CONFLICT(artifact_id) DO UPDATE SET
                    type = excluded.type,
                    payload_json = excluded.payload_json
                """,
                (artifact.artifact_id, artifact_type, payload_json),
            )
            _upsert_node(
                connection,
                node,
                semantics_json=semantics_json,
                executed_at=executed_at,
                status=status,
            )
            connection.executemany(
                """
                INSERT OR IGNORE INTO edges (parent_node_id, child_node_id)
                VALUES (?, ?)
                """,
                edge_rows,
            )
            if _should_invalidate_downstream(
                previous_output_state,
                artifact.artifact_id,
                invalidate_downstream_if_output_changed,
            ):
                _invalidate_downstream(connection, node.node_id)
            for superseded_node_id, superseded_artifact_id in superseded_output_states:
                if superseded_artifact_id != artifact.artifact_id:
                    _invalidate_downstream(connection, superseded_node_id)

    def save_replayed_node(
        self,
        node: ExecutionNode,
        upstream_node_ids: frozenset[str],
    ) -> None:
        """Persist a current node that reuses an existing artifact."""

        semantics_json = _serialize_json(node.semantics.model_dump(mode="json"))
        executed_at = node.executed_at.isoformat()
        status = node.status.value
        edge_rows = [
            (upstream_node_id, node.node_id) for upstream_node_id in sorted(upstream_node_ids)
        ]

        with self._write_transaction() as connection:
            _upsert_node(
                connection,
                node,
                semantics_json=semantics_json,
                executed_at=executed_at,
                status=status,
            )
            connection.executemany(
                """
                INSERT OR IGNORE INTO edges (parent_node_id, child_node_id)
                VALUES (?, ?)
                """,
                edge_rows,
            )

    def get_node(self, node_id: str) -> ExecutionNode | None:
        """Return an execution node by identifier when present."""

        with self._connection_lock:
            row = self._connection.execute(
                """
                SELECT
                    node_id,
                    input_hash,
                    operation_name,
                    status,
                    semantics_json,
                    executed_at,
                    output_artifact_id
                FROM nodes
                WHERE node_id = ?
                """,
                (node_id,),
            ).fetchone()

        if row is None:
            return None

        return _node_from_row(row)

    def get_node_status(self, node_id: str) -> NodeStatus | None:
        """Return the current status for a node when present."""

        with self._connection_lock:
            row = self._connection.execute(
                """
                SELECT status
                FROM nodes
                WHERE node_id = ?
                """,
                (node_id,),
            ).fetchone()

        if row is None:
            return None

        return NodeStatus(str(row["status"]))

    def get_replay_candidate(
        self,
        *,
        operation_name: str,
        input_hash: str,
        excluded_node_id: str,
    ) -> ExecutionNode | None:
        """Return a historical successful node for the same operation and input."""

        with self._connection_lock:
            row = self._connection.execute(
                """
                SELECT
                    node_id,
                    input_hash,
                    operation_name,
                    status,
                    semantics_json,
                    executed_at,
                    output_artifact_id
                FROM nodes
                WHERE operation_name = ?
                    AND input_hash = ?
                    AND node_id <> ?
                    AND status = ?
                    AND output_artifact_id IS NOT NULL
                ORDER BY executed_at ASC, node_id ASC
                LIMIT 1
                """,
                (
                    operation_name,
                    input_hash,
                    excluded_node_id,
                    NodeStatus.SUCCESS.value,
                ),
            ).fetchone()

        if row is None:
            return None

        return _node_from_row(row)

    def get_successful_producer_node_id(self, artifact_id: str) -> str | None:
        """Return the first successful node that produced an artifact."""

        with self._connection_lock:
            row = self._connection.execute(
                """
                SELECT node_id
                FROM nodes
                WHERE output_artifact_id = ?
                    AND status = ?
                ORDER BY executed_at ASC, node_id ASC
                LIMIT 1
                """,
                (artifact_id, NodeStatus.SUCCESS.value),
            ).fetchone()

        if row is None:
            return None

        return str(row["node_id"])

    def save_edge(self, parent_node_id: str, child_node_id: str) -> None:
        """Persist a directed edge between two existing execution nodes."""

        with self._write_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO edges (parent_node_id, child_node_id)
                VALUES (?, ?)
                """,
                (parent_node_id, child_node_id),
            )

    def invalidate_downstream(self, node_id: str) -> None:
        """Mark every downstream descendant of a node as stale in place."""

        with self._write_transaction() as connection:
            _invalidate_downstream(connection, node_id)

    def _configure_connection(self) -> None:
        with self._connection_lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA busy_timeout=5000")
            self._connection.execute("PRAGMA foreign_keys=ON")

    def _initialize_schema(self) -> None:
        statements = (
            """
            CREATE TABLE IF NOT EXISTS artifacts (
                artifact_id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                input_hash TEXT NOT NULL,
                operation_name TEXT NOT NULL,
                status TEXT NOT NULL,
                semantics_json TEXT NOT NULL,
                executed_at TEXT NOT NULL,
                output_artifact_id TEXT,
                FOREIGN KEY (output_artifact_id)
                    REFERENCES artifacts(artifact_id)
                    ON DELETE SET NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS edges (
                parent_node_id TEXT NOT NULL,
                child_node_id TEXT NOT NULL,
                PRIMARY KEY (parent_node_id, child_node_id),
                FOREIGN KEY (parent_node_id)
                    REFERENCES nodes(node_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (child_node_id)
                    REFERENCES nodes(node_id)
                    ON DELETE CASCADE,
                CHECK (parent_node_id <> child_node_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_nodes_input_hash ON nodes(input_hash)",
            """
            CREATE INDEX IF NOT EXISTS idx_edges_parent_node_id
            ON edges(parent_node_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_edges_child_node_id
            ON edges(child_node_id)
            """,
        )

        with self._write_transaction() as connection:
            for statement in statements:
                connection.execute(statement)

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection_lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()


def _serialize_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _deserialize_json(value: str) -> Any:
    return json.loads(value)


def _get_node_output_state(
    connection: sqlite3.Connection,
    node_id: str,
) -> tuple[NodeStatus, str | None] | None:
    row = connection.execute(
        """
        SELECT status, output_artifact_id
        FROM nodes
        WHERE node_id = ?
        """,
        (node_id,),
    ).fetchone()

    if row is None:
        return None

    return NodeStatus(str(row["status"])), _optional_string(row["output_artifact_id"])


def _get_superseded_output_states(
    connection: sqlite3.Connection,
    node: ExecutionNode,
    upstream_node_ids: frozenset[str],
    supersede_previous_successful_nodes: bool,
) -> list[tuple[str, str]]:
    if not supersede_previous_successful_nodes:
        return []

    rows = connection.execute(
        """
        SELECT node_id, output_artifact_id
        FROM nodes
        WHERE operation_name = ?
            AND node_id <> ?
            AND status = ?
            AND output_artifact_id IS NOT NULL
        """,
        (node.operation_name, node.node_id, NodeStatus.SUCCESS.value),
    ).fetchall()

    superseded_output_states: list[tuple[str, str]] = []
    for row in rows:
        candidate_node_id = str(row["node_id"])
        if _get_parent_node_ids(connection, candidate_node_id) == upstream_node_ids:
            superseded_output_states.append((candidate_node_id, str(row["output_artifact_id"])))

    return superseded_output_states


def _get_parent_node_ids(
    connection: sqlite3.Connection,
    node_id: str,
) -> frozenset[str]:
    rows = connection.execute(
        """
        SELECT parent_node_id
        FROM edges
        WHERE child_node_id = ?
        """,
        (node_id,),
    ).fetchall()

    return frozenset(str(row["parent_node_id"]) for row in rows)


def _upsert_node(
    connection: sqlite3.Connection,
    node: ExecutionNode,
    *,
    semantics_json: str,
    executed_at: str,
    status: str,
) -> None:
    connection.execute(
        """
        INSERT INTO nodes (
            node_id,
            input_hash,
            operation_name,
            status,
            semantics_json,
            executed_at,
            output_artifact_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            input_hash = excluded.input_hash,
            operation_name = excluded.operation_name,
            status = excluded.status,
            semantics_json = excluded.semantics_json,
            executed_at = excluded.executed_at,
            output_artifact_id = excluded.output_artifact_id
        """,
        (
            node.node_id,
            node.input_hash,
            node.operation_name,
            status,
            semantics_json,
            executed_at,
            node.output_artifact_id,
        ),
    )


def _should_invalidate_downstream(
    previous_output_state: tuple[NodeStatus, str | None] | None,
    new_artifact_id: str,
    invalidate_downstream_if_output_changed: bool,
) -> bool:
    if not invalidate_downstream_if_output_changed:
        return False

    if previous_output_state is None:
        return False

    previous_status, previous_artifact_id = previous_output_state
    if previous_status is not NodeStatus.SUCCESS:
        return True

    if previous_artifact_id is None:
        return True

    return previous_artifact_id != new_artifact_id


def _should_invalidate_after_failure(
    previous_output_state: tuple[NodeStatus, str | None] | None,
    invalidate_downstream_if_previously_successful: bool,
) -> bool:
    if not invalidate_downstream_if_previously_successful:
        return False

    if previous_output_state is None:
        return False

    previous_status, previous_artifact_id = previous_output_state
    return previous_status is NodeStatus.SUCCESS and previous_artifact_id is not None


def _invalidate_downstream(connection: sqlite3.Connection, node_id: str) -> None:
    connection.execute(
        """
        WITH RECURSIVE downstream(node_id, path, depth) AS (
            SELECT
                e.child_node_id,
                ',' || e.parent_node_id || ',' || e.child_node_id || ',',
                1
            FROM edges AS e
            WHERE e.parent_node_id = ?

            UNION ALL

            SELECT
                e.child_node_id,
                d.path || e.child_node_id || ',',
                d.depth + 1
            FROM edges AS e
            JOIN downstream AS d
                ON e.parent_node_id = d.node_id
            WHERE d.path NOT LIKE '%,' || e.child_node_id || ',%'
                AND d.depth < ?
        )
        UPDATE nodes
        SET status = ?
        WHERE node_id IN (
            SELECT DISTINCT node_id
            FROM downstream
        )
        """,
        (node_id, MAX_RECURSIVE_INVALIDATION_DEPTH, NodeStatus.STALE.value),
    )


def _node_from_row(row: sqlite3.Row) -> ExecutionNode:
    semantics_data = _deserialize_json(str(row["semantics_json"]))
    semantics = StepConfiguration.model_validate(semantics_data)

    return ExecutionNode(
        node_id=str(row["node_id"]),
        input_hash=str(row["input_hash"]),
        operation_name=str(row["operation_name"]),
        status=NodeStatus(str(row["status"])),
        semantics=semantics,
        executed_at=datetime.fromisoformat(str(row["executed_at"])),
        output_artifact_id=_optional_string(row["output_artifact_id"]),
    )


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None

    return str(value)
