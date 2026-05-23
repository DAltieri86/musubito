# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 Domenico Altieri <softwaretamrsv@gmail.com>
# This file is part of Musubito — https://github.com/daltieri86/musubito
"""Execution orchestration for deterministic DAG replay."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict

from musubito.hashing import compute_node_id, generate_deterministic_hash
from musubito.models import Artifact, ArtifactType, ExecutionNode, MusubitoResult, NodeStatus
from musubito.semantics import StepConfiguration, is_node_replayable
from musubito.storage import SQLiteStorage

current_execution_parents: ContextVar[frozenset[str]] = ContextVar(
    "current_execution_parents",
    default=frozenset(),
)
override_upstream_parents: ContextVar[frozenset[str] | None] = ContextVar(
    "override_upstream_parents",
    default=None,
)

SyncOperation = Callable[[Any], Any]
AsyncOperation = Callable[[Any], Awaitable[Any]]
NowProvider = Callable[[], datetime]


class PreparedNode(BaseModel):
    """Prepared deterministic node identity and existing persisted state."""

    model_config = ConfigDict(frozen=True)

    node_id: str
    input_hash: str
    operation_name: str
    upstream_node_ids: frozenset[str]
    existing_node: ExecutionNode | None


@contextmanager
def musubito_merge(*dependencies: Any) -> Iterator[None]:
    """Override upstream parents with producer nodes from Musubito results."""

    upstream_node_ids = frozenset(
        dependency.producer_node_id
        for dependency in dependencies
        if isinstance(dependency, MusubitoResult)
    )
    token = override_upstream_parents.set(upstream_node_ids)
    try:
        yield
    finally:
        override_upstream_parents.reset(token)


class MusubitoEngine:
    """Coordinate deterministic execution, replay, and lineage persistence."""

    def __init__(
        self,
        storage: SQLiteStorage,
        *,
        now_provider: NowProvider | None = None,
    ) -> None:
        self._storage = storage
        self._now_provider = now_provider or _utc_now

    def execute(
        self,
        operation_name: str,
        inputs: Any,
        func: SyncOperation,
        semantics: StepConfiguration,
    ) -> MusubitoResult[Any]:
        """Execute or replay a synchronous operation."""

        prepared_node = self._prepare_node(operation_name, inputs)
        token = current_execution_parents.set(frozenset([prepared_node.node_id]))
        try:
            return self._replay_or_run(prepared_node, inputs, func, semantics)
        finally:
            current_execution_parents.reset(token)

    async def execute_async(
        self,
        operation_name: str,
        inputs: Any,
        func: SyncOperation | AsyncOperation,
        semantics: StepConfiguration,
    ) -> MusubitoResult[Any]:
        """Execute or replay an asynchronous operation."""

        prepared_node = self._prepare_node(operation_name, inputs)
        token = current_execution_parents.set(frozenset([prepared_node.node_id]))
        try:
            replayed_result = self._replay(prepared_node, semantics)
            if replayed_result is not None:
                return replayed_result

            return await self._execute_node_async(prepared_node, inputs, func, semantics)
        finally:
            current_execution_parents.reset(token)

    def _prepare_node(self, operation_name: str, inputs: Any) -> PreparedNode:
        input_hash = generate_deterministic_hash(inputs)
        upstream_node_ids = _resolve_upstream_node_ids()
        node_id = compute_node_id(operation_name, input_hash, upstream_node_ids)
        existing_node = self._storage.get_node(node_id)

        return PreparedNode(
            node_id=node_id,
            input_hash=input_hash,
            operation_name=operation_name,
            upstream_node_ids=upstream_node_ids,
            existing_node=existing_node,
        )

    def _execute_node(
        self,
        prepared_node: PreparedNode,
        inputs: Any,
        func: SyncOperation,
        semantics: StepConfiguration,
    ) -> MusubitoResult[Any]:
        self._materialize_node_before_user_code(prepared_node, semantics)
        try:
            operation_output = func(inputs)
            if inspect.isawaitable(operation_output):
                msg = "Synchronous execute received an awaitable operation result."
                raise TypeError(msg)
        except BaseException:
            self._persist_failed_output(prepared_node, semantics)
            raise

        return self._persist_successful_output(prepared_node, operation_output, semantics)

    def _replay_or_run(
        self,
        prepared_node: PreparedNode,
        inputs: Any,
        func: SyncOperation,
        semantics: StepConfiguration,
    ) -> MusubitoResult[Any]:
        replayed_result = self._replay(prepared_node, semantics)
        if replayed_result is not None:
            return replayed_result

        return self._execute_node(prepared_node, inputs, func, semantics)

    async def _execute_node_async(
        self,
        prepared_node: PreparedNode,
        inputs: Any,
        func: SyncOperation | AsyncOperation,
        semantics: StepConfiguration,
    ) -> MusubitoResult[Any]:
        self._materialize_node_before_user_code(prepared_node, semantics)
        try:
            operation_output = func(inputs)
            if inspect.isawaitable(operation_output):
                operation_output = await operation_output
        except BaseException:
            self._persist_failed_output(prepared_node, semantics)
            raise

        return self._persist_successful_output(prepared_node, operation_output, semantics)

    def _replay(
        self,
        prepared_node: PreparedNode,
        semantics: StepConfiguration,
    ) -> MusubitoResult[Any] | None:
        replay_node = prepared_node.existing_node
        should_materialize_current_node = False
        if replay_node is None:
            replay_node = self._storage.get_replay_candidate(
                operation_name=prepared_node.operation_name,
                input_hash=prepared_node.input_hash,
                excluded_node_id=prepared_node.node_id,
            )
            should_materialize_current_node = replay_node is not None

        if replay_node is None or replay_node.output_artifact_id is None:
            return None

        if replay_node.status is not NodeStatus.SUCCESS:
            return None

        if not is_node_replayable(
            replay_node.status.value,
            semantics,
            replay_node.executed_at,
            self._now_provider(),
        ):
            return None

        artifact = self._storage.get_artifact(replay_node.output_artifact_id)
        if artifact is None:
            return None

        if should_materialize_current_node:
            current_node = ExecutionNode(
                node_id=prepared_node.node_id,
                input_hash=prepared_node.input_hash,
                operation_name=prepared_node.operation_name,
                status=NodeStatus.SUCCESS,
                semantics=semantics,
                executed_at=self._now_provider(),
                output_artifact_id=artifact.artifact_id,
            )
            self._storage.save_replayed_node(
                current_node,
                prepared_node.upstream_node_ids,
            )

        producer_node_id = (
            self._storage.get_successful_producer_node_id(artifact.artifact_id)
            or replay_node.node_id
        )

        return MusubitoResult[Any](
            value=artifact.payload,
            artifact_id=artifact.artifact_id,
            producer_node_id=producer_node_id,
            node_id=prepared_node.node_id,
        )

    def _persist_successful_output(
        self,
        prepared_node: PreparedNode,
        value: Any,
        semantics: StepConfiguration,
    ) -> MusubitoResult[Any]:
        artifact = Artifact(
            artifact_id=_compute_output_artifact_id(prepared_node.node_id, value),
            type=ArtifactType.RESPONSE,
            payload=value,
        )
        node = ExecutionNode(
            node_id=prepared_node.node_id,
            input_hash=prepared_node.input_hash,
            operation_name=prepared_node.operation_name,
            status=NodeStatus.SUCCESS,
            semantics=semantics,
            executed_at=self._now_provider(),
            output_artifact_id=artifact.artifact_id,
        )

        self._storage.save_execution(
            node,
            artifact,
            prepared_node.upstream_node_ids,
            invalidate_downstream_if_output_changed=True,
            supersede_previous_successful_nodes=True,
        )

        producer_node_id = (
            self._storage.get_successful_producer_node_id(artifact.artifact_id) or node.node_id
        )

        return MusubitoResult[Any](
            value=value,
            artifact_id=artifact.artifact_id,
            producer_node_id=producer_node_id,
            node_id=prepared_node.node_id,
        )

    def _materialize_node_before_user_code(
        self,
        prepared_node: PreparedNode,
        semantics: StepConfiguration,
    ) -> None:
        if prepared_node.existing_node is not None:
            return

        pending_node = ExecutionNode(
            node_id=prepared_node.node_id,
            input_hash=prepared_node.input_hash,
            operation_name=prepared_node.operation_name,
            status=NodeStatus.FAILED,
            semantics=semantics,
            executed_at=self._now_provider(),
            output_artifact_id=None,
        )
        self._storage.save_failed_execution(pending_node)

    def _persist_failed_output(
        self,
        prepared_node: PreparedNode,
        semantics: StepConfiguration,
    ) -> None:
        failed_node = ExecutionNode(
            node_id=prepared_node.node_id,
            input_hash=prepared_node.input_hash,
            operation_name=prepared_node.operation_name,
            status=NodeStatus.FAILED,
            semantics=semantics,
            executed_at=self._now_provider(),
            output_artifact_id=None,
        )
        self._storage.save_failed_execution(
            failed_node,
            invalidate_downstream_if_previously_successful=True,
        )


def _resolve_upstream_node_ids() -> frozenset[str]:
    overridden_upstream_node_ids = override_upstream_parents.get()
    if overridden_upstream_node_ids is not None:
        return overridden_upstream_node_ids

    return current_execution_parents.get()


def _compute_output_artifact_id(_node_id: str, value: Any) -> str:
    return generate_deterministic_hash(
        {
            "type": ArtifactType.RESPONSE.value,
            "value": value,
        }
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
