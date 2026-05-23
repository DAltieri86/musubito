# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 Domenico Altieri <softwaretamrsv@gmail.com>
# This file is part of Musubito — https://github.com/daltieri86/musubito
"""Core immutable DAG ontology models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Generic, Optional, TypeAlias, TypeVar

from pydantic import BaseModel, ConfigDict

from musubito.semantics import StepConfiguration


class ArtifactType(str, Enum):
    """Artifact classes emitted or consumed by execution nodes."""

    PROMPT = "PROMPT"
    RESPONSE = "RESPONSE"
    TOOL_CALL = "TOOL_CALL"
    ERROR = "ERROR"


class NodeStatus(str, Enum):
    """Durable execution states for nodes registered in the DAG."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    STALE = "STALE"


class Artifact(BaseModel):
    """Immutable payload stored as part of lineage execution."""

    model_config = ConfigDict(frozen=True)

    artifact_id: str
    type: ArtifactType
    payload: Any


class ExecutionNode(BaseModel):
    """Immutable execution node registered in the current DAG."""

    model_config = ConfigDict(frozen=True)

    node_id: str
    input_hash: str
    operation_name: str
    status: NodeStatus
    semantics: StepConfiguration
    executed_at: datetime
    output_artifact_id: Optional[str] = None  # noqa: UP045


T = TypeVar("T")


class MusubitoResult(BaseModel, Generic[T]):
    """Typed value returned by a node execution or deterministic replay."""

    model_config = ConfigDict(frozen=True)

    value: T
    artifact_id: str
    producer_node_id: str
    node_id: str


MusubitoJsonObject: TypeAlias = dict[str, Any]
MusubitoJsonObjectResult: TypeAlias = MusubitoResult[MusubitoJsonObject]
