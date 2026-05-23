# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 Domenico Altieri <softwaretamrsv@gmail.com>
# This file is part of Musubito — https://github.com/daltieri86/musubito
"""Replay semantics for deterministic execution lineage."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class StepType(str, Enum):
    """Execution classes used to isolate deterministic replay boundaries."""

    DETERMINISTIC = "DETERMINISTIC"
    STOCHASTIC = "STOCHASTIC"
    EXTERNAL_EFFECT = "EXTERNAL_EFFECT"


class StepConfiguration(BaseModel):
    """Immutable replay policy for a single execution step."""

    model_config = ConfigDict(frozen=True)

    step_type: StepType
    force_reexecution: bool = False
    ttl_seconds: Optional[int] = Field(default=None, ge=0)  # noqa: UP045


def is_node_replayable(
    node_status: str,
    semantics: StepConfiguration,
    execution_time: datetime,
    now: datetime,
) -> bool:
    """Return whether a node can be replayed from recorded execution state."""

    if node_status == "STALE":
        return False

    if semantics.force_reexecution:
        return False

    if semantics.ttl_seconds is not None:
        elapsed_seconds = (now - execution_time).total_seconds()
        return elapsed_seconds < semantics.ttl_seconds

    return True
