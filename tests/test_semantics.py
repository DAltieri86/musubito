from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from musubito.semantics import StepConfiguration, StepType, is_node_replayable


def test_step_configuration_is_frozen() -> None:
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    with pytest.raises(ValidationError):
        semantics.force_reexecution = True


def test_step_configuration_rejects_negative_ttl() -> None:
    with pytest.raises(ValidationError):
        StepConfiguration(step_type=StepType.DETERMINISTIC, ttl_seconds=-1)


def test_stale_node_is_not_replayable() -> None:
    execution_time = datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc)
    now = datetime(2026, 5, 22, 10, 1, tzinfo=timezone.utc)
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    assert not is_node_replayable("STALE", semantics, execution_time, now)


def test_force_reexecution_makes_node_not_replayable() -> None:
    execution_time = datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc)
    now = datetime(2026, 5, 22, 10, 1, tzinfo=timezone.utc)
    semantics = StepConfiguration(
        step_type=StepType.STOCHASTIC,
        force_reexecution=True,
    )

    assert not is_node_replayable("COMPLETED", semantics, execution_time, now)


def test_node_is_replayable_without_ttl_when_not_stale_or_forced() -> None:
    execution_time = datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc)
    now = datetime(2026, 5, 23, 10, 0, tzinfo=timezone.utc)
    semantics = StepConfiguration(step_type=StepType.EXTERNAL_EFFECT)

    assert is_node_replayable("COMPLETED", semantics, execution_time, now)


def test_node_is_replayable_before_ttl_expiration() -> None:
    execution_time = datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc)
    now = execution_time + timedelta(seconds=59)
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        ttl_seconds=60,
    )

    assert is_node_replayable("COMPLETED", semantics, execution_time, now)


def test_node_is_not_replayable_at_ttl_boundary() -> None:
    execution_time = datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc)
    now = execution_time + timedelta(seconds=60)
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        ttl_seconds=60,
    )

    assert not is_node_replayable("COMPLETED", semantics, execution_time, now)
