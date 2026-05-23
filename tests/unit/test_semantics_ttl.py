from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from musubito.semantics import StepConfiguration, StepType, is_node_replayable

BASE_EXECUTION_TIME = datetime(2026, 5, 23, 10, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("force_reexecution", "ttl_seconds"),
    [
        (False, None),
        (True, None),
        (False, 60),
        (True, 60),
        (True, 0),
    ],
)
def test_stale_status_is_never_replayable_regardless_of_force_or_ttl(
    force_reexecution: bool,
    ttl_seconds: int | None,
) -> None:
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=force_reexecution,
        ttl_seconds=ttl_seconds,
    )
    now = BASE_EXECUTION_TIME + timedelta(seconds=1)

    assert (
        is_node_replayable(
            "STALE",
            semantics,
            BASE_EXECUTION_TIME,
            now,
        )
        is False
    )


def test_force_reexecution_with_success_status_and_unexpired_ttl_is_not_replayable() -> None:
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        force_reexecution=True,
        ttl_seconds=60,
    )
    now = BASE_EXECUTION_TIME + timedelta(seconds=10)

    assert is_node_replayable("SUCCESS", semantics, BASE_EXECUTION_TIME, now) is False


def test_ttl_boundary_uses_strictly_less_than_ttl_seconds() -> None:
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        ttl_seconds=60,
    )

    assert (
        is_node_replayable(
            "SUCCESS",
            semantics,
            BASE_EXECUTION_TIME,
            BASE_EXECUTION_TIME + timedelta(seconds=59),
        )
        is True
    )
    assert (
        is_node_replayable(
            "SUCCESS",
            semantics,
            BASE_EXECUTION_TIME,
            BASE_EXECUTION_TIME + timedelta(seconds=60),
        )
        is False
    )
    assert (
        is_node_replayable(
            "SUCCESS",
            semantics,
            BASE_EXECUTION_TIME,
            BASE_EXECUTION_TIME + timedelta(seconds=61),
        )
        is False
    )


def test_zero_ttl_seconds_expires_immediately() -> None:
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        ttl_seconds=0,
    )

    assert (
        is_node_replayable("SUCCESS", semantics, BASE_EXECUTION_TIME, BASE_EXECUTION_TIME) is False
    )


def test_now_before_execution_time_is_replayable_without_exception() -> None:
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        ttl_seconds=60,
    )
    now = BASE_EXECUTION_TIME - timedelta(seconds=1)

    assert is_node_replayable("SUCCESS", semantics, BASE_EXECUTION_TIME, now) is True


def test_success_status_without_ttl_is_replayable() -> None:
    semantics = StepConfiguration(
        step_type=StepType.DETERMINISTIC,
        ttl_seconds=None,
    )
    now = BASE_EXECUTION_TIME + timedelta(days=30)

    assert is_node_replayable("SUCCESS", semantics, BASE_EXECUTION_TIME, now) is True


def test_stochastic_step_with_unexpired_ttl_and_no_force_reexecution_is_replayable() -> None:
    semantics = StepConfiguration(
        step_type=StepType.STOCHASTIC,
        force_reexecution=False,
        ttl_seconds=60,
    )
    now = BASE_EXECUTION_TIME + timedelta(seconds=10)

    assert is_node_replayable("SUCCESS", semantics, BASE_EXECUTION_TIME, now) is True
