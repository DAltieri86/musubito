from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from musubito.engine import current_execution_parents, override_upstream_parents

CONCURRENT_COROUTINE_COUNT = 50


@pytest.mark.asyncio
async def test_fifty_concurrent_coroutines_keep_contextvars_isolated() -> None:
    await asyncio.gather(
        *(
            _assert_contextvars_are_isolated_for_coroutine(worker_index)
            for worker_index in range(CONCURRENT_COROUTINE_COUNT)
        )
    )


@pytest.mark.asyncio
async def test_nested_coroutines_restore_parent_context_after_child_finishes() -> None:
    parent_current_parents = frozenset({"parent-current"})
    parent_override_parents = frozenset({"parent-override"})
    child_current_parents = frozenset({"child-current"})
    child_override_parents = frozenset({"child-override"})

    async def child_coroutine() -> None:
        with _temporary_execution_context(child_current_parents, child_override_parents):
            await asyncio.sleep(0)
            assert current_execution_parents.get() == child_current_parents
            assert override_upstream_parents.get() == child_override_parents

    async def parent_coroutine() -> None:
        with _temporary_execution_context(parent_current_parents, parent_override_parents):
            assert current_execution_parents.get() == parent_current_parents
            assert override_upstream_parents.get() == parent_override_parents

            await child_coroutine()

            assert current_execution_parents.get() == parent_current_parents
            assert override_upstream_parents.get() == parent_override_parents

    await parent_coroutine()

    assert current_execution_parents.get() == frozenset()
    assert override_upstream_parents.get() is None


@pytest.mark.asyncio
async def test_contextvars_return_to_defaults_after_fifty_coroutines_finish() -> None:
    await asyncio.gather(
        *(
            _assert_contextvars_are_isolated_for_coroutine(worker_index)
            for worker_index in range(CONCURRENT_COROUTINE_COUNT)
        )
    )

    assert current_execution_parents.get() == frozenset()
    assert override_upstream_parents.get() is None


async def _assert_contextvars_are_isolated_for_coroutine(worker_index: int) -> None:
    current_parents = frozenset({f"current-parent-{worker_index}"})
    override_parents = frozenset(
        {
            f"override-parent-{worker_index}-left",
            f"override-parent-{worker_index}-right",
        }
    )

    with _temporary_execution_context(current_parents, override_parents):
        await asyncio.sleep(0)
        assert current_execution_parents.get() == current_parents
        assert override_upstream_parents.get() == override_parents


@contextmanager
def _temporary_execution_context(
    current_parents: frozenset[str],
    override_parents: frozenset[str],
) -> Iterator[None]:
    current_token = current_execution_parents.set(current_parents)
    override_token = override_upstream_parents.set(override_parents)
    try:
        yield
    finally:
        override_upstream_parents.reset(override_token)
        current_execution_parents.reset(current_token)
