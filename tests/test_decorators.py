from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any, cast

import pytest

from musubito.decorators import musubito_step, use_musubito_engine
from musubito.engine import MusubitoEngine
from musubito.models import MusubitoResult
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage

EXPECTED_ADD_RESULT = 3
EXPECTED_DOUBLE_RESULT = 42


def test_musubito_step_sync_returns_intact_result_and_preserves_signature(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)

    @musubito_step(semantics)
    def add(left: int, right: int = 1) -> int:
        return left + right

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = add(2)

    assert isinstance(result, MusubitoResult)
    assert result.value == EXPECTED_ADD_RESULT
    signature = inspect.signature(add)
    assert list(signature.parameters) == ["left", "right"]
    assert signature.parameters["right"].default == 1


def test_musubito_step_async_routes_to_async_engine(tmp_path: Path) -> None:
    asyncio.run(_assert_async_decorator_routes_to_async_engine(tmp_path))


def test_musubito_step_instance_method_uses_explicit_receiver_identity(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "musubito.db"

    class Worker:
        def __init__(self, prefix: str) -> None:
            self.prefix = prefix

        def __musubito_identity__(self) -> dict[str, str]:
            return {"prefix": self.prefix}

        @musubito_step()
        def echo(self, value: str) -> str:
            return f"{self.prefix}:{value}"

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            first_result = Worker("first").echo("method-ok")
            second_result = Worker("second").echo("method-ok")

    assert isinstance(first_result, MusubitoResult)
    assert isinstance(second_result, MusubitoResult)
    assert first_result.value == "first:method-ok"
    assert second_result.value == "second:method-ok"
    assert first_result.node_id != second_result.node_id


def test_musubito_step_instance_method_rejects_ambiguous_receiver() -> None:
    class Worker:
        def __init__(self, prefix: str) -> None:
            self.prefix = prefix

        @musubito_step()
        def echo(self, value: str) -> str:
            return f"{self.prefix}:{value}"

    with pytest.raises(TypeError) as exc_info:
        Worker("unsafe").echo("method-fails")

    assert "must provide a deterministic __musubito_identity__() value" in str(exc_info.value)


async def _assert_async_decorator_routes_to_async_engine(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"

    @musubito_step()
    async def double(value: int) -> int:
        await asyncio.sleep(0)
        return value * 2

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = await double(21)
            replayed = await double(21)

    assert isinstance(result, MusubitoResult)
    assert result.value == EXPECTED_DOUBLE_RESULT
    assert replayed == result


def test_decorated_invalid_call_raises_signature_consistent_type_error() -> None:
    @musubito_step()
    def requires_two_arguments(left: int, right: int) -> int:
        return left + right

    invalid_call = cast(Any, requires_two_arguments)

    with pytest.raises(TypeError) as exc_info:
        invalid_call(1)

    assert "missing a required argument: 'right'" in str(exc_info.value)


def test_decorated_function_never_exposes_raw_value(tmp_path: Path) -> None:
    db_path = tmp_path / "musubito.db"

    @musubito_step()
    def build_payload(value: str) -> dict[str, str]:
        return {"value": value}

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = build_payload("visible-through-result")

    assert isinstance(result, MusubitoResult)
    assert result.value == {"value": "visible-through-result"}
