from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from musubito.decorators import musubito_step, use_musubito_engine
from musubito.engine import MusubitoEngine
from musubito.models import MusubitoResult
from musubito.storage import SQLiteStorage

EXPECTED_ASYNC_INT_VALUE = 42


def test_sync_function_returning_str_returns_musubito_result(tmp_path: Path) -> None:
    db_path = tmp_path / "test_1.db"

    @musubito_step()
    def build_text(value: str) -> str:
        return f"text:{value}"

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = build_text("alpha")

    assert isinstance(result, MusubitoResult)
    assert isinstance(result.value, str)
    assert result.value == "text:alpha"


@pytest.mark.asyncio
async def test_async_function_returning_int_returns_musubito_result(tmp_path: Path) -> None:
    db_path = tmp_path / "test_2.db"

    @musubito_step()
    async def build_number(value: int) -> int:
        await asyncio.sleep(0)
        return value * 2

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = await build_number(21)

    assert isinstance(result, MusubitoResult)
    assert isinstance(result.value, int)
    assert result.value == EXPECTED_ASYNC_INT_VALUE


def test_function_returning_none_returns_musubito_result_with_none_value(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_3.db"

    @musubito_step()
    def return_none(value: str) -> None:
        assert value == "ignored"

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = return_none("ignored")

    assert isinstance(result, MusubitoResult)
    assert result.value is None


def test_function_returning_complex_dict_returns_musubito_result(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test_4.db"
    expected_value = {
        "summary": {"status": "ok", "score": 0.98},
        "items": [
            {"id": "a", "enabled": True},
            {"id": "b", "enabled": False},
        ],
        "metadata": {"tokens": 12, "warnings": None},
    }

    @musubito_step()
    def build_complex_dict(value: str) -> dict[str, object]:
        return {
            "summary": {"status": value, "score": 0.98},
            "items": [
                {"id": "a", "enabled": True},
                {"id": "b", "enabled": False},
            ],
            "metadata": {"tokens": 12, "warnings": None},
        }

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = build_complex_dict("ok")

    assert isinstance(result, MusubitoResult)
    assert isinstance(result.value, dict)
    assert result.value == expected_value


def test_function_raising_exception_reraises_original_exception(tmp_path: Path) -> None:
    db_path = tmp_path / "test_5.db"

    @musubito_step()
    def fail(value: str) -> str:
        raise ValueError(f"cannot build {value}")

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine), pytest.raises(ValueError) as exc_info:
            fail("payload")

    assert str(exc_info.value) == "cannot build payload"


def test_musubito_result_value_exposes_original_return_value(tmp_path: Path) -> None:
    db_path = tmp_path / "test_6.db"
    expected_value: dict[str, object] = {"result": "visible", "count": 3}

    @musubito_step()
    def build_value() -> dict[str, object]:
        return expected_value

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = build_value()

    assert isinstance(result, MusubitoResult)
    assert result.value == expected_value
