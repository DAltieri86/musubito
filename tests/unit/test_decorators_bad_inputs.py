from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from musubito.decorators import musubito_step, use_musubito_engine
from musubito.engine import MusubitoEngine
from musubito.models import MusubitoResult
from musubito.storage import SQLiteStorage


class OpaqueInput:
    __slots__ = ("value",)

    def __init__(self, value: str) -> None:
        self.value = value


def test_set_argument_is_rejected_before_decorated_function_executes(
    tmp_path: Path,
) -> None:
    _assert_invalid_input_rejected_before_function_call(
        tmp_path,
        payload={"alpha", "beta"},
        expected_message_parts=("set",),
    )


def test_frozenset_argument_is_rejected_before_decorated_function_executes(
    tmp_path: Path,
) -> None:
    _assert_invalid_input_rejected_before_function_call(
        tmp_path,
        payload=frozenset({"alpha", "beta"}),
        expected_message_parts=("frozenset",),
    )


def test_lambda_argument_is_rejected_before_decorated_function_executes(
    tmp_path: Path,
) -> None:
    _assert_invalid_input_rejected_before_function_call(
        tmp_path,
        payload=lambda value: value,
        expected_message_parts=("function",),
    )


def test_custom_object_argument_is_rejected_before_decorated_function_executes(
    tmp_path: Path,
) -> None:
    _assert_invalid_input_rejected_before_function_call(
        tmp_path,
        payload=OpaqueInput("not-json-compatible"),
        expected_message_parts=("OpaqueInput",),
    )


def test_none_required_argument_is_accepted_as_valid_value(tmp_path: Path) -> None:
    db_path = tmp_path / "none_required_argument.db"

    @musubito_step()
    def accepts_none(payload: str | None) -> str:
        if payload is None:
            return "accepted-none"

        return payload

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            result = accepts_none(None)

    assert isinstance(result, MusubitoResult)
    assert result.value == "accepted-none"


def _assert_invalid_input_rejected_before_function_call(
    tmp_path: Path,
    *,
    payload: Any,
    expected_message_parts: tuple[str, ...],
) -> None:
    db_path = tmp_path / "invalid_input.db"
    was_called = False

    @musubito_step()
    def should_not_execute(payload: Any) -> str:
        nonlocal was_called
        was_called = True
        return f"executed:{payload}"

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine), pytest.raises(TypeError) as exc_info:
            should_not_execute(payload)

    error_message = str(exc_info.value)
    for expected_message_part in expected_message_parts:
        assert expected_message_part in error_message

    assert was_called is False
