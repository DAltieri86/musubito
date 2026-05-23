from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any, cast

import pytest

from musubito.decorators import musubito_step, use_musubito_engine
from musubito.engine import MusubitoEngine
from musubito.storage import SQLiteStorage


def test_musubito_step_preserves_sync_async_and_method_signatures() -> None:
    def original_sync(left: int, /, middle: str, *, right: int = 7) -> str:
        return f"{left}:{middle}:{right}"

    async def original_async(left: int, middle: str = "default", *, right: int = 9) -> str:
        await asyncio.sleep(0)
        return f"{left}:{middle}:{right}"

    def original_method(self: Any, left: int, *, right: int = 11) -> str:
        return f"{self}:{left}:{right}"

    class Worker:
        method = musubito_step()(original_method)

    decorated_sync = musubito_step()(original_sync)
    decorated_async = musubito_step()(original_async)

    assert inspect.signature(decorated_sync) == inspect.signature(original_sync)
    assert inspect.signature(decorated_async) == inspect.signature(original_async)
    assert inspect.signature(Worker.method) == inspect.signature(original_method)


def test_musubito_step_missing_required_argument_raises_signature_type_error() -> None:
    def original(left: int, right: int) -> int:
        return left + right

    decorated = musubito_step()(original)
    invalid_call = cast(Any, decorated)

    with pytest.raises(TypeError) as exc_info:
        invalid_call(1)

    assert "missing a required argument: 'right'" in str(exc_info.value)


def test_musubito_step_apply_defaults_includes_default_value_in_input_hash(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "apply_defaults.db"

    @musubito_step()
    def with_default(required: str, defaulted: int = 42) -> dict[str, int | str]:
        return {"required": required, "defaulted": defaulted}

    with SQLiteStorage(str(db_path)) as storage:
        engine = MusubitoEngine(storage)
        with use_musubito_engine(engine):
            implicit_default_result = with_default("same-input")
            explicit_default_result = with_default("same-input", defaulted=42)
            different_default_result = with_default("same-input", defaulted=43)

    assert implicit_default_result.node_id == explicit_default_result.node_id
    assert implicit_default_result.node_id != different_default_result.node_id


def test_musubito_step_exposes_wrapped_original_function() -> None:
    def original(value: int) -> int:
        return value + 1

    decorated = musubito_step()(original)
    decorated_runtime = cast(Any, decorated)

    assert decorated_runtime.__wrapped__ is original
    assert decorated_runtime.__name__ == original.__name__
    assert decorated_runtime.__doc__ == original.__doc__
