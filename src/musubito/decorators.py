# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 Domenico Altieri <softwaretamrsv@gmail.com>
# This file is part of Musubito — https://github.com/daltieri86/musubito
"""Public SDK decorators for Musubito execution steps."""

from __future__ import annotations

import inspect
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from typing import Any, Optional, ParamSpec, cast

from pydantic import BaseModel

from musubito.engine import MusubitoEngine
from musubito.models import MusubitoResult
from musubito.semantics import StepConfiguration, StepType
from musubito.storage import SQLiteStorage

P = ParamSpec("P")
_MUSUBITO_IDENTITY_METHOD = "__musubito_identity__"
_RECEIVER_PARAMETER_NAMES = frozenset({"self", "cls"})

_active_engine: ContextVar[MusubitoEngine | None] = ContextVar(
    "active_musubito_engine",
    default=None,
)
_default_engine_lock = threading.Lock()


@dataclass
class _DefaultEngineState:
    storage: SQLiteStorage | None = None
    engine: MusubitoEngine | None = None


_default_engine_state = _DefaultEngineState()


def musubito_step(
    semantics: Optional[StepConfiguration] = None,  # noqa: UP045
) -> Callable[[Callable[P, Any]], Callable[P, Any]]:
    """Decorate a function as a Musubito-tracked execution step."""

    step_semantics = semantics or StepConfiguration(step_type=StepType.DETERMINISTIC)

    def decorator(func: Callable[P, Any]) -> Callable[P, Any]:
        signature = inspect.signature(func)
        operation_name = f"{func.__module__}.{func.__qualname__}"

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> MusubitoResult[Any]:
                bound_arguments = signature.bind(*args, **kwargs)
                bound_arguments.apply_defaults()
                inputs = _canonical_inputs_from_bound_arguments(bound_arguments)

                async def invoke(_: Any) -> Any:
                    return await func(*bound_arguments.args, **bound_arguments.kwargs)

                return await _get_engine().execute_async(
                    operation_name,
                    inputs,
                    invoke,
                    step_semantics,
                )

            cast(Any, async_wrapper).__signature__ = signature
            return cast(Callable[P, Any], async_wrapper)

        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> MusubitoResult[Any]:
            bound_arguments = signature.bind(*args, **kwargs)
            bound_arguments.apply_defaults()
            inputs = _canonical_inputs_from_bound_arguments(bound_arguments)

            def invoke(_: Any) -> Any:
                return func(*bound_arguments.args, **bound_arguments.kwargs)

            return _get_engine().execute(
                operation_name,
                inputs,
                invoke,
                step_semantics,
            )

        cast(Any, sync_wrapper).__signature__ = signature
        return cast(Callable[P, Any], sync_wrapper)

    return decorator


@contextmanager
def use_musubito_engine(engine: MusubitoEngine) -> Iterator[None]:
    """Temporarily route decorated steps through an explicit engine instance."""

    token = _active_engine.set(engine)
    try:
        yield
    finally:
        _active_engine.reset(token)


def _get_engine() -> MusubitoEngine:
    active_engine = _active_engine.get()
    if active_engine is not None:
        return active_engine

    return _get_default_engine()


def _get_default_engine() -> MusubitoEngine:
    with _default_engine_lock:
        if _default_engine_state.engine is None:
            _default_engine_state.storage = SQLiteStorage()
            _default_engine_state.engine = MusubitoEngine(_default_engine_state.storage)

        return _default_engine_state.engine


def _canonical_inputs_from_bound_arguments(
    bound_arguments: inspect.BoundArguments,
) -> dict[str, Any]:
    normalized_inputs: dict[str, Any] = {}
    for parameter_name, value in bound_arguments.arguments.items():
        if parameter_name in _RECEIVER_PARAMETER_NAMES:
            normalized_inputs[parameter_name] = _receiver_identity_to_json_compatible(
                parameter_name,
                value,
            )
        else:
            normalized_inputs[parameter_name] = _to_json_compatible(value)

    return normalized_inputs


def _receiver_identity_to_json_compatible(parameter_name: str, value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _to_json_compatible(value)

    identity_provider = getattr(value, _MUSUBITO_IDENTITY_METHOD, None)
    if callable(identity_provider):
        return _to_json_compatible(identity_provider())

    msg = (
        f"Decorated Musubito method receiver '{parameter_name}' must provide "
        f"a deterministic {_MUSUBITO_IDENTITY_METHOD}() value or be a Pydantic model."
    )
    raise TypeError(msg)


def _to_json_compatible(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")

    if isinstance(value, Mapping):
        return _mapping_to_json_compatible(value)

    if isinstance(value, tuple):
        return [_to_json_compatible(item) for item in value]

    if isinstance(value, list):
        return [_to_json_compatible(item) for item in value]

    return value


def _mapping_to_json_compatible(value: Mapping[Any, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            msg = "Decorated Musubito step inputs require mapping keys to be strings."
            raise TypeError(msg)

        normalized[key] = _to_json_compatible(item)

    return normalized
