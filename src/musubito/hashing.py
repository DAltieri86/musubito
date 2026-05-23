# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 Domenico Altieri <softwaretamrsv@gmail.com>
# This file is part of Musubito — https://github.com/daltieri86/musubito
"""Deterministic hashing utilities for DAG identity."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

_UNSUPPORTED_SCALAR = object()


def _canonicalize_json_value(data: Any) -> Any:
    scalar_value = _canonicalize_scalar_value(data)
    if scalar_value is not _UNSUPPORTED_SCALAR:
        return scalar_value

    if isinstance(data, Mapping):
        for key in data:
            if not isinstance(key, str):
                msg = "Deterministic hashing requires JSON object keys to be strings."
                raise TypeError(msg)

        return [
            "dict",
            [
                [key, _canonicalize_json_value(value)]
                for key, value in sorted(data.items(), key=lambda item: item[0])
            ],
        ]

    if isinstance(data, list):
        return [
            "list",
            [_canonicalize_json_value(item) for item in data],
        ]

    if isinstance(data, tuple):
        return [
            "tuple",
            [_canonicalize_json_value(item) for item in data],
        ]

    msg = f"Deterministic hashing does not support values of type {type(data).__name__}."
    raise TypeError(msg)


def _canonicalize_scalar_value(data: Any) -> Any:
    if data is None:
        return ["none", None]

    if isinstance(data, bool):
        return ["bool", data]

    if isinstance(data, int):
        return ["int", data]

    if isinstance(data, float):
        return ["float", data]

    if isinstance(data, str):
        return ["str", data]

    return _UNSUPPORTED_SCALAR


def generate_deterministic_hash(data: Any) -> str:
    """Return a stable SHA-256 hash for JSON-compatible data."""

    canonical_data = _canonicalize_json_value(data)
    serialized_data = json.dumps(
        canonical_data,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized_data.encode("utf-8")).hexdigest()


def compute_node_id(
    operation_name: str,
    input_hash: str,
    upstream_ids: frozenset[str],
) -> str:
    """Return the deterministic identity of a DAG execution node."""

    node_identity_payload = {
        "input_hash": input_hash,
        "operation_name": operation_name,
        "upstream_ids": sorted(upstream_ids),
    }
    return generate_deterministic_hash(node_identity_payload)
