from __future__ import annotations

from typing import Any

import pytest

from musubito.hashing import generate_deterministic_hash


def test_list_and_tuple_with_same_items_produce_different_hashes() -> None:
    list_hash = generate_deterministic_hash([1, 2, 3])
    tuple_hash = generate_deterministic_hash((1, 2, 3))

    assert list_hash != tuple_hash


def test_set_input_raises_type_error_with_unsupported_set_message() -> None:
    with pytest.raises(TypeError, match="set"):
        generate_deterministic_hash({1, 2, 3})


def test_frozenset_input_raises_type_error_with_unsupported_frozenset_message() -> None:
    with pytest.raises(TypeError, match="frozenset"):
        generate_deterministic_hash(frozenset({1, 2, 3}))


def test_nested_set_input_raises_type_error_with_unsupported_set_message() -> None:
    payload: dict[str, Any] = {
        "operation": "collect",
        "values": [1, {"unsupported": {"a", "b"}}],
    }

    with pytest.raises(TypeError, match="set"):
        generate_deterministic_hash(payload)
