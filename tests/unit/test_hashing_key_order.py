from __future__ import annotations

from typing import Any

from musubito.hashing import generate_deterministic_hash


def test_flat_dictionary_with_different_key_order_produces_same_hash() -> None:
    first_payload = {"alpha": 1, "beta": 2, "gamma": 3}
    second_payload = {"gamma": 3, "alpha": 1, "beta": 2}

    assert generate_deterministic_hash(first_payload) == generate_deterministic_hash(second_payload)


def test_nested_dictionary_with_different_key_order_at_each_level_produces_same_hash() -> None:
    first_payload: dict[str, Any] = {
        "outer_b": {
            "inner_c": "stable",
            "inner_a": {
                "leaf_b": False,
                "leaf_a": True,
            },
        },
        "outer_a": {
            "inner_b": 20,
            "inner_a": 10,
        },
    }
    second_payload: dict[str, Any] = {
        "outer_a": {
            "inner_a": 10,
            "inner_b": 20,
        },
        "outer_b": {
            "inner_a": {
                "leaf_a": True,
                "leaf_b": False,
            },
            "inner_c": "stable",
        },
    }

    assert generate_deterministic_hash(first_payload) == generate_deterministic_hash(second_payload)


def test_list_of_dictionaries_with_different_key_order_produces_same_hash() -> None:
    first_payload: list[dict[str, Any]] = [
        {"name": "first", "score": 0.91, "enabled": True},
        {"name": "second", "score": 0.83, "enabled": False},
    ]
    second_payload: list[dict[str, Any]] = [
        {"enabled": True, "score": 0.91, "name": "first"},
        {"score": 0.83, "enabled": False, "name": "second"},
    ]

    assert generate_deterministic_hash(first_payload) == generate_deterministic_hash(second_payload)


def test_dictionary_with_scalar_values_in_different_key_order_produces_same_hash() -> None:
    first_payload: dict[str, object] = {
        "none_value": None,
        "bool_value": True,
        "int_value": 42,
        "float_value": 3.14,
        "str_value": "axiomix",
    }
    second_payload: dict[str, object] = {
        "str_value": "axiomix",
        "float_value": 3.14,
        "int_value": 42,
        "bool_value": True,
        "none_value": None,
    }

    assert generate_deterministic_hash(first_payload) == generate_deterministic_hash(second_payload)
