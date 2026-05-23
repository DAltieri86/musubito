from datetime import datetime, timezone
from typing import Any, TypeAlias

import pytest
from pydantic import ValidationError

from musubito.hashing import compute_node_id, generate_deterministic_hash
from musubito.models import (
    Artifact,
    ArtifactType,
    ExecutionNode,
    MusubitoJsonObject,
    MusubitoJsonObjectResult,
    MusubitoResult,
    NodeStatus,
)
from musubito.semantics import StepConfiguration, StepType

EXPECTED_INTEGER_VALUE = 42

ConcreteStringResult: TypeAlias = MusubitoResult[str]


def test_generate_deterministic_hash_orders_nested_dictionary_keys() -> None:
    left_payload: dict[str, Any] = {
        "b": [{"z": 3, "a": 1}],
        "a": {"y": True, "x": None},
    }
    right_payload: dict[str, Any] = {
        "a": {"x": None, "y": True},
        "b": [{"a": 1, "z": 3}],
    }

    assert generate_deterministic_hash(left_payload) == generate_deterministic_hash(right_payload)


def test_generate_deterministic_hash_preserves_list_order() -> None:
    first_hash = generate_deterministic_hash({"items": ["alpha", "beta"]})
    second_hash = generate_deterministic_hash({"items": ["beta", "alpha"]})

    assert first_hash != second_hash


def test_generate_deterministic_hash_rejects_non_string_dictionary_keys() -> None:
    payload: dict[Any, str] = {1: "integer-key"}

    with pytest.raises(TypeError):
        generate_deterministic_hash(payload)


def test_generate_deterministic_hash_rejects_mixed_dictionary_key_types() -> None:
    payload: dict[Any, str] = {1: "integer-key", "1": "string-key"}

    with pytest.raises(TypeError):
        generate_deterministic_hash(payload)


def test_compute_node_id_orders_upstream_ids_alphabetically() -> None:
    input_hash = generate_deterministic_hash({"prompt": "hello"})
    left_upstream_ids = frozenset({"node-c", "node-a", "node-b"})
    right_upstream_ids = frozenset({"node-b", "node-c", "node-a"})

    assert compute_node_id("summarize", input_hash, left_upstream_ids) == compute_node_id(
        "summarize",
        input_hash,
        right_upstream_ids,
    )


def test_compute_node_id_changes_when_operation_changes() -> None:
    input_hash = generate_deterministic_hash({"prompt": "hello"})
    upstream_ids = frozenset({"node-a"})

    first_node_id = compute_node_id("summarize", input_hash, upstream_ids)
    second_node_id = compute_node_id("classify", input_hash, upstream_ids)

    assert first_node_id != second_node_id


def test_artifact_model_validates_enum_values() -> None:
    artifact = Artifact(
        artifact_id="artifact-1",
        type=ArtifactType.PROMPT,
        payload={"text": "hello"},
    )

    assert artifact.type is ArtifactType.PROMPT


def test_execution_node_model_keeps_current_and_historical_node_identity_separate() -> None:
    input_hash = generate_deterministic_hash({"value": 1})
    node_id = compute_node_id("increment", input_hash, frozenset())
    semantics = StepConfiguration(step_type=StepType.DETERMINISTIC)
    executed_at = datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc)

    node = ExecutionNode(
        node_id=node_id,
        input_hash=input_hash,
        operation_name="increment",
        status=NodeStatus.SUCCESS,
        semantics=semantics,
        executed_at=executed_at,
        output_artifact_id="artifact-1",
    )
    result = MusubitoResult[int](
        value=2,
        artifact_id="artifact-1",
        producer_node_id=node.node_id,
        node_id=node.node_id,
    )

    assert result.producer_node_id == result.node_id


def test_musubito_result_str_parameterization_validates_value_type() -> None:
    result = MusubitoResult[str](
        value="ok",
        artifact_id="artifact-1",
        producer_node_id="node-origin",
        node_id="node-current",
    )

    assert result.value == "ok"


def test_musubito_result_int_parameterization_validates_value_type() -> None:
    result = MusubitoResult[int](
        value=EXPECTED_INTEGER_VALUE,
        artifact_id="artifact-1",
        producer_node_id="node-origin",
        node_id="node-current",
    )

    assert result.value == EXPECTED_INTEGER_VALUE


def test_musubito_result_dict_parameterization_validates_json_object_shape() -> None:
    payload: dict[str, Any] = {"answer": "ok", "usage": {"tokens": 12}}
    result = MusubitoResult[dict[str, Any]](
        value=payload,
        artifact_id="artifact-1",
        producer_node_id="node-origin",
        node_id="node-current",
    )

    assert result.value == payload


def test_public_json_object_result_alias_is_reusable() -> None:
    value: MusubitoJsonObject = {"score": 0.98, "label": "stable"}
    result = MusubitoJsonObjectResult(
        value=value,
        artifact_id="artifact-1",
        producer_node_id="node-origin",
        node_id="node-current",
    )

    assert result.value["label"] == "stable"


def test_concrete_string_result_alias_is_reusable() -> None:
    result = ConcreteStringResult(
        value="stable",
        artifact_id="artifact-1",
        producer_node_id="node-origin",
        node_id="node-current",
    )

    assert result.value == "stable"


def test_musubito_result_str_rejects_integer_value() -> None:
    invalid_value: Any = 1

    with pytest.raises(ValidationError):
        MusubitoResult[str](
            value=invalid_value,
            artifact_id="artifact-1",
            producer_node_id="node-origin",
            node_id="node-current",
        )


def test_artifact_rejects_unknown_type() -> None:
    invalid_type: Any = "UNKNOWN"

    with pytest.raises(ValidationError):
        Artifact(
            artifact_id="artifact-1",
            type=invalid_type,
            payload={"text": "hello"},
        )
