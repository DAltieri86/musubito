from __future__ import annotations

from musubito.hashing import compute_node_id, generate_deterministic_hash


def test_same_upstream_ids_constructed_in_different_orders_produce_same_node_id() -> None:
    input_hash = generate_deterministic_hash({"prompt": "summarize"})
    first_upstream_ids = frozenset(("node-c", "node-a", "node-b"))
    second_upstream_ids = frozenset(("node-b", "node-c", "node-a"))

    assert compute_node_id("summarize", input_hash, first_upstream_ids) == compute_node_id(
        "summarize",
        input_hash,
        second_upstream_ids,
    )


def test_empty_upstream_ids_produce_valid_stable_node_id_across_calls() -> None:
    input_hash = generate_deterministic_hash({"prompt": "standalone"})
    upstream_ids = frozenset[str]()
    first_node_id = compute_node_id("standalone", input_hash, upstream_ids)
    second_node_id = compute_node_id("standalone", input_hash, upstream_ids)

    assert first_node_id == second_node_id
    assert isinstance(first_node_id, str)
    assert first_node_id != ""


def test_distinct_upstream_ids_produce_different_node_ids() -> None:
    input_hash = generate_deterministic_hash({"prompt": "shared"})
    first_upstream_ids = frozenset(("node-a", "node-b"))
    second_upstream_ids = frozenset(("node-a", "node-c"))

    assert compute_node_id("shared-operation", input_hash, first_upstream_ids) != compute_node_id(
        "shared-operation",
        input_hash,
        second_upstream_ids,
    )


def test_different_operation_names_with_same_upstream_ids_produce_different_node_ids() -> None:
    input_hash = generate_deterministic_hash({"prompt": "shared"})
    upstream_ids = frozenset(("node-a", "node-b"))

    assert compute_node_id("classify", input_hash, upstream_ids) != compute_node_id(
        "summarize",
        input_hash,
        upstream_ids,
    )


def test_fifty_upstream_ids_produce_stable_node_id_independent_of_insertion_order() -> None:
    input_hash = generate_deterministic_hash({"prompt": "large-merge"})
    upstream_ids_in_order = [f"node-{index:02d}" for index in range(50)]
    upstream_ids_in_mixed_order = (
        upstream_ids_in_order[1::2]
        + upstream_ids_in_order[::2]
        + list(reversed(upstream_ids_in_order))
    )

    assert compute_node_id(
        "large-merge",
        input_hash,
        frozenset(upstream_ids_in_order),
    ) == compute_node_id(
        "large-merge",
        input_hash,
        frozenset(upstream_ids_in_mixed_order),
    )
