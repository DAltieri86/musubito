# Musubito

[![CI](https://github.com/daltieri86/musubito/actions/workflows/ci.yml/badge.svg)](https://github.com/daltieri86/musubito/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/musubito)](https://pypi.org/project/musubito/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Musubito records execution lineage so agentic LLM workflows can safely skip redundant expensive calls instead of recomputing the same DAG steps.

## Why Musubito?

LLM-heavy pipelines often re-run the same expensive steps because the runtime has no durable memory of what was executed, which inputs were used, and which upstream results contributed to the output.

Musubito gives each execution node a deterministic identity derived from the operation name, canonical input hash, and sorted upstream node IDs. If the same logical node is reached again and its replay policy allows reuse, Musubito returns the stored artifact instead of executing the function again.

Lineage is stored locally in SQLite, so replay decisions are fast, deterministic, and inspectable without requiring a remote service.

Fan-in DAG patterns are first-class: `musubito_merge()` lets an aggregate step explicitly depend on multiple upstream `MusubitoResult[T]` values.

## Install

```bash
pip install musubito
```

## Core Concepts

A **node** is one recorded execution of a decorated function. Its identity is computed from three things: the operation name, the canonical hash of the function inputs, and the sorted set of upstream node IDs. This makes node identity stable across runs when the logical computation is the same.

**Replay** means Musubito returns a previously stored artifact instead of calling the decorated function again. Replay is allowed when the stored node is successful, not stale, not forced to re-execute, and any configured TTL has not expired.

`StepType.DETERMINISTIC` marks work that is safe to replay freely, such as pure transformations or deterministic parsers.

`StepType.STOCHASTIC` marks work that may produce different outputs, such as LLM calls. It can still be replayed intentionally, often with a TTL to bound how long the stored result remains valid.

`StepType.EXTERNAL_EFFECT` marks work with side effects, such as network calls or tool invocations. When replay is allowed, Musubito returns the stored artifact; it does not re-run the external effect. Use `force_reexecution=True` when the effect must be issued again.

An **Artifact** is the persisted output of a node. `MusubitoResult[T]` points to that artifact and carries both the current DAG node ID and the historical producer node ID.

`musubito_merge()` declares an explicit multi-parent context. It is used when one step depends on multiple previous `MusubitoResult[T]` values, making fan-in DAG edges visible to the lineage engine.

## Quickstart

```python
import asyncio

from musubito import MusubitoResult, musubito_merge, musubito_step

calls = {
    "fetch_context": 0,
    "fetch_policy": 0,
    "summarize": 0,
}


@musubito_step()
async def fetch_context(topic: str) -> dict[str, str]:
    calls["fetch_context"] += 1
    return {"topic": topic, "source": "local"}


@musubito_step()
async def fetch_policy(topic: str) -> dict[str, str]:
    calls["fetch_policy"] += 1
    return {"topic": topic, "policy": "strict-replay"}


@musubito_step()
def summarize(
    context: MusubitoResult[dict[str, str]],
    policy: MusubitoResult[dict[str, str]],
) -> dict[str, str]:
    calls["summarize"] += 1
    return {
        "topic": context.value["topic"],
        "source": context.value["source"],
        "policy": policy.value["policy"],
    }


async def main(label: str) -> None:
    context, policy = await asyncio.gather(
        fetch_context("lineage"),
        fetch_policy("lineage"),
    )

    with musubito_merge(context, policy):
        result = summarize(context, policy)

    print(label, result.value)
    print(label, calls)


asyncio.run(main("first run"))
asyncio.run(main("second run"))

# Assumes a fresh .musubito/musubito.db in the current working directory.
# The second asyncio.run(...) replays from SQLite in the same Python process
# and does not re-execute the decorated functions, so each counter remains at 1.
```

Decorated functions return `MusubitoResult[T]`. The raw user value is available through `.value`; the wrapper is not unwrapped automatically.

## Step Configuration

The default configuration is deterministic and requires no extra setup:

```python
from musubito import musubito_step


@musubito_step()
def normalize(text: str) -> str:
    return text.strip().lower()
```

For stochastic work, such as an LLM call, use a TTL when the cached answer should only be reused for a bounded time:

```python
from musubito import StepConfiguration, StepType, musubito_step

llm_semantics = StepConfiguration(
    step_type=StepType.STOCHASTIC,
    ttl_seconds=3600,
)


@musubito_step(semantics=llm_semantics)
def draft_answer(prompt: str) -> str:
    return prompt.upper()
```

To force a stochastic step to run again, set `force_reexecution=True`:

```python
from musubito import StepConfiguration, StepType, musubito_step

fresh_semantics = StepConfiguration(
    step_type=StepType.STOCHASTIC,
    force_reexecution=True,
)


@musubito_step(semantics=fresh_semantics)
def generate_fresh_answer(prompt: str) -> str:
    return prompt.upper()
```

For external effects, Musubito stores and returns the artifact when replay is allowed. The side effect itself is not repeated unless `force_reexecution=True` is used:

```python
from musubito import StepConfiguration, StepType, musubito_step

external_semantics = StepConfiguration(
    step_type=StepType.EXTERNAL_EFFECT,
)


@musubito_step(semantics=external_semantics)
def call_external_tool(payload: dict[str, str]) -> dict[str, str]:
    return {"status": "recorded", "id": payload["id"]}
```

## Using a Custom Engine

Use `use_musubito_engine()` when you want explicit control over the storage path or engine instance:

```python
from musubito import (
    MusubitoEngine,
    SQLiteStorage,
    musubito_step,
    use_musubito_engine,
)


@musubito_step()
def summarize_text(text: str) -> str:
    return text.upper()


with SQLiteStorage(db_path=".my_run/run.db") as storage:
    engine = MusubitoEngine(storage)

    with use_musubito_engine(engine):
        result = summarize_text("Musubito records deterministic lineage.")

print(result.value)
```

## Storage

Musubito uses SQLite as its local relational storage layer. By default, it stores runtime data under:

```text
.musubito/musubito.db
```

The path can be customized with `SQLiteStorage(db_path=...)`.

SQLite is opened in WAL mode and uses short `BEGIN IMMEDIATE` write transactions for node, artifact, edge, and invalidation updates. This keeps local concurrent writes predictable while still allowing normal reads.

Downstream invalidation is performed in place with a recursive CTE. When a node output changes, dependent downstream nodes can be marked stale so future runs recompute only the affected part of the DAG.

## Use Cases

- Agentic LLM pipelines with repeated planning, tool, or synthesis steps.
- Multi-step RAG workflows where retrieval, filtering, and summarization can be replayed.
- CI regression workflows that call expensive tools or model-based checks.
- Long-running research agents that need durable local execution memory.
- Reproducible evaluation pipelines with explicit lineage and cache boundaries.

## License

Musubito is dual-licensed:

- **Open Source**: AGPL-3.0-or-later — free for open-source projects.
  See [LICENSE](LICENSE).
- **Commercial**: closed-source or proprietary use requires a separate license.
  See [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md) or contact
  softwaretamrsv@gmail.com.

Open-source projects and personal use: AGPL-3.0. Closed-source or commercial products: commercial license required.
