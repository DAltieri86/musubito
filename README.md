# Musubito

[![CI](https://github.com/daltieri86/musubito/actions/workflows/ci.yml/badge.svg)](https://github.com/daltieri86/musubito/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/musubito)](https://pypi.org/project/musubito/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

![Musubito - AI Workflow Lineage](https://user-gen-media-assets.s3.amazonaws.com/gpt4o_images/dba2a29d-53f5-4833-8205-f5244e183df7.png)

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

## Real-World Examples

The examples below use real LLM SDK calls. Install the provider SDK you need
(`pip install openai` or `pip install anthropic`) and set the matching API key in
your environment before running them.

### Single LLM call with permanent cache

```python
import time

from openai import OpenAI

from musubito import StepConfiguration, StepType, musubito_step

client = OpenAI()


@musubito_step(
    semantics=StepConfiguration(step_type=StepType.DETERMINISTIC),
)
def explain_runtime(prompt: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content or ""


prompt = "Explain deterministic replay for LLM research agents in five bullets."

start = time.perf_counter()
first = explain_runtime(prompt)
first_ms = (time.perf_counter() - start) * 1000

start = time.perf_counter()
second = explain_runtime(prompt)
second_ms = (time.perf_counter() - start) * 1000

# The second call saves one OpenAI API request for the same stable prompt.
print(first.value[:200])
print(second.value[:200])
print(f"first={first_ms:.1f} ms replay={second_ms:.1f} ms")
```

### Expiring cache for fresh answers

```python
from openai import OpenAI

from musubito import StepConfiguration, StepType, musubito_step

client = OpenAI()

fresh_hourly = StepConfiguration(
    step_type=StepType.STOCHASTIC,
    ttl_seconds=3600,
)


@musubito_step(semantics=fresh_hourly)
def market_brief(topic: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.4,
        messages=[
            {
                "role": "user",
                "content": f"Write a concise market-watch brief about {topic}.",
            }
        ],
    )
    return response.choices[0].message.content or ""


result = market_brief("AI infrastructure startups")

# STOCHASTIC + TTL saves repeat API calls for one hour, then refreshes naturally.
print(result.value)
```

### Two-step pipeline with lineage

```python
from anthropic import Anthropic

from musubito import MusubitoResult, StepConfiguration, StepType
from musubito import musubito_merge, musubito_step

client = Anthropic()


@musubito_step()
def extract_key_facts(text: str) -> str:
    message = client.messages.create(
        model="claude-3-5-haiku-latest",
        max_tokens=300,
        messages=[{"role": "user", "content": f"Extract key facts:\n{text}"}],
    )
    return message.content[0].text


@musubito_step(
    semantics=StepConfiguration(step_type=StepType.STOCHASTIC, ttl_seconds=86400),
)
def write_social_summary(facts: MusubitoResult[str]) -> str:
    message = client.messages.create(
        model="claude-3-5-haiku-latest",
        max_tokens=120,
        messages=[{"role": "user", "content": f"Write one tweet:\n{facts.value}"}],
    )
    return message.content[0].text


source_text = "Musubito records execution lineage for replayable agent steps."
facts = extract_key_facts(source_text)

with musubito_merge(facts):
    summary = write_social_summary(facts)

# Re-running saves the extraction call immediately; the summary refreshes after TTL.
print(summary.value)
```

### Custom storage path for a project

```python
from openai import OpenAI

from musubito import MusubitoEngine, SQLiteStorage
from musubito import musubito_step, use_musubito_engine

client = OpenAI()


@musubito_step()
def classify_note(note: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {
                "role": "user",
                "content": f"Classify this research note in one label:\n{note}",
            }
        ],
    )
    return response.choices[0].message.content or ""


storage = SQLiteStorage(db_path=".musubito/project-alpha.db")
engine = MusubitoEngine(storage)

with storage, use_musubito_engine(engine):
    result = classify_note("GPU scheduling dominates the serving bottleneck.")

# A project-specific DB keeps replay separate across teams or experiments.
print(result.value)
```

## When to Use Which StepType

| StepType | When to use it | LLM example |
|---|---|---|
| `DETERMINISTIC` | Pure or stable outputs | Text normalization, embeddings, structured extraction |
| `STOCHASTIC` | Outputs may vary or go stale | Chat completions, generative summaries |
| `EXTERNAL_EFFECT` | Side effects beyond the return value | Sending email, writing to a DB, calling a webhook |

## Core Concepts

A **node** is one recorded execution of a decorated function. Its identity is computed from three things: the operation name, the canonical hash of the function inputs, and the sorted set of upstream node IDs. This makes node identity stable across runs when the logical computation is the same.

**Replay** means Musubito returns a previously stored artifact instead of calling the decorated function again. Replay is allowed when the stored node is successful, not stale, not forced to re-execute, and any configured TTL has not expired.

`StepType.DETERMINISTIC` marks work that is safe to replay freely, such as pure transformations or deterministic parsers.

`StepType.STOCHASTIC` marks work that may produce different outputs, such as LLM calls. It can still be replayed intentionally, often with a TTL to bound how long the stored result remains valid.

`StepType.EXTERNAL_EFFECT` marks work with side effects, such as network calls or tool invocations. When replay is allowed, Musubito returns the stored artifact; it does not re-run the external effect. Use `force_reexecution=True` when the effect must be issued again.

An **Artifact** is the persisted output of a node. `MusubitoResult[T]` points to that artifact and carries both the current DAG node ID and the historical producer node ID.

`musubito_merge()` declares an explicit multi-parent context. It is used when one step depends on multiple previous `MusubitoResult[T]` values, making fan-in DAG edges visible to the lineage engine.

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

## License

Musubito is dual-licensed:

- **Open Source**: AGPL-3.0-or-later — free for open-source projects.
  See [LICENSE](LICENSE).
- **Commercial**: closed-source or proprietary use requires a separate license.
  See [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md) or contact
  softwaretamrsv@gmail.com.

Open-source projects and personal use: AGPL-3.0. Closed-source or commercial products: commercial license required.
