# Musubito

Musubito is a Python runtime for recording execution lineage in a relational DAG.
It is designed for agentic and LLM-heavy workflows where repeated non-deterministic
loops create avoidable token cost and latency.

## Install

```bash
pip install musubito
```

## Quickstart

Define tracked steps with `@musubito_step`:

```python
import asyncio

from musubito import MusubitoResult, musubito_merge, musubito_step


@musubito_step()
async def fetch_context(topic: str) -> dict[str, str]:
    return {"topic": topic, "source": "local"}


@musubito_step()
async def fetch_policy(topic: str) -> dict[str, str]:
    return {"topic": topic, "policy": "strict-replay"}


@musubito_step()
def summarize(
    context: MusubitoResult[dict[str, str]],
    policy: MusubitoResult[dict[str, str]],
) -> dict[str, str]:
    return {
        "topic": context.value["topic"],
        "policy": policy.value["policy"],
    }


async def main() -> None:
    context, policy = await asyncio.gather(
        fetch_context("lineage"),
        fetch_policy("lineage"),
    )

    with musubito_merge(context, policy):
        result = summarize(context, policy)

    print(result.value)


asyncio.run(main())
```

Decorated functions return `MusubitoResult[T]`. The wrapper does not unwrap `.value`;
the result carries lineage meta

- `value`: the typed user value.
- `artifact_id`: the persisted output artifact identifier.
- `producer_node_id`: the historical node that originally produced the value.
- `node_id`: the node identity in the current DAG run.

## Replay Model

Musubito computes deterministic node identity from:

- The operation name (module + qualified name of the decorated function)
- The canonical input hash (recursive key-sorted JSON serialization)
- The sorted set of upstream producer node identifiers

| Scenario | Tokens | Latency |
|---|---|---|
| Traditional loop (no lineage) | 11,655 | 45s |
| Musubito DAG replay | 382 | 7.9s |

Replay is allowed only when the stored node is successful, has an available output
artifact, is not stale, does not force re-execution, and any configured TTL has not
expired. Time-dependent checks use an explicit `now` parameter — the replay decision
does not read the system clock.

## Local Storage

SQLite is used as local relational storage with WAL mode and short
`BEGIN IMMEDIATE` write transactions. Downstream invalidation uses a recursive CTE.

By default, local state is stored under:

```
.musubito/musubito.db
```

## Advanced — Custom Engine

```python
from musubito import MusubitoEngine, SQLiteStorage, musubito_step, use_musubito_engine


@musubito_step()
def summarize_text(text: str) -> str:
    return text.upper()


storage = SQLiteStorage(".musubito/custom.db")
engine = MusubitoEngine(storage)
with use_musubito_engine(engine):
    result = summarize_text("Musubito records deterministic execution lineage.")
```

## Instance Methods

Decorated instance or class methods must expose a deterministic receiver identity
through `__musubito_identity__()` or use a Pydantic model as the receiver. This
prevents replay from merging calls made on distinct stateful objects.

## License

Musubito is dual-licensed:

- **Open Source**: AGPL-3.0-or-later — free for open-source projects.
  See [LICENSE](https://github.com/daltieri86/musubito/blob/main/LICENSE).
- **Commercial**: closed-source or proprietary use requires a separate license.
  Contact softwaretamrsv@gmail.com.
