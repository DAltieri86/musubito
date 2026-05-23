# Musubito

[![CI](https://github.com/daltieri86/musubito/actions/workflows/ci.yml/badge.svg)](https://github.com/daltieri86/musubito/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/musubito)](https://pypi.org/project/musubito/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Musubito is a Python runtime for recording execution lineage in a relational DAG.
It is designed for agentic and LLM-heavy workflows where repeated non-deterministic
loops create avoidable token cost and latency.

The runtime records execution nodes, artifacts, and dependencies in SQLite. It uses
deterministic node identity, replay semantics, downstream invalidation, and explicit
multi-parent merge contexts to decide when a previous result can be reused safely.

## Install

```bash
pip install musubito
```

## Quickstart

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

Musubito computes deterministic node identity from the operation name, canonical input
hash, and sorted upstream node identifiers. Replay is allowed only when the stored node
is successful, has an available output artifact, is not stale, does not force
re-execution, and any configured TTL has not expired.

Time-dependent replay checks use an explicit `now` value inside the semantics layer;
the replay decision itself does not read the system clock.

## Local Storage

SQLite is used as local relational storage. The storage layer enables WAL mode and uses
short `BEGIN IMMEDIATE` write transactions. Downstream invalidation is performed with a
recursive CTE. By default, local state is stored under:

```
.musubito/musubito.db
```

## License

Musubito is dual-licensed:

- **Open Source**: AGPL-3.0-or-later — free for open-source projects.
  See [LICENSE](LICENSE).
- **Commercial**: closed-source or proprietary use requires a separate license.
  See [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md) or contact
  softwaretamrsv@gmail.com.
