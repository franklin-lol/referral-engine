# referral-engine

[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen)](#running-tests)
[![CI](https://github.com/franklin-lol/referral-engine/actions/workflows/python-tests.yml/badge.svg)](https://github.com/franklin-lol/referral-engine/actions/workflows/python-tests.yml)

**Language / Язык:** [English](#) · [Русский](README.ru.md)

---

Pluggable, async-native multi-level referral distribution engine for Python services.

Drop it into any backend — Telegram bot, REST API, gRPC service — without touching your existing schema. Swap the storage adapter to change the database. Tune every distribution parameter via a single YAML file.

---

## What it solves

Most referral implementations are hard-coded into the business layer: fixed depth, no idempotency, no cycle protection, impossible to test in isolation. This library separates concerns cleanly:

| Concern | Owner |
|---|---|
| Distribution algorithm | `ReferralEngine` |
| Storage & queries | `BaseAdapter` (you implement or use built-in) |
| Configuration | `config.yaml` (hot-swappable) |
| Your application | Calls the engine, owns the transaction |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Your Application                       │
│  (Telegram bot / FastAPI / gRPC / background worker)    │
└────────────────────┬────────────────────────────────────┘
                     │  async with adapter:
                     │      await engine.distribute(...)
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  ReferralEngine                          │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────┐  │
│  │  Distributor │  │  TreeTraversal│  │ CycleGuard  │  │
│  │  (algorithm) │  │  (up / down)  │  │ (DFS check) │  │
│  └──────────────┘  └───────────────┘  └─────────────┘  │
└────────────────────┬────────────────────────────────────┘
                     │  implements BaseAdapter
          ┌──────────┴──────────┐
          ▼                     ▼
┌─────────────────┐   ┌──────────────────┐
│ PostgresAdapter │   │  MemoryAdapter   │
│  (asyncpg)      │   │  (tests / local) │
└─────────────────┘   └──────────────────┘
          │
          ▼
┌─────────────────┐
│   PostgreSQL    │
│  re_users       │
│  re_referrals   │  ← closure table (levels 1..N)
│  re_accruals    │  ← immutable audit log
└─────────────────┘
```

---

## Core features

**Configurable depth and rates**  
Define up to 50 levels. Each level has its own accrual rate. Change rates in YAML — no code change, no redeployment.

**Idempotent distribution**  
Every `distribute()` call builds a compound key from `(source_user_id, source_tag, date)`. Retrying the same event produces zero duplicate accruals.

**Cycle detection**  
Upward DFS on every referrer assignment. O(depth), not O(tree size). Raises `CycleDetectedError` before any write.

**Active-deposit gate**  
When `require_active_deposit: true`, ancestors without an active deposit are skipped — the walk continues upward. Toggle per-user via `update_deposit_status()`.

**Income cap**  
Optional per-event maximum: no single user receives more than `income_cap_per_event` from one distribution event regardless of rate.

**Pluggable storage**  
Implement the 8-method `BaseAdapter` interface to connect any database. Built-in adapters: PostgreSQL (`asyncpg`) and in-memory (for tests).

**Zero ORM coupling**  
Domain models are plain Python dataclasses. No SQLAlchemy, no Django ORM, no framework dependencies in the core library.

---

## Quick start

```python
import asyncio
from decimal import Decimal
import asyncpg

from referral_engine import ReferralEngine, EngineConfig, PostgresAdapter

async def main():
    pool   = await asyncpg.create_pool("postgresql://user:pass@localhost/db")
    config = EngineConfig.from_yaml("config.yaml")

    adapter = PostgresAdapter(pool)
    engine  = ReferralEngine(config, adapter)

    # Register users
    async with adapter:
        alice = await engine.register_user("alice")
        bob   = await engine.register_user("bob",   referrer_id=alice.id)
        carol = await engine.register_user("carol", referrer_id=bob.id)

    # Mark alice and bob as having active deposits
    async with adapter:
        await engine.update_deposit_status(alice.id, True)
        await engine.update_deposit_status(bob.id,   True)

    # Carol earns 150 USDT profit → distribute bonuses upward
    async with adapter:
        result = await engine.distribute(
            source_user_id=carol.id,
            base_amount=Decimal("150.00"),
            source_tag="deposit_7_profit_20250624",
        )

    print(result.summary())
    # bob   → L1 → 12% → 18.00 USDT
    # alice → L2 → 10% → 15.00 USDT

asyncio.run(main())
```

No database? Use the in-memory adapter:

```python
from referral_engine.adapters.memory import MemoryAdapter

adapter = MemoryAdapter()   # drop-in replacement, same API
engine  = ReferralEngine(config, adapter)
```

---

## Configuration

Copy `config.example.yaml` and adjust:

```yaml
# Per-level accrual rates (index 0 = level 1)
rates:
  - 12.0   # Level 1 — direct referrer
  - 10.0   # Level 2
  -  8.0   # Level 3
  -  6.0   # Level 4
  -  5.0   # Level 5
  -  4.0   # Level 6
  -  3.0   # Level 7
  -  3.0   # Level 8
  -  2.0   # Level 9
  -  2.0   # Level 10
  -  1.0   # Levels 11–15
  -  1.0
  -  1.0
  -  1.0
  -  1.0

max_depth: 50
require_active_deposit: true

# income_cap_per_event: 500.0   # optional — uncomment to enable

min_accrual_amount: 0.000001
idempotency_window_seconds: 86400

leader_thresholds:
  - { level: 1, volume:   10000, bonus:   100 }
  - { level: 2, volume:   30000, bonus:   300 }
  - { level: 3, volume:   50000, bonus:   500 }
  - { level: 4, volume:  100000, bonus:  1000 }
```

Load at startup — no redeployment on rate changes:

```python
config = EngineConfig.from_yaml("config.yaml")  # YAML
config = EngineConfig.from_json("config.json")  # JSON
config = EngineConfig.from_dict(my_dict)         # plain dict
```

---

## API reference

### `ReferralEngine`

```python
engine = ReferralEngine(config: EngineConfig, adapter: BaseAdapter)
```

| Method | Description |
|---|---|
| `register_user(external_id, referrer_id?)` | Create user, optionally link to referrer |
| `set_referrer(user_id, referrer_id)` | Change referrer (with cycle check) |
| `update_deposit_status(user_id, bool)` | Toggle active-deposit gate |
| `distribute(source_user_id, base_amount, source_tag)` | Run distribution upward |
| `get_tree_up(user_id, max_levels?)` | Flat ancestor list |
| `get_tree_down(user_id, max_levels?)` | Nested descendant tree |
| `rebuild_tree(scope_user_id?)` | Rebuild multi-level closure table |
| `get_user_accruals(user_id, since?, limit?)` | Accrual history |
| `get_user_total_accrued(user_id)` | Lifetime sum |

### `BaseAdapter` — implement to plug in any database

```python
class MyAdapter(BaseAdapter):
    async def get_user(self, user_id: int) -> Optional[User]: ...
    async def create_user(self, external_id: str) -> User: ...
    async def get_parent(self, user_id: int) -> Optional[ReferralLink]: ...
    async def get_chain_up(self, user_id, max_depth) -> List[ReferralLink]: ...
    async def create_referral_link(self, user_id, referrer_id) -> ReferralLink: ...
    async def rebuild_tree(self, scope_user_id=None) -> int: ...
    async def accrual_exists(self, source_key, recipient_user_id, level) -> bool: ...
    async def save_accrual(self, accrual: AccrualRecord) -> AccrualRecord: ...
    # + transaction control: begin / commit / rollback
```

---

## Distribution algorithm

```
distribute(source_user_id=4, base=200 USDT, tag="profit_day1")

Tree: root(1) ─► A(2) ─► B(3) ─► source(4)
Deposits: root=✓  A=✗  B=✓

Walk upward from source(4):
  hop 1 → B(3)   rate=12%  deposit=✓  amount=24.00  ✓ accrued
  hop 2 → A(2)   rate=10%  deposit=✗  SKIP (walk continues)
  hop 3 → root(1) rate=8%  deposit=✓  amount=16.00  ✓ accrued (now at level 3)

Result: total=40.00 USDT, levels_reached=2, skipped=1
```

Key properties:
- **Skip ≠ stop.** An ancestor without a deposit is bypassed; their ancestors are still evaluated.
- **Rate is level-positional.** The rate applied to root is `rates[2]` (8%) because it was reached at the 3rd hop — not `rates[0]`.
- **Idempotent.** Calling with the same `source_tag` on the same calendar day is a no-op.

---

## Database schema

```sql
-- Users
re_users (id, external_id, has_active_deposit, is_active, created_at)

-- Closure table — direct links (level=1) + derived multi-level links
re_referrals (user_id, referrer_id, level, created_at)
  UNIQUE (user_id, level)

-- Immutable accrual log
re_accruals (source_user_id, recipient_user_id, level,
             base_amount, accrual_rate, accrual_amount,
             source_key, source_tag, accrued_at)
  UNIQUE (source_key, recipient_user_id, level)  -- idempotency

-- Leader awards (one-time bonuses)
re_leader_awards (user_id, level, bonus, awarded_at)
  UNIQUE (user_id, level)
```

All tables are prefixed `re_` to avoid collisions with your existing schema. Apply the migration once:

```bash
psql $DATABASE_URL -f migrations/001_schema.sql
```

---

## Running with Docker

```bash
docker-compose up
```

This starts PostgreSQL with the schema applied automatically and the FastAPI demo on port 8000.

**API endpoints:**

```
POST   /api/v1/users                         Register user
PATCH  /api/v1/users/{id}/deposit            Toggle deposit status
GET    /api/v1/users/{id}/accruals           Accrual history

POST   /api/v1/referrals                     Set referrer
GET    /api/v1/referrals/{id}/tree/up        Ancestor chain
GET    /api/v1/referrals/{id}/tree/down      Descendant tree
POST   /api/v1/referrals/rebuild             Rebuild closure table

POST   /api/v1/distributions                 Distribute profit
GET    /config                               Current engine config
```

Interactive docs: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Running tests

```bash
pip install ".[test]"
pytest tests/ -v
```

All tests use `MemoryAdapter` — no database or Docker required.

```
tests/test_distributor.py::TestRegistration::test_create_root_user              PASSED
tests/test_distributor.py::TestRegistration::test_register_with_referrer        PASSED
tests/test_distributor.py::TestRegistration::test_referrer_not_found_raises     PASSED
tests/test_distributor.py::TestRegistration::test_cycle_detection_self          PASSED
tests/test_distributor.py::TestRegistration::test_cycle_detection_two_nodes     PASSED
tests/test_distributor.py::TestRegistration::test_cycle_detection_deep          PASSED
tests/test_distributor.py::TestRegistration::test_change_referrer               PASSED
tests/test_distributor.py::TestTreeRebuild::test_multi_level_links_after_rebuild PASSED
tests/test_distributor.py::TestDistribution::test_single_level_distribution     PASSED
tests/test_distributor.py::TestDistribution::test_multi_level_distribution      PASSED
tests/test_distributor.py::TestDistribution::test_active_deposit_skip           PASSED
tests/test_distributor.py::TestDistribution::test_income_cap_per_event          PASSED
tests/test_distributor.py::TestDistribution::test_min_accrual_threshold         PASSED
tests/test_distributor.py::TestIdempotency::test_same_tag_same_day_no_duplicate PASSED
tests/test_distributor.py::TestIdempotency::test_different_tags_produce_multiple_accruals PASSED
tests/test_distributor.py::TestAccrualQueries::test_get_user_accruals_returns_records PASSED
tests/test_distributor.py::TestAccrualQueries::test_get_user_total_accrued      PASSED
tests/test_distributor.py::TestConfig::test_from_dict                           PASSED
tests/test_distributor.py::TestConfig::test_get_rate_out_of_range               PASSED
tests/test_distributor.py::TestConfig::test_total_payout_rate                   PASSED
tests/test_distributor.py::TestConfig::test_invalid_rate_raises                 PASSED
tests/test_distributor.py::TestConfig::test_invalid_level_raises                PASSED
```

---

## Local demo (no database needed)

```bash
python examples/quickstart.py
```

```
[TREE] Registered 5 users in a straight chain
  root(1) → l1(2) → l2(3) → l3(4) → src(5)

[DEPOSITS] root, l1, l2: active | l3: none

[DISTRIBUTION]  base=500.00 USDT
  Levels reached : 3
  Levels skipped : 1  (l3 has no deposit)
  Total paid out : 130.000000 USDT

  L2  user_id=4    rate= 10.0%  amount=50.000000
  L3  user_id=3    rate=  8.0%  amount=40.000000
  L4  user_id=2    rate=  6.0%  amount=30.000000
  L5  user_id=1    rate=  5.0%  amount=25.000000  (root skipped l3 → ends at L5)

[IDEMPOTENCY] Second call with same source_tag:
  Levels reached : 0  (all skipped as duplicates)
  Total paid out : 0
```

---

## Extending

**Custom adapter (e.g. MongoDB)**

```python
from referral_engine.adapters.base import BaseAdapter

class MongoAdapter(BaseAdapter):
    def __init__(self, db): self._db = db

    async def get_parent(self, user_id):
        doc = await self._db.referrals.find_one(
            {"user_id": user_id, "level": 1}
        )
        return ReferralLink(**doc) if doc else None

    # … implement remaining 7 methods
```

**Custom distribution hook (e.g. send Telegram notification)**

```python
async with adapter:
    result = await engine.distribute(
        source_user_id=user_id,
        base_amount=profit,
        source_tag=f"deposit_{deposit_id}_profit_{today}",
    )

for accrual in result.accruals:
    await notify_user(
        user_id=accrual.recipient_user_id,
        amount=accrual.accrual_amount,
        level=accrual.level,
    )
```

**Override config at runtime**

```python
# A/B test: higher rates for new users
vip_config = EngineConfig.from_dict({"rates": [20.0, 15.0, 10.0]})
vip_engine  = ReferralEngine(vip_config, adapter)  # same adapter, different rates
```

---

## Project structure

```
referral-engine/
│
├── referral_engine/               # core library — zero framework dependencies
│   ├── __init__.py                # public surface (ReferralEngine, EngineConfig, adapters, exceptions)
│   ├── config.py                  # EngineConfig, LevelConfig, LeaderThreshold — YAML/JSON/dict loaders
│   ├── models.py                  # User, ReferralLink, TreeNode, AccrualRecord, DistributionResult
│   ├── exceptions.py              # CycleDetectedError, UserNotFoundError, DuplicateReferralError, ...
│   ├── tree.py                    # cycle detection (upward DFS), get_tree_up, get_tree_down
│   ├── distributor.py             # distribution algorithm — idempotency, cap, deposit gate
│   ├── engine.py                  # ReferralEngine — public API, wires config + adapter + distributor
│   ├── batch.py                   # BatchDistributor — chunked multi-event processing
│   ├── hooks.py                   # HookRegistry + HookedEngine — post-accrual callbacks
│   ├── analytics.py               # Analytics — level stats, structure volume queries
│   └── adapters/
│       ├── __init__.py
│       ├── base.py                # BaseAdapter — abstract interface, 8 methods + transaction control
│       ├── postgres.py            # PostgresAdapter — asyncpg, recursive CTE for rebuild_tree
│       └── memory.py              # MemoryAdapter — pure-Python, no DB, used in all tests
│
├── api/                           # FastAPI demo — shows how to integrate the library
│   ├── __init__.py
│   ├── main.py                    # lifespan (pool init), app factory, CORS, /health, /config
│   ├── state.py                   # shared mutable app state (pool, config) populated at startup
│   ├── deps.py                    # FastAPI dependency injection — get_adapter(), get_engine()
│   ├── core/
│   │   ├── __init__.py
│   │   └── config.py              # Settings via pydantic-settings — DATABASE_URL, ENGINE_CONFIG
│   └── v1/
│       ├── __init__.py
│       ├── router.py              # mounts users / referrals / distributions routers
│       ├── users.py               # POST /users, PATCH /users/{id}/deposit, GET /users/{id}/accruals
│       ├── referrals.py           # POST /referrals, GET /tree/up|down, POST /rebuild
│       └── distributions.py      # POST /distributions
│
├── migrations/
│   └── 001_schema.sql             # re_users, re_referrals, re_accruals, re_leader_awards + indexes
│
├── tests/
│   ├── __init__.py
│   ├── test_distributor.py        # 22 tests — registration, tree, distribution, idempotency, config
│   └── test_extensions.py        # 14 tests — batch, hooks, hook resilience, analytics
│
├── examples/
│   └── quickstart.py              # runnable demo, MemoryAdapter, no DB or Docker needed
│
├── .github/
│   └── workflows/
│       └── tests.yml              # CI — runs all tests on push/PR
│
├── .gitignore
├── CHANGELOG.md                   # version history
├── CONTRIBUTING.md                # contribution guide
├── LICENSE                        # MIT
├── SECURITY.md                    # vulnerability reporting
├── config.example.yaml            # fully documented configuration template
├── docker-compose.yml             # postgres (schema auto-applied) + api on :8000
├── Dockerfile
├── pyproject.toml                 # deps, optional groups: [api] [test] [dev], pytest config
├── README.md                      # English documentation
└── README.ru.md                   # Русская документация
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

MIT — see [LICENSE](LICENSE).
