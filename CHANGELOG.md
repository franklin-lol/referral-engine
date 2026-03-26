# Changelog

All notable changes to this project will be documented in this file.

Format: [Semantic Versioning](https://semver.org/)

---

## [1.0.0] — 2025-06-25

### Added
- `ReferralEngine` — core distribution engine
- `EngineConfig` — declarative config with YAML/JSON/dict loaders
- `BaseAdapter` — abstract storage interface (8 methods + transaction control)
- `PostgresAdapter` — asyncpg implementation with recursive CTE for `rebuild_tree`
- `MemoryAdapter` — pure-Python in-memory adapter for tests
- `BatchDistributor` — chunked multi-event processing for cron jobs
- `HookRegistry` + `HookedEngine` — post-accrual callback system
- `Analytics` — level stats and structure volume queries
- Cycle detection via upward DFS — O(depth), raises `CycleDetectedError`
- Idempotency via compound key `(source_user_id, source_tag, date)`
- Active-deposit gate — skip ancestors without deposits, walk continues upward
- Income cap per distribution event
- Minimum accrual threshold
- Leader bonus threshold ladder
- FastAPI demo application with `/health`, `/config`, and full CRUD
- PostgreSQL migration `001_schema.sql` with indexes and constraints
- 33 unit tests — no database required
- Docker Compose setup
- GitHub Actions CI (Python 3.11 + 3.12)
- English and Russian documentation
