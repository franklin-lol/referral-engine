# Contributing to referral-engine

## Setup

```bash
git clone https://github.com/franklin-lol/referral-engine.git
cd referral-engine
pip install -e ".[dev]"
```

## Running tests

```bash
pytest tests/ -v
```

No database required — all tests use `MemoryAdapter`.

## Code style

```bash
ruff check referral_engine/
mypy referral_engine/
```

## What to work on

- New storage adapters (MySQL, SQLite, MongoDB, Redis)
- Additional analytics queries in `analytics.py`
- Performance benchmarks for large trees
- Documentation improvements

## Pull request checklist

- [ ] All existing tests pass
- [ ] New functionality covered by tests
- [ ] Docstrings updated if public API changed
- [ ] `CHANGELOG.md` entry added

## Reporting bugs

Open an issue with a minimal reproducible example. If the bug involves
incorrect financial calculation, include exact input amounts and expected
vs. actual output.
