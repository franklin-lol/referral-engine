"""
tests.test_distributor
=======================
Covers the core distribution algorithm using the in-memory adapter.
No database or network required.

Run with:
    pytest tests/ -v
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
import pytest_asyncio

from referral_engine import EngineConfig, ReferralEngine
from referral_engine.adapters.memory import MemoryAdapter
from referral_engine.config import LevelConfig
from referral_engine.exceptions import CycleDetectedError, UserNotFoundError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config() -> EngineConfig:
    """Default 15-level config."""
    return EngineConfig()


@pytest.fixture
def config_no_deposit_check() -> EngineConfig:
    return EngineConfig(require_active_deposit=False)


@pytest.fixture
def config_capped() -> EngineConfig:
    """All levels 10%, single-event income cap of 5.0."""
    levels = [LevelConfig(level=i, rate=10.0) for i in range(1, 6)]
    return EngineConfig(levels=levels, income_cap_per_event=5.0)


@pytest.fixture
def adapter() -> MemoryAdapter:
    return MemoryAdapter()


@pytest.fixture
def engine(config, adapter) -> ReferralEngine:
    return ReferralEngine(config, adapter)


@pytest.fixture
def engine_no_check(config_no_deposit_check, adapter) -> ReferralEngine:
    return ReferralEngine(config_no_deposit_check, adapter)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def make_chain(engine: ReferralEngine, adapter: MemoryAdapter, depth: int):
    """
    Build a straight chain:  root → u1 → u2 → … → u{depth}
    Returns list of users, index 0 = root.
    """
    users = []
    async with adapter:
        root = await engine.register_user("root")
        users.append(root)

    for i in range(1, depth + 1):
        async with adapter:
            u = await engine.register_user(
                f"u{i}", referrer_id=users[-1].id
            )
            users.append(u)

    return users


# ---------------------------------------------------------------------------
# Registration & linking
# ---------------------------------------------------------------------------

class TestRegistration:
    @pytest.mark.asyncio
    async def test_create_root_user(self, engine, adapter):
        async with adapter:
            user = await engine.register_user("alice")
        assert user.id is not None
        assert user.external_id == "alice"

    @pytest.mark.asyncio
    async def test_register_with_referrer(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
        async with adapter:
            bob = await engine.register_user("bob", referrer_id=alice.id)

        async with adapter:
            chain = await engine.get_tree_up(bob.id)
        assert len(chain) == 1
        assert chain[0].user_id == alice.id

    @pytest.mark.asyncio
    async def test_referrer_not_found_raises(self, engine, adapter):
        with pytest.raises(UserNotFoundError):
            async with adapter:
                await engine.register_user("ghost", referrer_id=99999)

    @pytest.mark.asyncio
    async def test_cycle_detection_self(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
        with pytest.raises(CycleDetectedError):
            async with adapter:
                await engine.set_referrer(alice.id, alice.id)

    @pytest.mark.asyncio
    async def test_cycle_detection_two_nodes(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
        async with adapter:
            bob = await engine.register_user("bob", referrer_id=alice.id)

        # alice → bob would create a cycle (alice is already bob's ancestor)
        with pytest.raises(CycleDetectedError):
            async with adapter:
                await engine.set_referrer(alice.id, bob.id)

    @pytest.mark.asyncio
    async def test_cycle_detection_deep(self, engine, adapter):
        """A → B → C → D; try to set A's parent to D — must raise."""
        users = await make_chain(engine, adapter, depth=3)
        a, b, c, d = users
        with pytest.raises(CycleDetectedError):
            async with adapter:
                await engine.set_referrer(a.id, d.id)

    @pytest.mark.asyncio
    async def test_change_referrer(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
            carol = await engine.register_user("carol")
        async with adapter:
            bob = await engine.register_user("bob", referrer_id=alice.id)

        # Move bob under carol
        async with adapter:
            link = await engine.set_referrer(bob.id, carol.id)
        assert link.referrer_id == carol.id

        async with adapter:
            chain = await engine.get_tree_up(bob.id)
        assert chain[0].user_id == carol.id


# ---------------------------------------------------------------------------
# Tree rebuild
# ---------------------------------------------------------------------------

class TestTreeRebuild:
    @pytest.mark.asyncio
    async def test_multi_level_links_after_rebuild(self, engine, adapter):
        """After rebuild, level-2 links must exist."""
        users = await make_chain(engine, adapter, depth=4)
        root, u1, u2, u3, u4 = users

        async with adapter:
            chain = await engine.get_tree_up(u4.id)

        levels = {n.level: n.user_id for n in chain}
        assert levels[1] == u3.id
        assert levels[2] == u2.id
        assert levels[3] == u1.id
        assert levels[4] == root.id

    @pytest.mark.asyncio
    async def test_rebuild_returns_count(self, engine, adapter):
        await make_chain(engine, adapter, depth=5)
        async with adapter:
            n = await engine.rebuild_tree()
        assert n >= 0  # at minimum did not fail


# ---------------------------------------------------------------------------
# Distribution algorithm
# ---------------------------------------------------------------------------

class TestDistribution:
    @pytest.mark.asyncio
    async def test_single_level_distribution(self, engine_no_check, adapter):
        """
        Direct referrer (L1) earns 12% of 100 = 12.000000
        """
        async with adapter:
            alice = await engine_no_check.register_user("alice")
        async with adapter:
            bob = await engine_no_check.register_user("bob", referrer_id=alice.id)

        async with adapter:
            result = await engine_no_check.distribute(
                source_user_id=bob.id,
                base_amount=Decimal("100.00"),
                source_tag="test_single",
            )

        assert result.levels_reached == 1
        assert result.accruals[0].recipient_user_id == alice.id
        assert result.accruals[0].accrual_rate == 12.0
        assert result.accruals[0].accrual_amount == Decimal("12.000000")
        assert result.total_distributed == Decimal("12.000000")

    @pytest.mark.asyncio
    async def test_multi_level_distribution(self, engine_no_check, adapter):
        """
        Chain: root → L1 → L2 → L3 → source
        source distributes 200:
          L1 = 12%  → 24
          L2 = 10%  → 20
          L3 = 8%   → 16
        """
        users = await make_chain(engine_no_check, adapter, depth=4)
        root, l1, l2, l3, source = users

        async with adapter:
            result = await engine_no_check.distribute(
                source_user_id=source.id,
                base_amount=Decimal("200.00"),
                source_tag="test_multi",
            )

        assert result.levels_reached == 4

        amounts = {a.level: a.accrual_amount for a in result.accruals}
        assert amounts[1] == Decimal("24.000000")   # 12%
        assert amounts[2] == Decimal("20.000000")   # 10%
        assert amounts[3] == Decimal("16.000000")   # 8%
        assert amounts[4] == Decimal("12.000000")   # 6%

    @pytest.mark.asyncio
    async def test_active_deposit_skip(self, engine, adapter):
        """
        require_active_deposit=True (default config).
        alice has no active deposit → skipped.
        root has deposit → receives L2 payout (walk continues past alice).
        """
        async with adapter:
            root = await engine.register_user("root")
            await engine.update_deposit_status(root.id, True)
        async with adapter:
            alice = await engine.register_user("alice", referrer_id=root.id)
            # alice has no deposit — will be skipped
        async with adapter:
            bob = await engine.register_user("bob", referrer_id=alice.id)

        async with adapter:
            result = await engine.distribute(
                source_user_id=bob.id,
                base_amount=Decimal("100.00"),
                source_tag="test_skip",
            )

        # alice skipped, root receives level-2 rate (10%)
        recipients = {a.recipient_user_id for a in result.accruals}
        assert alice.id not in recipients
        assert root.id in recipients
        assert result.skipped_levels == 1

    @pytest.mark.asyncio
    async def test_no_skip_when_deposit_required_false(
        self, engine_no_check, adapter
    ):
        """All ancestors paid regardless of deposit status."""
        async with adapter:
            alice = await engine_no_check.register_user("alice")
        async with adapter:
            bob = await engine_no_check.register_user("bob", referrer_id=alice.id)

        async with adapter:
            result = await engine_no_check.distribute(
                source_user_id=bob.id,
                base_amount=Decimal("50.00"),
                source_tag="test_noskip",
            )

        assert result.levels_reached == 1
        assert result.skipped_levels == 0

    @pytest.mark.asyncio
    async def test_income_cap_per_event(self, config_capped, adapter):
        """Income cap: each recipient can earn at most 5.0 per event."""
        engine = ReferralEngine(config_capped, adapter)
        async with adapter:
            alice = await engine.register_user("alice")
        async with adapter:
            bob = await engine.register_user("bob", referrer_id=alice.id)

        async with adapter:
            result = await engine.distribute(
                source_user_id=bob.id,
                base_amount=Decimal("1000.00"),
                source_tag="test_cap",
            )

        # 10% of 1000 = 100, but cap is 5.0
        assert result.accruals[0].accrual_amount == Decimal("5.000000")

    @pytest.mark.asyncio
    async def test_min_accrual_threshold(self, adapter):
        """Amounts below min_accrual_amount are silently dropped."""
        config = EngineConfig(
            levels=[LevelConfig(level=1, rate=0.0001)],
            require_active_deposit=False,
            min_accrual_amount=0.01,
        )
        engine = ReferralEngine(config, adapter)
        async with adapter:
            alice = await engine.register_user("alice")
        async with adapter:
            bob = await engine.register_user("bob", referrer_id=alice.id)

        async with adapter:
            result = await engine.distribute(
                source_user_id=bob.id,
                base_amount=Decimal("1.00"),
                source_tag="test_min",
            )

        # 0.0001% of 1.00 = 0.000001 < 0.01 threshold → skipped
        assert result.levels_reached == 0
        assert result.skipped_levels == 1


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

class TestIdempotency:
    @pytest.mark.asyncio
    async def test_same_tag_same_day_no_duplicate(
        self, engine_no_check, adapter
    ):
        """Calling distribute twice with same tag → only one accrual."""
        async with adapter:
            alice = await engine_no_check.register_user("alice")
        async with adapter:
            bob = await engine_no_check.register_user("bob", referrer_id=alice.id)

        for _ in range(3):
            async with adapter:
                await engine_no_check.distribute(
                    source_user_id=bob.id,
                    base_amount=Decimal("100.00"),
                    source_tag="deposit_1_profit",
                )

        async with adapter:
            total = await engine_no_check.get_user_total_accrued(alice.id)

        # Only one accrual of 12 should exist
        assert total == pytest.approx(12.0)

    @pytest.mark.asyncio
    async def test_different_tags_produce_multiple_accruals(
        self, engine_no_check, adapter
    ):
        async with adapter:
            alice = await engine_no_check.register_user("alice")
        async with adapter:
            bob = await engine_no_check.register_user("bob", referrer_id=alice.id)

        for day in range(3):
            async with adapter:
                await engine_no_check.distribute(
                    source_user_id=bob.id,
                    base_amount=Decimal("100.00"),
                    source_tag=f"deposit_1_profit_day{day}",
                )

        async with adapter:
            total = await engine_no_check.get_user_total_accrued(alice.id)

        assert total == pytest.approx(36.0)   # 3 × 12.0


# ---------------------------------------------------------------------------
# Accrual queries
# ---------------------------------------------------------------------------

class TestAccrualQueries:
    @pytest.mark.asyncio
    async def test_get_user_accruals_returns_records(
        self, engine_no_check, adapter
    ):
        async with adapter:
            alice = await engine_no_check.register_user("alice")
        async with adapter:
            bob = await engine_no_check.register_user("bob", referrer_id=alice.id)

        async with adapter:
            await engine_no_check.distribute(
                source_user_id=bob.id,
                base_amount=Decimal("200.00"),
                source_tag="q_test",
            )

        async with adapter:
            records = await engine_no_check.get_user_accruals(alice.id)

        assert len(records) == 1
        assert records[0].level == 1
        assert records[0].accrual_rate == 12.0

    @pytest.mark.asyncio
    async def test_get_user_total_accrued(self, engine_no_check, adapter):
        async with adapter:
            alice = await engine_no_check.register_user("alice")
        async with adapter:
            bob = await engine_no_check.register_user("bob", referrer_id=alice.id)

        for i in range(5):
            async with adapter:
                await engine_no_check.distribute(
                    source_user_id=bob.id,
                    base_amount=Decimal("100.00"),
                    source_tag=f"profit_day_{i}",
                )

        async with adapter:
            total = await engine_no_check.get_user_total_accrued(alice.id)

        assert total == pytest.approx(60.0)   # 5 × 12.0


# ---------------------------------------------------------------------------
# Config edge cases
# ---------------------------------------------------------------------------

class TestConfig:
    def test_from_dict(self):
        cfg = EngineConfig.from_dict(
            {"rates": [10.0, 8.0, 5.0], "require_active_deposit": False}
        )
        assert cfg.max_levels == 3
        assert cfg.get_rate(1) == 10.0
        assert cfg.require_active_deposit is False

    def test_get_rate_out_of_range(self):
        cfg = EngineConfig.from_dict({"rates": [10.0, 5.0]})
        assert cfg.get_rate(99) == 0.0

    def test_total_payout_rate(self):
        cfg = EngineConfig.from_dict({"rates": [10.0, 10.0, 10.0]})
        assert cfg.total_payout_rate() == 30.0

    def test_invalid_rate_raises(self):
        with pytest.raises(ValueError):
            LevelConfig(level=1, rate=150.0)

    def test_invalid_level_raises(self):
        with pytest.raises(ValueError):
            LevelConfig(level=0, rate=10.0)
