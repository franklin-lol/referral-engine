"""
tests.test_extensions
=====================
Tests for batch distributor, hook system, and analytics.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from referral_engine.config import EngineConfig
from referral_engine.engine import ReferralEngine
from referral_engine.adapters.memory import MemoryAdapter
from referral_engine.batch import BatchDistributor, BatchEvent
from referral_engine.hooks import HookRegistry, HookedEngine
from referral_engine.analytics import Analytics


@pytest.fixture
def cfg():
    return EngineConfig.from_dict(
        {"rates": [12.0, 10.0, 8.0], "require_active_deposit": False}
    )


@pytest.fixture
def adapter():
    return MemoryAdapter()


@pytest.fixture
def engine(cfg, adapter):
    return ReferralEngine(cfg, adapter)


# ---------------------------------------------------------------------------
# Batch distributor
# ---------------------------------------------------------------------------

class TestBatch:
    @pytest.mark.asyncio
    async def test_batch_distributes_all_events(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
            bob   = await engine.register_user("bob",   referrer_id=alice.id)
            carol = await engine.register_user("carol", referrer_id=alice.id)

        batch = BatchDistributor(engine, chunk_size=10)
        events = [
            BatchEvent(bob.id,   Decimal("100"), "bob_profit_d1"),
            BatchEvent(carol.id, Decimal("200"), "carol_profit_d1"),
        ]
        result = await batch.distribute_many(events)

        assert result.ok == 2
        assert result.failed == 0
        # alice receives 12% of 100 + 12% of 200 = 12 + 24 = 36
        async with adapter:
            total = await engine.get_user_total_accrued(alice.id)
        assert abs(total - 36.0) < 0.001

    @pytest.mark.asyncio
    async def test_batch_skip_on_error(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
            bob   = await engine.register_user("bob", referrer_id=alice.id)

        batch = BatchDistributor(engine, chunk_size=10)
        events = [
            BatchEvent(9999,    Decimal("100"), "bad_user"),   # non-existent user
            BatchEvent(bob.id,  Decimal("100"), "good_profit"),
        ]
        # on_error="skip" — bad event skipped, good one processed
        result = await batch.distribute_many(events, on_error="skip")

        assert result.failed == 1
        assert result.ok == 1

    @pytest.mark.asyncio
    async def test_batch_chunking(self, engine, adapter):
        """chunk_size=2 with 5 events — should still process all."""
        async with adapter:
            root = await engine.register_user("root")
            users = []
            for i in range(5):
                u = await engine.register_user(f"u{i}", referrer_id=root.id)
                users.append(u)

        events = [
            BatchEvent(u.id, Decimal("100"), f"profit_u{i}_d1")
            for i, u in enumerate(users)
        ]
        batch = BatchDistributor(engine, chunk_size=2)
        result = await batch.distribute_many(events)

        assert result.ok == 5
        async with adapter:
            total = await engine.get_user_total_accrued(root.id)
        assert abs(total - 60.0) < 0.001  # 5 × 12%


# ---------------------------------------------------------------------------
# Hook system
# ---------------------------------------------------------------------------

class TestHooks:
    @pytest.mark.asyncio
    async def test_accrual_created_hook_fires(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
            bob   = await engine.register_user("bob", referrer_id=alice.id)

        registry = HookRegistry()
        fired = []

        @registry.on("accrual_created")
        async def capture(event):
            fired.append(event)

        hooked = HookedEngine(engine, registry)

        async with adapter:
            await hooked.distribute(bob.id, Decimal("100"), "hook_test")

        assert len(fired) == 1
        assert fired[0].accrual.recipient_user_id == alice.id

    @pytest.mark.asyncio
    async def test_distribution_complete_hook(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
            bob   = await engine.register_user("bob", referrer_id=alice.id)

        registry = HookRegistry()
        complete_events = []

        registry.register("distribution_complete", lambda e: complete_events.append(e) or asyncio.sleep(0))

        hooked = HookedEngine(engine, registry)

        async with adapter:
            await hooked.distribute(bob.id, Decimal("100"), "complete_test")

        assert len(complete_events) == 1

    @pytest.mark.asyncio
    async def test_distribution_empty_hook_when_no_referrer(self, engine, adapter):
        async with adapter:
            lone = await engine.register_user("lone_wolf")

        registry = HookRegistry()
        empty_events = []
        complete_events = []

        @registry.on("distribution_empty")
        async def on_empty(event): empty_events.append(event)

        @registry.on("distribution_complete")
        async def on_complete(event): complete_events.append(event)

        hooked = HookedEngine(engine, registry)

        async with adapter:
            await hooked.distribute(lone.id, Decimal("100"), "empty_test")

        assert len(empty_events) == 1
        assert len(complete_events) == 0

    @pytest.mark.asyncio
    async def test_hook_exception_does_not_break_result(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
            bob   = await engine.register_user("bob", referrer_id=alice.id)

        registry = HookRegistry()

        @registry.on("accrual_created")
        async def bad_hook(event):
            raise RuntimeError("Notification service down")

        hooked = HookedEngine(engine, registry)

        # Must NOT raise even though hook raises
        async with adapter:
            result = await hooked.distribute(bob.id, Decimal("100"), "resilience_test")

        assert result.levels_reached == 1

    @pytest.mark.asyncio
    async def test_multiple_handlers_all_fire(self, engine, adapter):
        async with adapter:
            alice = await engine.register_user("alice")
            bob   = await engine.register_user("bob", referrer_id=alice.id)

        registry = HookRegistry()
        calls = []

        for i in range(3):
            idx = i
            @registry.on("accrual_created")
            async def h(event, _i=idx): calls.append(_i)

        hooked = HookedEngine(engine, registry)
        async with adapter:
            await hooked.distribute(bob.id, Decimal("100"), "multi_hook")

        assert sorted(calls) == [0, 1, 2]


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

class TestAnalytics:
    @pytest.mark.asyncio
    async def test_structure_volume(self, engine, adapter):
        async with adapter:
            root  = await engine.register_user("root")
            child1 = await engine.register_user("c1", referrer_id=root.id)
            child2 = await engine.register_user("c2", referrer_id=root.id)
            grand  = await engine.register_user("g1", referrer_id=child1.id)

        async with adapter:
            vol = await Analytics(adapter).structure_volume(root.id)

        assert vol.direct_members == 2
        assert vol.total_members == 3   # c1, c2, g1

    @pytest.mark.asyncio
    async def test_level_stats(self, engine, adapter):
        cfg = EngineConfig.from_dict(
            {"rates": [10.0, 5.0], "require_active_deposit": False}
        )
        eng = ReferralEngine(cfg, adapter)

        async with adapter:
            root  = await eng.register_user("root")
            child = await eng.register_user("child", referrer_id=root.id)
            leaf  = await eng.register_user("leaf",  referrer_id=child.id)

        async with adapter:
            await eng.distribute(leaf.id, Decimal("100"), "stats_test")

        async with adapter:
            stats = await Analytics(adapter).level_stats(root.id)

        assert len(stats) == 2
        levels = {s.level: s for s in stats}
        # child at L1: 10% of 100 = 10
        # child received 10% of 100 = 10 from leaf's distribution
        assert levels[1].total_accrued == Decimal("10.000000")
        # leaf is the SOURCE — it received nothing → its accruals = 0
        assert levels[2].total_accrued == Decimal("0")
