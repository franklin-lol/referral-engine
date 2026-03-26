"""
examples/quickstart.py
=======================
Demonstrates the full engine lifecycle with the in-memory adapter —
no database or Docker required.

Run:
    python examples/quickstart.py
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

from referral_engine import EngineConfig, ReferralEngine
from referral_engine.adapters.memory import MemoryAdapter
from referral_engine.config import LevelConfig


# ---------------------------------------------------------------------------
# 1. Custom config  (override per product need)
# ---------------------------------------------------------------------------

config = EngineConfig.from_dict(
    {
        "rates": [12.0, 10.0, 8.0, 6.0, 5.0, 4.0, 3.0, 3.0, 2.0, 2.0],
        "require_active_deposit": True,
        "income_cap_per_event": None,       # no cap
        "min_accrual_amount": 0.000001,
    }
)


# ---------------------------------------------------------------------------
# 2. Wire up
# ---------------------------------------------------------------------------

adapter = MemoryAdapter()
engine  = ReferralEngine(config, adapter)


# ---------------------------------------------------------------------------
# 3. Demo
# ---------------------------------------------------------------------------

async def main() -> None:
    print("=" * 60)
    print("  referral-engine  •  quickstart demo")
    print("=" * 60)

    # ── Register users ──────────────────────────────────────────────────────
    async with adapter:
        root  = await engine.register_user("root_user")
        l1    = await engine.register_user("level1_user",  referrer_id=root.id)
        l2    = await engine.register_user("level2_user",  referrer_id=l1.id)
        l3    = await engine.register_user("level3_user",  referrer_id=l2.id)
        src   = await engine.register_user("source_user",  referrer_id=l3.id)

    print(f"\n[TREE] Registered 5 users in a straight chain")
    print(f"  root({root.id}) → l1({l1.id}) → l2({l2.id}) → l3({l3.id}) → src({src.id})")

    # ── Activate deposits (root, l1, l2 have deposits; l3 does NOT) ─────────
    async with adapter:
        await engine.update_deposit_status(root.id, True)
        await engine.update_deposit_status(l1.id, True)
        await engine.update_deposit_status(l2.id, True)
        # l3 intentionally left without active deposit

    print("\n[DEPOSITS] root, l1, l2: active | l3: none")

    # ── Tree inspection ──────────────────────────────────────────────────────
    async with adapter:
        ancestors = await engine.get_tree_up(src.id)
    print(f"\n[TREE UP] Ancestors of src({src.id}):")
    for node in ancestors:
        print(f"  level {node.level} → user_id={node.user_id} ({node.external_id})")

    # ── Distribute profit ────────────────────────────────────────────────────
    async with adapter:
        result = await engine.distribute(
            source_user_id=src.id,
            base_amount=Decimal("500.00"),
            source_tag="deposit_42_profit_day1",
        )

    print(f"\n[DISTRIBUTION]  base=500.00 USDT")
    print(f"  Levels reached : {result.levels_reached}")
    print(f"  Levels skipped : {result.skipped_levels}  (l3 has no deposit)")
    print(f"  Total paid out : {result.total_distributed} USDT")
    print()
    for accrual in result.accruals:
        print(
            f"  L{accrual.level:>2}  user_id={accrual.recipient_user_id:<5}"
            f"  rate={accrual.accrual_rate:>5.1f}%"
            f"  amount={accrual.accrual_amount}"
        )

    # ── Idempotency check ────────────────────────────────────────────────────
    async with adapter:
        result2 = await engine.distribute(
            source_user_id=src.id,
            base_amount=Decimal("500.00"),
            source_tag="deposit_42_profit_day1",   # same tag!
        )

    print(f"\n[IDEMPOTENCY] Second call with same source_tag:")
    print(f"  Levels reached : {result2.levels_reached}  (should be 0 — all skipped as duplicates)")
    print(f"  Total paid out : {result2.total_distributed}  (should be 0)")

    # ── Second day — different tag ────────────────────────────────────────────
    async with adapter:
        result3 = await engine.distribute(
            source_user_id=src.id,
            base_amount=Decimal("500.00"),
            source_tag="deposit_42_profit_day2",   # new tag
        )
    print(f"\n[DAY 2] New source_tag → fresh distribution")
    print(f"  Levels reached : {result3.levels_reached}")

    # ── Total accrued per user ────────────────────────────────────────────────
    print("\n[TOTALS]")
    for user in (root, l1, l2, l3, src):
        async with adapter:
            total = await engine.get_user_total_accrued(user.id)
        print(f"  {user.external_id:<20} total accrued = {total:.6f} USDT")

    # ── JSON summary ─────────────────────────────────────────────────────────
    print("\n[SUMMARY JSON]")
    print(json.dumps(result.summary(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
