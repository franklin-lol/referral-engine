"""
referral_engine.distributor
============================
Core profit-distribution algorithm.

Walk upward from ``source_user_id``, one hop per referral level.
For each ancestor:

1. Fetch their direct parent (level=1 hop in the *direct-link* table).
2. Check active-deposit requirement (configurable).
3. Apply income cap (configurable).
4. Idempotency check — skip if already issued for this ``source_key``.
5. Persist ``AccrualRecord``.

The walk uses only direct (level=1) links for each step, so the
``re_referrals`` table acts as a *closure table* for fast subtree queries
but the distribution walk itself is O(depth) with one DB read per level.
"""
from __future__ import annotations

from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from typing import List, Optional

from referral_engine.adapters.base import BaseAdapter
from referral_engine.config import EngineConfig
from referral_engine.models import AccrualRecord, DistributionResult


_QUANTIZE = Decimal("0.000001")


class Distributor:
    """
    Stateless distribution engine.  One instance per ``ReferralEngine``.
    """

    def __init__(self, config: EngineConfig, adapter: BaseAdapter) -> None:
        self.config = config
        self.adapter = adapter

    async def distribute(
        self,
        source_user_id: int,
        base_amount: Decimal,
        source_tag: str,
        accrual_date: Optional[datetime] = None,
    ) -> DistributionResult:
        """
        Distribute bonuses upward from a profit event.

        This method is **idempotent**: calling it twice with the same
        ``source_tag`` on the same calendar day produces no duplicates.

        Parameters
        ----------
        source_user_id:
            Internal user ID whose profit event triggered distribution.
        base_amount:
            Amount to calculate percentages from (e.g. daily interest accrual).
        source_tag:
            Unique tag identifying this event family, e.g.
            ``"deposit_42_profit"``.  Combined with date to build the
            idempotency key.
        accrual_date:
            Override the timestamp stored in accrual records.
            Defaults to ``datetime.utcnow()``.

        Returns
        -------
        DistributionResult
            Full audit trail of what was distributed (and skipped).
        """
        if accrual_date is None:
            accrual_date = datetime.utcnow()

        # Validate source user exists — silent success on ghost IDs is a bug
        from referral_engine.exceptions import UserNotFoundError
        source_user = await self.adapter.get_user(source_user_id)
        if source_user is None:
            raise UserNotFoundError(source_user_id)

        date_key = accrual_date.strftime("%Y%m%d")
        accruals: List[AccrualRecord] = []
        total_distributed = Decimal("0")
        skipped = 0

        # Walk upward using direct (level=1) parent links
        current_user_id = source_user_id

        for level_idx in range(1, self.config.max_levels + 1):
            parent_link = await self.adapter.get_parent(current_user_id)
            if parent_link is None:
                break  # reached the root

            referrer_id = parent_link.referrer_id
            rate = self.config.get_rate(level_idx)

            if rate <= 0:
                current_user_id = referrer_id
                skipped += 1
                continue

            # ── Active deposit check ─────────────────────────────────────
            if self.config.require_active_deposit:
                has_deposit = await self.adapter.user_has_active_deposit(referrer_id)
                if not has_deposit:
                    current_user_id = referrer_id
                    skipped += 1
                    continue

            # ── Calculate amount ─────────────────────────────────────────
            amount = (
                base_amount * Decimal(str(rate)) / Decimal("100")
            ).quantize(_QUANTIZE, rounding=ROUND_DOWN)

            # ── Income cap ───────────────────────────────────────────────
            if self.config.income_cap_per_event is not None:
                cap = Decimal(str(self.config.income_cap_per_event))
                amount = min(amount, cap)

            # ── Minimum threshold ────────────────────────────────────────
            if amount < Decimal(str(self.config.min_accrual_amount)):
                current_user_id = referrer_id
                skipped += 1
                continue

            # ── Idempotency ──────────────────────────────────────────────
            source_key = f"{source_user_id}_{source_tag}_{date_key}"
            already_issued = await self.adapter.accrual_exists(
                source_key, referrer_id, level_idx
            )
            if already_issued:
                current_user_id = referrer_id
                continue

            # ── Persist ──────────────────────────────────────────────────
            record = AccrualRecord(
                source_user_id=source_user_id,
                recipient_user_id=referrer_id,
                level=level_idx,
                base_amount=base_amount,
                accrual_rate=rate,
                accrual_amount=amount,
                source_key=source_key,
                accrued_at=accrual_date,
                source_tag=source_tag,
            )
            saved = await self.adapter.save_accrual(record)
            accruals.append(saved)
            total_distributed += amount

            current_user_id = referrer_id

        return DistributionResult(
            source_user_id=source_user_id,
            base_amount=base_amount,
            source_tag=source_tag,
            accruals=accruals,
            total_distributed=total_distributed,
            skipped_levels=skipped,
        )
