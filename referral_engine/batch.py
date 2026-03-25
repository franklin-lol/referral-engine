"""
referral_engine.batch
======================
Batch distribution — process multiple source events in a single
adapter transaction, or in configurable chunks to avoid long-running
transactions on large datasets.

Typical use case: daily profit accrual job that processes 10 000+
active deposits in one run.

Usage::

    from referral_engine.batch import BatchDistributor

    batch = BatchDistributor(engine, chunk_size=500)

    results = await batch.distribute_many(
        events=[
            BatchEvent(source_user_id=1, base_amount=Decimal("120.00"), source_tag="dep_1_profit_20250625"),
            BatchEvent(source_user_id=2, base_amount=Decimal("85.50"),  source_tag="dep_2_profit_20250625"),
            # …
        ],
        on_error="skip",   # "skip" | "raise"
    )

    print(f"Processed: {results.ok}  Failed: {results.failed}")
    print(f"Total distributed: {results.total_distributed}")
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import List, Literal, Optional

from referral_engine.engine import ReferralEngine
from referral_engine.models import DistributionResult

logger = logging.getLogger(__name__)


@dataclass
class BatchEvent:
    source_user_id: int
    base_amount: Decimal
    source_tag: str
    accrual_date: Optional[datetime] = None


@dataclass
class BatchResult:
    ok: int = 0
    failed: int = 0
    total_distributed: Decimal = Decimal("0")
    errors: List[tuple] = field(default_factory=list)   # [(source_user_id, exc), …]
    results: List[DistributionResult] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "ok": self.ok,
            "failed": self.failed,
            "total_distributed": str(self.total_distributed),
            "error_user_ids": [uid for uid, _ in self.errors],
        }


class BatchDistributor:
    """
    Wraps ``ReferralEngine.distribute`` for bulk processing.

    Transaction strategy
    --------------------
    Each **chunk** runs inside one adapter transaction.  A failure in
    an event inside a chunk does not roll back events already committed
    in previous chunks.  Within a chunk, ``on_error="skip"`` catches
    per-event exceptions without rolling back the whole chunk.

    Parameters
    ----------
    engine:
        Configured ``ReferralEngine`` instance.
    chunk_size:
        Number of events per transaction.  Tune based on your DB
        transaction size limits.  Default 200.
    """

    def __init__(self, engine: ReferralEngine, chunk_size: int = 200) -> None:
        self.engine = engine
        self.chunk_size = chunk_size

    async def distribute_many(
        self,
        events: List[BatchEvent],
        on_error: Literal["skip", "raise"] = "skip",
    ) -> BatchResult:
        """
        Process all *events*, chunked into transactions of ``chunk_size``.

        Parameters
        ----------
        events:
            List of distribution events to process.
        on_error:
            ``"skip"`` — log the error, record it in ``BatchResult.errors``,
            continue processing remaining events.
            ``"raise"`` — re-raise the first exception encountered.
        """
        aggregate = BatchResult()
        adapter = self.engine.adapter

        for chunk_start in range(0, len(events), self.chunk_size):
            chunk = events[chunk_start : chunk_start + self.chunk_size]

            async with adapter:
                for event in chunk:
                    try:
                        result = await self.engine.distribute(
                            source_user_id=event.source_user_id,
                            base_amount=event.base_amount,
                            source_tag=event.source_tag,
                            accrual_date=event.accrual_date,
                        )
                        aggregate.ok += 1
                        aggregate.total_distributed += result.total_distributed
                        aggregate.results.append(result)

                    except Exception as exc:
                        logger.error(
                            "BatchDistributor error for user %d tag=%s: %s",
                            event.source_user_id,
                            event.source_tag,
                            exc,
                        )
                        aggregate.failed += 1
                        aggregate.errors.append((event.source_user_id, exc))

                        if on_error == "raise":
                            raise

        return aggregate
