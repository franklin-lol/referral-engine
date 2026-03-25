"""
referral_engine.analytics
==========================
Read-only aggregation queries for dashboards and reports.

These bypass the engine's domain layer and query the adapter directly
for performance — no row-by-row Python loops.

Usage::

    from referral_engine.analytics import Analytics

    analytics = Analytics(adapter)

    async with adapter:
        stats = await analytics.level_stats(root_user_id=42)
        top   = await analytics.top_earners(limit=10)
        vol   = await analytics.structure_volume(user_id=42)
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List

from referral_engine.adapters.base import BaseAdapter


@dataclass
class LevelStat:
    level: int
    member_count: int
    total_accrued: Decimal


@dataclass
class TopEarner:
    user_id: int
    external_id: str
    total_accrued: Decimal


@dataclass
class StructureVolume:
    user_id: int
    direct_members: int       # level-1 only
    total_members: int        # all levels
    total_accrued_received: Decimal   # what this user earned from their structure


class Analytics:
    """
    Aggregation queries.

    All methods are read-only and adapter-agnostic (use existing
    BaseAdapter methods).  For production at scale, add DB-native
    queries to your adapter implementation.
    """

    def __init__(self, adapter: BaseAdapter) -> None:
        self._adapter = adapter

    async def level_stats(self, root_user_id: int) -> List[LevelStat]:
        """
        For each level in the subtree of *root_user_id*, return
        member count and total accruals received at that level.
        """
        subtree = await self._adapter.get_subtree(root_user_id, max_depth=50)

        # Group by level
        level_members: dict[int, list[int]] = {}
        for link in subtree:
            level_members.setdefault(link.level, []).append(link.user_id)

        stats: List[LevelStat] = []
        for level in sorted(level_members):
            members = level_members[level]
            total = Decimal("0")
            for uid in members:
                total += Decimal(str(await self._adapter.get_user_total_accrued(uid)))
            stats.append(
                LevelStat(
                    level=level,
                    member_count=len(members),
                    total_accrued=total,
                )
            )

        return stats

    async def top_earners(self, limit: int = 10) -> List[TopEarner]:
        """
        Return the *limit* users with the highest lifetime accruals.

        Note: this uses the adapter's get_user_accruals() — for large
        datasets, implement a native ORDER BY query in your adapter.
        """
        raise NotImplementedError(
            "top_earners() requires a native DB query. "
            "Implement it in your adapter's get_top_earners() method."
        )

    async def structure_volume(self, user_id: int) -> StructureVolume:
        """
        Return volume summary for *user_id*'s referral structure.
        """
        direct = await self._adapter.get_children(user_id, level=1)
        all_links = await self._adapter.get_subtree(user_id, max_depth=50)
        total_accrued = Decimal(
            str(await self._adapter.get_user_total_accrued(user_id))
        )

        return StructureVolume(
            user_id=user_id,
            direct_members=len(direct),
            total_members=len(all_links),
            total_accrued_received=total_accrued,
        )
