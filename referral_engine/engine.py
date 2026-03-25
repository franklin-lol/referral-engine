"""
referral_engine.engine
========================
Public entry point.  Import and instantiate this class in your service.

Quick start
-----------
::

    import asyncpg
    from referral_engine import ReferralEngine, EngineConfig, PostgresAdapter

    pool   = await asyncpg.create_pool(dsn)
    config = EngineConfig.from_yaml("referral_config.yaml")

    # Adapter is also the transaction manager
    adapter = PostgresAdapter(pool)
    engine  = ReferralEngine(config, adapter)

    # Register user
    async with adapter:
        user = await engine.register_user("tg_12345678", referrer_id=42)

    # Distribute profit accrual
    async with adapter:
        result = await engine.distribute(
            source_user_id=user.id,
            base_amount=Decimal("150.00"),
            source_tag="deposit_7_profit",
        )

    print(result.summary())
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from referral_engine.adapters.base import BaseAdapter
from referral_engine.config import EngineConfig
from referral_engine.distributor import Distributor
from referral_engine.exceptions import CycleDetectedError, UserNotFoundError
from referral_engine.models import (
    AccrualRecord,
    DistributionResult,
    ReferralLink,
    TreeNode,
    User,
)
from referral_engine.tree import check_cycle, get_tree_down, get_tree_up


class ReferralEngine:
    """
    Pluggable multi-level referral distribution engine.

    All methods require an open adapter transaction.  Use the adapter
    as an async context manager::

        async with adapter:
            await engine.distribute(...)

    The engine itself is stateless — safe to share across coroutines.
    """

    def __init__(self, config: EngineConfig, adapter: BaseAdapter) -> None:
        self.config = config
        self.adapter = adapter
        self._distributor = Distributor(config, adapter)

    # ------------------------------------------------------------------
    # User management
    # ------------------------------------------------------------------

    async def register_user(
        self,
        external_id: str,
        referrer_id: Optional[int] = None,
        rebuild_tree: bool = True,
    ) -> User:
        """
        Create a user and optionally link them to a referrer.

        Parameters
        ----------
        external_id:
            Your system's stable user identifier (Telegram ID, UUID, etc.).
        referrer_id:
            Internal ``re_users.id`` of the referrer.  Pass *None* for
            root-level users.
        rebuild_tree:
            Automatically rebuild multi-level links after insertion.
            Set to *False* when batch-importing users; call
            ``rebuild_tree()`` once at the end.

        Raises
        ------
        UserNotFoundError
            ``referrer_id`` does not exist in the database.
        CycleDetectedError
            Linking would create a cycle (logically impossible for new users,
            but guarded for ``set_referrer`` re-use).
        """
        user = await self.adapter.create_user(external_id)

        if referrer_id is not None:
            referrer = await self.adapter.get_user(referrer_id)
            if referrer is None:
                raise UserNotFoundError(referrer_id)

            if await check_cycle(
                self.adapter, user.id, referrer_id, self.config.max_depth
            ):
                raise CycleDetectedError(user.id, referrer_id)

            await self.adapter.create_referral_link(user.id, referrer_id)

            if rebuild_tree:
                await self.adapter.rebuild_tree(user.id)

        return user

    async def set_referrer(
        self,
        user_id: int,
        referrer_id: int,
        rebuild_tree: bool = True,
    ) -> ReferralLink:
        """
        Assign or change the referrer of an existing user.

        Safe to call multiple times — upsert semantics on the direct link.

        Raises
        ------
        UserNotFoundError
            Either user not found.
        CycleDetectedError
            Would create a cycle.
        """
        for uid in (user_id, referrer_id):
            if await self.adapter.get_user(uid) is None:
                raise UserNotFoundError(uid)

        if await check_cycle(
            self.adapter, user_id, referrer_id, self.config.max_depth
        ):
            raise CycleDetectedError(user_id, referrer_id)

        link = await self.adapter.create_referral_link(user_id, referrer_id)

        if rebuild_tree:
            await self.adapter.rebuild_tree(user_id)

        return link

    async def update_deposit_status(
        self, user_id: int, has_active_deposit: bool
    ) -> None:
        """
        Toggle the active-deposit flag.

        Call this whenever a user opens or closes a deposit in your system.
        When ``config.require_active_deposit=True``, users without an active
        deposit are skipped during distribution (the walk continues upward).
        """
        await self.adapter.update_user_deposit_status(user_id, has_active_deposit)

    # ------------------------------------------------------------------
    # Distribution
    # ------------------------------------------------------------------

    async def distribute(
        self,
        source_user_id: int,
        base_amount: Decimal,
        source_tag: str,
        accrual_date: Optional[datetime] = None,
    ) -> DistributionResult:
        """
        Distribute profit bonuses upward through the referral tree.

        Idempotent — safe to retry.  Duplicate accruals for the same
        ``(source_user_id, source_tag, date)`` triplet are skipped
        automatically.

        Parameters
        ----------
        source_user_id:
            The user whose profit event triggered distribution.
        base_amount:
            Amount bonuses are calculated from (e.g. daily interest).
        source_tag:
            Stable event identifier, e.g. ``"deposit_42_interest"``.
        accrual_date:
            Override the accrual timestamp.  Useful for back-dating
            missed accruals.
        """
        return await self._distributor.distribute(
            source_user_id=source_user_id,
            base_amount=base_amount,
            source_tag=source_tag,
            accrual_date=accrual_date,
        )

    # ------------------------------------------------------------------
    # Tree queries
    # ------------------------------------------------------------------

    async def get_tree_up(
        self,
        user_id: int,
        max_levels: Optional[int] = None,
    ) -> List[TreeNode]:
        """Return the ancestor chain as a flat list, closest first."""
        depth = min(
            max_levels or self.config.max_levels,
            self.config.max_depth,
        )
        return await get_tree_up(self.adapter, user_id, depth)

    async def get_tree_down(
        self,
        user_id: int,
        max_levels: Optional[int] = None,
    ) -> TreeNode:
        """Return a nested ``TreeNode`` subtree rooted at *user_id*."""
        depth = min(
            max_levels or self.config.max_levels,
            self.config.max_depth,
        )
        return await get_tree_down(self.adapter, user_id, depth)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def rebuild_tree(
        self, scope_user_id: Optional[int] = None
    ) -> int:
        """
        Rebuild multi-level referral links from direct (level=1) links.

        Call after bulk imports or when referrer assignments change en masse.
        Uses a recursive CTE — single DB round-trip regardless of tree size.

        Returns
        -------
        int
            Number of records inserted/updated.
        """
        return await self.adapter.rebuild_tree(scope_user_id)

    # ------------------------------------------------------------------
    # Accrual queries
    # ------------------------------------------------------------------

    async def get_user_accruals(
        self,
        user_id: int,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[AccrualRecord]:
        """Fetch accruals credited to *user_id*, newest first."""
        return await self.adapter.get_user_accruals(user_id, since, limit)

    async def get_user_total_accrued(self, user_id: int) -> float:
        """Return the lifetime sum of all accruals for *user_id*."""
        return await self.adapter.get_user_total_accrued(user_id)
