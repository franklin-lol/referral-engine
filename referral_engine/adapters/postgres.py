"""
referral_engine.adapters.postgres
===================================
asyncpg-based PostgreSQL adapter.

Tree rebuild is implemented with a recursive CTE — no Python loops for
large trees.  The UNIQUE constraint on (user_id, level) enables
idempotent upserts.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

import asyncpg

from referral_engine.adapters.base import BaseAdapter
from referral_engine.exceptions import AdapterError
from referral_engine.models import AccrualRecord, ReferralLink, User


class PostgresAdapter(BaseAdapter):
    """
    Usage::

        pool = await asyncpg.create_pool(dsn)
        async with PostgresAdapter(pool) as adapter:
            engine = ReferralEngine(config, adapter)
            result = await engine.distribute(...)
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._conn: Optional[asyncpg.Connection] = None
        self._tx: Optional[asyncpg.transaction.Transaction] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def conn(self) -> asyncpg.Connection:
        if self._conn is None:
            raise AdapterError(
                "No active connection. Use 'async with adapter:' to open a transaction."
            )
        return self._conn

    # ------------------------------------------------------------------
    # Transaction control
    # ------------------------------------------------------------------

    async def begin(self) -> None:
        self._conn = await self._pool.acquire()
        self._tx = self._conn.transaction()
        await self._tx.start()

    async def commit(self) -> None:
        await self._tx.commit()
        await self._pool.release(self._conn)
        self._conn = None

    async def rollback(self) -> None:
        await self._tx.rollback()
        await self._pool.release(self._conn)
        self._conn = None

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------

    async def get_user(self, user_id: int) -> Optional[User]:
        row = await self.conn.fetchrow(
            "SELECT * FROM re_users WHERE id = $1", user_id
        )
        return _map_user(row) if row else None

    async def get_user_by_external_id(self, external_id: str) -> Optional[User]:
        row = await self.conn.fetchrow(
            "SELECT * FROM re_users WHERE external_id = $1", external_id
        )
        return _map_user(row) if row else None

    async def create_user(self, external_id: str) -> User:
        row = await self.conn.fetchrow(
            """
            INSERT INTO re_users (external_id, has_active_deposit, is_active, created_at)
            VALUES ($1, FALSE, TRUE, NOW())
            ON CONFLICT (external_id) DO UPDATE
                SET is_active = TRUE
            RETURNING *
            """,
            external_id,
        )
        return _map_user(row)

    async def update_user_deposit_status(
        self, user_id: int, has_active_deposit: bool
    ) -> None:
        await self.conn.execute(
            "UPDATE re_users SET has_active_deposit = $1 WHERE id = $2",
            has_active_deposit,
            user_id,
        )

    async def user_has_active_deposit(self, user_id: int) -> bool:
        val = await self.conn.fetchval(
            "SELECT has_active_deposit FROM re_users WHERE id = $1", user_id
        )
        return bool(val)

    # ------------------------------------------------------------------
    # Referral tree
    # ------------------------------------------------------------------

    async def get_parent(self, user_id: int) -> Optional[ReferralLink]:
        row = await self.conn.fetchrow(
            "SELECT * FROM re_referrals WHERE user_id = $1 AND level = 1",
            user_id,
        )
        return _map_referral(row) if row else None

    async def get_chain_up(
        self, user_id: int, max_depth: int
    ) -> List[ReferralLink]:
        rows = await self.conn.fetch(
            """
            SELECT * FROM re_referrals
            WHERE user_id = $1 AND level <= $2
            ORDER BY level ASC
            """,
            user_id,
            max_depth,
        )
        return [_map_referral(r) for r in rows]

    async def get_children(
        self, user_id: int, level: int = 1
    ) -> List[ReferralLink]:
        rows = await self.conn.fetch(
            "SELECT * FROM re_referrals WHERE referrer_id = $1 AND level = $2",
            user_id,
            level,
        )
        return [_map_referral(r) for r in rows]

    async def get_subtree(
        self, user_id: int, max_depth: int
    ) -> List[ReferralLink]:
        rows = await self.conn.fetch(
            """
            SELECT * FROM re_referrals
            WHERE referrer_id = $1 AND level <= $2
            ORDER BY level ASC
            """,
            user_id,
            max_depth,
        )
        return [_map_referral(r) for r in rows]

    async def create_referral_link(
        self, user_id: int, referrer_id: int
    ) -> ReferralLink:
        row = await self.conn.fetchrow(
            """
            INSERT INTO re_referrals (user_id, referrer_id, level, created_at)
            VALUES ($1, $2, 1, NOW())
            ON CONFLICT (user_id, level)
                DO UPDATE SET referrer_id = EXCLUDED.referrer_id,
                              created_at  = EXCLUDED.created_at
            RETURNING *
            """,
            user_id,
            referrer_id,
        )
        return _map_referral(row)

    async def rebuild_tree(
        self, scope_user_id: Optional[int] = None
    ) -> int:
        """
        Recursive CTE walks all direct (level=1) links and materialises
        every ancestor path.  O(N) in tree size, one round-trip.
        """
        result = await self.conn.execute(
            """
            WITH RECURSIVE chain AS (
                -- Seed: direct links only
                SELECT
                    dl.user_id,
                    dl.referrer_id,
                    1 AS computed_level
                FROM re_referrals dl
                WHERE dl.level = 1

                UNION ALL

                -- Climb: follow parent's direct link
                SELECT
                    c.user_id,
                    parent.referrer_id,
                    c.computed_level + 1
                FROM chain c
                JOIN re_referrals parent
                    ON parent.user_id = c.referrer_id
                   AND parent.level   = 1
                WHERE c.computed_level < 50
            )
            INSERT INTO re_referrals (user_id, referrer_id, level, created_at)
            SELECT DISTINCT user_id, referrer_id, computed_level, NOW()
            FROM chain
            WHERE computed_level > 1
            ON CONFLICT (user_id, level)
                DO UPDATE SET referrer_id = EXCLUDED.referrer_id
            """
        )
        # "INSERT 0 N" → N
        try:
            return int(result.split()[-1])
        except (IndexError, ValueError):
            return 0

    # ------------------------------------------------------------------
    # Accruals
    # ------------------------------------------------------------------

    async def accrual_exists(
        self, source_key: str, recipient_user_id: int, level: int
    ) -> bool:
        val = await self.conn.fetchval(
            """
            SELECT 1 FROM re_accruals
            WHERE source_key = $1
              AND recipient_user_id = $2
              AND level = $3
            LIMIT 1
            """,
            source_key,
            recipient_user_id,
            level,
        )
        return val is not None

    async def save_accrual(self, accrual: AccrualRecord) -> AccrualRecord:
        row = await self.conn.fetchrow(
            """
            INSERT INTO re_accruals (
                source_user_id, recipient_user_id, level,
                base_amount, accrual_rate, accrual_amount,
                source_key, source_tag, accrued_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (source_key, recipient_user_id, level) DO NOTHING
            RETURNING id
            """,
            accrual.source_user_id,
            accrual.recipient_user_id,
            accrual.level,
            accrual.base_amount,
            accrual.accrual_rate,
            accrual.accrual_amount,
            accrual.source_key,
            accrual.source_tag,
            accrual.accrued_at,
        )
        if row:
            accrual.id = row["id"]
        return accrual

    async def get_user_accruals(
        self,
        user_id: int,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[AccrualRecord]:
        if since:
            rows = await self.conn.fetch(
                """
                SELECT * FROM re_accruals
                WHERE recipient_user_id = $1 AND accrued_at >= $2
                ORDER BY accrued_at DESC
                LIMIT $3
                """,
                user_id,
                since,
                limit,
            )
        else:
            rows = await self.conn.fetch(
                """
                SELECT * FROM re_accruals
                WHERE recipient_user_id = $1
                ORDER BY accrued_at DESC
                LIMIT $2
                """,
                user_id,
                limit,
            )
        return [_map_accrual(r) for r in rows]

    async def get_user_total_accrued(self, user_id: int) -> float:
        val = await self.conn.fetchval(
            "SELECT COALESCE(SUM(accrual_amount), 0) FROM re_accruals WHERE recipient_user_id = $1",
            user_id,
        )
        return float(val or 0)


# ------------------------------------------------------------------
# Mappers (asyncpg Record → domain model)
# ------------------------------------------------------------------

def _map_user(row: asyncpg.Record) -> User:
    return User(
        id=row["id"],
        external_id=row["external_id"],
        created_at=row["created_at"],
        has_active_deposit=bool(row["has_active_deposit"]),
        is_active=bool(row["is_active"]),
    )


def _map_referral(row: asyncpg.Record) -> ReferralLink:
    return ReferralLink(
        user_id=row["user_id"],
        referrer_id=row["referrer_id"],
        level=row["level"],
        created_at=row["created_at"],
    )


def _map_accrual(row: asyncpg.Record) -> AccrualRecord:
    return AccrualRecord(
        id=row["id"],
        source_user_id=row["source_user_id"],
        recipient_user_id=row["recipient_user_id"],
        level=row["level"],
        base_amount=Decimal(str(row["base_amount"])),
        accrual_rate=float(row["accrual_rate"]),
        accrual_amount=Decimal(str(row["accrual_amount"])),
        source_key=row["source_key"],
        source_tag=row["source_tag"],
        accrued_at=row["accrued_at"],
    )
