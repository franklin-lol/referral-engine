"""
referral_engine.adapters.base
==============================
Abstract interface every storage adapter must implement.

Implement this to plug ReferralEngine into PostgreSQL, MySQL,
SQLite, MongoDB, or any in-memory store.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from referral_engine.models import AccrualRecord, ReferralLink, User


class BaseAdapter(ABC):
    """
    Contract between the engine and the storage layer.

    Transaction semantics
    ---------------------
    The adapter **must** support explicit transaction control via
    ``begin`` / ``commit`` / ``rollback``.  Use as an async context
    manager so the engine can batch writes atomically::

        async with adapter:
            await engine.distribute(...)
    """

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------

    @abstractmethod
    async def get_user(self, user_id: int) -> Optional[User]:
        ...

    @abstractmethod
    async def get_user_by_external_id(self, external_id: str) -> Optional[User]:
        ...

    @abstractmethod
    async def create_user(self, external_id: str) -> User:
        ...

    @abstractmethod
    async def update_user_deposit_status(
        self, user_id: int, has_active_deposit: bool
    ) -> None:
        ...

    @abstractmethod
    async def user_has_active_deposit(self, user_id: int) -> bool:
        ...

    # ------------------------------------------------------------------
    # Referral tree
    # ------------------------------------------------------------------

    @abstractmethod
    async def get_parent(self, user_id: int) -> Optional[ReferralLink]:
        """Return the direct (level=1) referrer of *user_id*."""
        ...

    @abstractmethod
    async def get_chain_up(
        self, user_id: int, max_depth: int
    ) -> List[ReferralLink]:
        """
        Return all ancestor links up to *max_depth*, ordered by level ASC
        (level 1 = direct parent first).
        """
        ...

    @abstractmethod
    async def get_children(
        self, user_id: int, level: int = 1
    ) -> List[ReferralLink]:
        """Return direct referrals (level=1 by default) of *user_id*."""
        ...

    @abstractmethod
    async def get_subtree(
        self, user_id: int, max_depth: int
    ) -> List[ReferralLink]:
        """Return all descendant links down to *max_depth*."""
        ...

    @abstractmethod
    async def create_referral_link(
        self, user_id: int, referrer_id: int
    ) -> ReferralLink:
        """
        Persist a direct (level=1) link. Upsert semantics: if a link
        already exists for *user_id* at level 1, overwrite it.
        Multi-level links are populated by ``rebuild_tree``.
        """
        ...

    @abstractmethod
    async def rebuild_tree(
        self, scope_user_id: Optional[int] = None
    ) -> int:
        """
        Rebuild multi-level referral links from direct (level=1) links.

        Parameters
        ----------
        scope_user_id:
            When provided, only rebuild descendants of this user.
            *None* rebuilds the entire tree.

        Returns
        -------
        int
            Number of records inserted/updated.
        """
        ...

    # ------------------------------------------------------------------
    # Accruals
    # ------------------------------------------------------------------

    @abstractmethod
    async def accrual_exists(
        self, source_key: str, recipient_user_id: int, level: int
    ) -> bool:
        """
        Idempotency check — return *True* if this exact accrual was
        already issued.
        """
        ...

    @abstractmethod
    async def save_accrual(self, accrual: AccrualRecord) -> AccrualRecord:
        """Persist *accrual* and return it with ``id`` populated."""
        ...

    @abstractmethod
    async def get_user_accruals(
        self,
        user_id: int,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[AccrualRecord]:
        ...

    @abstractmethod
    async def get_user_total_accrued(self, user_id: int) -> float:
        """Return sum of all accruals credited to *user_id*."""
        ...

    # ------------------------------------------------------------------
    # Transaction control
    # ------------------------------------------------------------------

    @abstractmethod
    async def begin(self) -> None:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    async def rollback(self) -> None:
        ...

    async def __aenter__(self) -> "BaseAdapter":
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            await self.rollback()
        else:
            await self.commit()
