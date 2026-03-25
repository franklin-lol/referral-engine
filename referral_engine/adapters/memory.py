"""
referral_engine.adapters.memory
================================
Thread-safe in-memory adapter for unit tests and local prototyping.
No database required.  State lives for the lifetime of the object.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

from referral_engine.adapters.base import BaseAdapter
from referral_engine.models import AccrualRecord, ReferralLink, User


class MemoryAdapter(BaseAdapter):
    """
    Pure-Python in-memory adapter.

    Usage::

        adapter = MemoryAdapter()
        engine  = ReferralEngine(config, adapter)

        async with adapter:
            user = await engine.register_user("alice")
    """

    def __init__(self) -> None:
        self._users: Dict[int, User] = {}
        self._ext_index: Dict[str, int] = {}        # external_id → id
        self._referrals: Dict[int, List[ReferralLink]] = defaultdict(list)
        self._accruals: List[AccrualRecord] = []
        self._id_seq: int = 0
        self._accrual_seq: int = 0
        self._in_tx: bool = False

    # ------------------------------------------------------------------
    # Transaction control (no-op — memory is always consistent)
    # ------------------------------------------------------------------

    async def begin(self) -> None:
        self._in_tx = True

    async def commit(self) -> None:
        self._in_tx = False

    async def rollback(self) -> None:
        self._in_tx = False

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        self._id_seq += 1
        return self._id_seq

    async def get_user(self, user_id: int) -> Optional[User]:
        return self._users.get(user_id)

    async def get_user_by_external_id(self, external_id: str) -> Optional[User]:
        uid = self._ext_index.get(external_id)
        return self._users.get(uid) if uid else None

    async def create_user(self, external_id: str) -> User:
        existing_id = self._ext_index.get(external_id)
        if existing_id:
            user = self._users[existing_id]
            user.is_active = True
            return user
        uid = self._next_id()
        user = User(
            id=uid,
            external_id=external_id,
            created_at=datetime.utcnow(),
            has_active_deposit=False,
            is_active=True,
        )
        self._users[uid] = user
        self._ext_index[external_id] = uid
        return user

    async def update_user_deposit_status(
        self, user_id: int, has_active_deposit: bool
    ) -> None:
        if user_id in self._users:
            self._users[user_id].has_active_deposit = has_active_deposit

    async def user_has_active_deposit(self, user_id: int) -> bool:
        user = self._users.get(user_id)
        return bool(user.has_active_deposit) if user else False

    # ------------------------------------------------------------------
    # Referral tree
    # ------------------------------------------------------------------

    async def get_parent(self, user_id: int) -> Optional[ReferralLink]:
        for link in self._referrals.get(user_id, []):
            if link.level == 1:
                return link
        return None

    async def get_chain_up(
        self, user_id: int, max_depth: int
    ) -> List[ReferralLink]:
        return [
            link
            for link in self._referrals.get(user_id, [])
            if link.level <= max_depth
        ]

    async def get_children(
        self, user_id: int, level: int = 1
    ) -> List[ReferralLink]:
        result = []
        for links in self._referrals.values():
            for link in links:
                if link.referrer_id == user_id and link.level == level:
                    result.append(link)
        return result

    async def get_subtree(
        self, user_id: int, max_depth: int
    ) -> List[ReferralLink]:
        result = []
        for links in self._referrals.values():
            for link in links:
                if link.referrer_id == user_id and link.level <= max_depth:
                    result.append(link)
        return result

    async def create_referral_link(
        self, user_id: int, referrer_id: int
    ) -> ReferralLink:
        # Remove existing level-1 link for this user
        self._referrals[user_id] = [
            lnk for lnk in self._referrals[user_id] if lnk.level != 1
        ]
        link = ReferralLink(
            user_id=user_id,
            referrer_id=referrer_id,
            level=1,
            created_at=datetime.utcnow(),
        )
        self._referrals[user_id].append(link)
        return link

    async def rebuild_tree(
        self, scope_user_id: Optional[int] = None
    ) -> int:
        """
        BFS expansion of all multi-level links from existing level-1 links.
        Matches the recursive CTE logic of the PostgreSQL adapter.
        """
        count = 0

        # Determine which users to rebuild
        if scope_user_id is not None:
            # Rebuild only descendants of scope_user_id and the node itself
            targets = {scope_user_id}
            queue = [scope_user_id]
            while queue:
                current = queue.pop()
                children = await self.get_children(current, level=1)
                for child in children:
                    if child.user_id not in targets:
                        targets.add(child.user_id)
                        queue.append(child.user_id)
            user_ids_to_rebuild = targets
        else:
            user_ids_to_rebuild = set(self._users.keys())

        for uid in user_ids_to_rebuild:
            # Remove all non-direct links
            self._referrals[uid] = [
                lnk for lnk in self._referrals[uid] if lnk.level == 1
            ]

            # Walk upward starting from the level-1 parent.
            # Level-1 link already exists; we only need levels 2, 3, ...
            direct_parent = await self.get_parent(uid)
            if direct_parent is None:
                continue

            # Start climbing from level-1 parent to build level-2, 3, ...
            current = direct_parent.referrer_id
            visited = {uid, current}
            level = 1

            while level < 50:
                grandparent = await self.get_parent(current)
                if grandparent is None:
                    break
                ancestor_id = grandparent.referrer_id
                if ancestor_id in visited:
                    break
                visited.add(ancestor_id)
                level += 1
                link = ReferralLink(
                    user_id=uid,
                    referrer_id=ancestor_id,
                    level=level,
                    created_at=datetime.utcnow(),
                )
                self._referrals[uid].append(link)
                count += 1
                current = ancestor_id

        return count

    # ------------------------------------------------------------------
    # Accruals
    # ------------------------------------------------------------------

    async def accrual_exists(
        self, source_key: str, recipient_user_id: int, level: int
    ) -> bool:
        return any(
            a.source_key == source_key
            and a.recipient_user_id == recipient_user_id
            and a.level == level
            for a in self._accruals
        )

    async def save_accrual(self, accrual: AccrualRecord) -> AccrualRecord:
        # Idempotent
        if await self.accrual_exists(
            accrual.source_key, accrual.recipient_user_id, accrual.level
        ):
            return accrual
        self._accrual_seq += 1
        accrual.id = self._accrual_seq
        self._accruals.append(accrual)
        return accrual

    async def get_user_accruals(
        self,
        user_id: int,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[AccrualRecord]:
        records = [
            a for a in self._accruals if a.recipient_user_id == user_id
        ]
        if since:
            records = [a for a in records if a.accrued_at >= since]
        return sorted(records, key=lambda a: a.accrued_at, reverse=True)[:limit]

    async def get_user_total_accrued(self, user_id: int) -> float:
        return float(
            sum(
                a.accrual_amount
                for a in self._accruals
                if a.recipient_user_id == user_id
            )
        )
