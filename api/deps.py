"""FastAPI dependency factories."""
from __future__ import annotations

from typing import AsyncGenerator

from fastapi import Depends

from api import state as app_state
from referral_engine import EngineConfig, PostgresAdapter, ReferralEngine


async def get_adapter() -> AsyncGenerator[PostgresAdapter, None]:
    """
    Yield an open-transaction adapter.
    Commits on clean exit, rolls back on exception.
    """
    adapter = PostgresAdapter(app_state.pool)
    async with adapter:
        yield adapter


def get_engine(
    adapter: PostgresAdapter = Depends(get_adapter),
) -> ReferralEngine:
    return ReferralEngine(app_state.config, adapter)
