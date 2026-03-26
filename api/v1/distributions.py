from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, field_validator

from api.deps import get_engine
from referral_engine import ReferralEngine

router = APIRouter()


class DistributeRequest(BaseModel):
    source_user_id: int
    base_amount: str          # string to preserve decimal precision
    source_tag: str
    accrual_date: Optional[datetime] = None

    @field_validator("base_amount")
    @classmethod
    def parse_amount(cls, v: str) -> str:
        Decimal(v)            # raises ValueError if not a valid decimal
        return v


@router.post("")
async def distribute(
    body: DistributeRequest,
    engine: ReferralEngine = Depends(get_engine),
) -> dict:
    """
    Distribute profit bonuses upward through the referral tree.

    Idempotent — safe to retry with the same ``source_tag``.
    """
    result = await engine.distribute(
        source_user_id=body.source_user_id,
        base_amount=Decimal(body.base_amount),
        source_tag=body.source_tag,
        accrual_date=body.accrual_date,
    )
    return result.summary()
