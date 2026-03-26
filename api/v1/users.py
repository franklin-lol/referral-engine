from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from api.deps import get_engine
from referral_engine import ReferralEngine, UserNotFoundError
from referral_engine.models import User

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class UserCreate(BaseModel):
    external_id: str
    referrer_id: Optional[int] = None


class UserDepositUpdate(BaseModel):
    has_active_deposit: bool


class UserOut(BaseModel):
    id: int
    external_id: str
    has_active_deposit: bool
    is_active: bool

    @classmethod
    def from_domain(cls, u: User) -> "UserOut":
        return cls(
            id=u.id,
            external_id=u.external_id,
            has_active_deposit=u.has_active_deposit,
            is_active=u.is_active,
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def create_user(
    body: UserCreate,
    engine: ReferralEngine = Depends(get_engine),
) -> UserOut:
    """Register a new user, optionally with a referrer."""
    try:
        user = await engine.register_user(
            external_id=body.external_id,
            referrer_id=body.referrer_id,
        )
    except UserNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Referrer {exc.user_id} not found",
        )
    return UserOut.from_domain(user)


@router.patch("/{user_id}/deposit", status_code=status.HTTP_204_NO_CONTENT)
async def update_deposit_status(
    user_id: int,
    body: UserDepositUpdate,
    engine: ReferralEngine = Depends(get_engine),
) -> None:
    """Toggle the active-deposit flag (controls accrual eligibility)."""
    await engine.update_deposit_status(user_id, body.has_active_deposit)


@router.get("/{user_id}/accruals")
async def get_accruals(
    user_id: int,
    limit: int = 50,
    engine: ReferralEngine = Depends(get_engine),
) -> dict:
    """Return the latest accruals credited to this user."""
    records = await engine.get_user_accruals(user_id, limit=limit)
    total = await engine.get_user_total_accrued(user_id)
    return {
        "user_id": user_id,
        "total_accrued": total,
        "records": [
            {
                "id": r.id,
                "source_user_id": r.source_user_id,
                "level": r.level,
                "rate": r.accrual_rate,
                "base_amount": str(r.base_amount),
                "amount": str(r.accrual_amount),
                "source_tag": r.source_tag,
                "accrued_at": r.accrued_at.isoformat(),
            }
            for r in records
        ],
    }
