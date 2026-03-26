from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from api.deps import get_engine
from referral_engine import CycleDetectedError, ReferralEngine, UserNotFoundError

router = APIRouter()


class SetReferrerBody(BaseModel):
    user_id: int
    referrer_id: int


@router.post("", status_code=status.HTTP_200_OK)
async def set_referrer(
    body: SetReferrerBody,
    engine: ReferralEngine = Depends(get_engine),
) -> dict:
    """Assign or change the referrer of an existing user."""
    try:
        link = await engine.set_referrer(body.user_id, body.referrer_id)
    except UserNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User {exc.user_id} not found",
        )
    except CycleDetectedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )
    return {
        "user_id": link.user_id,
        "referrer_id": link.referrer_id,
        "level": link.level,
    }


@router.get("/{user_id}/tree/up")
async def tree_up(
    user_id: int,
    max_levels: Optional[int] = None,
    engine: ReferralEngine = Depends(get_engine),
) -> dict:
    """Return the ancestor chain (closest first)."""
    nodes = await engine.get_tree_up(user_id, max_levels=max_levels)
    return {
        "user_id": user_id,
        "ancestors": [
            {
                "user_id": n.user_id,
                "external_id": n.external_id,
                "level": n.level,
            }
            for n in nodes
        ],
    }


@router.get("/{user_id}/tree/down")
async def tree_down(
    user_id: int,
    max_levels: Optional[int] = None,
    engine: ReferralEngine = Depends(get_engine),
) -> dict:
    """Return the descendant subtree (nested)."""

    def _serialize(node) -> dict:
        return {
            "user_id": node.user_id,
            "external_id": node.external_id,
            "level": node.level,
            "children": [_serialize(c) for c in node.children],
        }

    root = await engine.get_tree_down(user_id, max_levels=max_levels)
    return _serialize(root)


@router.post("/rebuild", status_code=status.HTTP_200_OK)
async def rebuild_tree(
    scope_user_id: Optional[int] = None,
    engine: ReferralEngine = Depends(get_engine),
) -> dict:
    """
    Rebuild multi-level referral links.
    Call after bulk imports or when referrer assignments change.
    """
    updated = await engine.rebuild_tree(scope_user_id)
    return {"records_updated": updated}
