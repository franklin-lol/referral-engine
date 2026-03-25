from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import List, Optional


@dataclass
class User:
    id: int
    external_id: str
    created_at: datetime
    has_active_deposit: bool = False
    is_active: bool = True


@dataclass
class ReferralLink:
    user_id: int
    referrer_id: int
    level: int
    created_at: datetime


@dataclass
class TreeNode:
    user_id: int
    level: int
    children: List["TreeNode"] = field(default_factory=list)


@dataclass
class AccrualRecord:
    source_user_id: int
    recipient_user_id: int
    level: int
    base_amount: Decimal
    accrual_rate: float
    accrual_amount: Decimal
    source_key: str
    source_tag: str
    accrued_at: datetime
    id: Optional[int] = None


@dataclass
class DistributionResult:
    source_user_id: int
    base_amount: Decimal
    source_tag: str
    accruals: List[AccrualRecord]
    total_distributed: Decimal
    skipped_levels: int

    @property
    def levels_reached(self) -> int:
        return len(self.accruals)

    def summary(self) -> str:
        lines = [
            f"source_user_id : {self.source_user_id}",
            f"base_amount    : {self.base_amount}",
            f"levels_reached : {self.levels_reached}",
            f"levels_skipped : {self.skipped_levels}",
            f"total_paid_out : {self.total_distributed}",
        ]
        for a in self.accruals:
            lines.append(
                f"  L{a.level}  user_id={a.recipient_user_id:<6} "
                f"rate={a.accrual_rate:>5.1f}%  amount={a.accrual_amount}"
            )
        return "\n".join(lines)