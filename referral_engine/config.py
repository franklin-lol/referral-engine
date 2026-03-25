from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import List, Optional

DEFAULT_RATES = [
    12.0, 10.0, 8.0, 6.0, 5.0,
    4.0,  3.0,  3.0, 2.0, 2.0,
    1.0,  1.0,  1.0, 1.0, 1.0,
]


@dataclass
class LevelConfig:
    level: int
    rate: float

    def __post_init__(self) -> None:
        if self.level < 1:
            raise ValueError(f"level must be >= 1, got {self.level}")
        if not (0.0 <= self.rate <= 100.0):
            raise ValueError(f"rate must be between 0 and 100, got {self.rate}")


@dataclass
class LeaderThreshold:
    level: int
    volume: float
    bonus: float


@dataclass
class EngineConfig:
    levels: List[LevelConfig] = field(
        default_factory=lambda: [
            LevelConfig(level=i, rate=r)
            for i, r in enumerate(DEFAULT_RATES, 1)
        ]
    )
    max_depth: int = 50
    require_active_deposit: bool = True
    income_cap_per_event: Optional[float] = None
    min_accrual_amount: float = 0.000001
    idempotency_window_seconds: int = 86400
    leader_thresholds: List[LeaderThreshold] = field(default_factory=list)

    @property
    def max_levels(self) -> int:
        return len(self.levels)

    def get_rate(self, level_idx: int) -> float:
        """1-based index. Returns 0.0 if out of range."""
        if level_idx < 1 or level_idx > len(self.levels):
            return 0.0
        return self.levels[level_idx - 1].rate

    def total_payout_rate(self) -> float:
        return sum(lc.rate for lc in self.levels)

    @classmethod
    def from_dict(cls, d: dict) -> "EngineConfig":
        rates = d.get("rates", DEFAULT_RATES)
        levels = [LevelConfig(level=i, rate=r) for i, r in enumerate(rates, 1)]
        thresholds = [
            LeaderThreshold(**t) for t in d.get("leader_thresholds", [])
        ]
        return cls(
            levels=levels,
            max_depth=d.get("max_depth", 50),
            require_active_deposit=d.get("require_active_deposit", True),
            income_cap_per_event=d.get("income_cap_per_event"),
            min_accrual_amount=d.get("min_accrual_amount", 0.000001),
            idempotency_window_seconds=d.get("idempotency_window_seconds", 86400),
            leader_thresholds=thresholds,
        )

    @classmethod
    def from_yaml(cls, path: str) -> "EngineConfig":
        import yaml
        with open(path) as f:
            return cls.from_dict(yaml.safe_load(f))

    @classmethod
    def from_json(cls, path: str) -> "EngineConfig":
        with open(path) as f:
            return cls.from_dict(json.load(f))