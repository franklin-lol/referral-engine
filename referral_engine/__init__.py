from referral_engine.config import EngineConfig
from referral_engine.engine import ReferralEngine
from referral_engine.adapters.memory import MemoryAdapter
from referral_engine.exceptions import (
    AdapterError,
    CycleDetectedError,
    DuplicateReferralError,
    UserNotFoundError,
)

# PostgresAdapter requires asyncpg (optional [db] dependency)
try:
    from referral_engine.adapters.postgres import PostgresAdapter
except ImportError:
    PostgresAdapter = None  # type: ignore[assignment,misc]

__all__ = [
    "ReferralEngine",
    "EngineConfig",
    "PostgresAdapter",
    "MemoryAdapter",
    "CycleDetectedError",
    "UserNotFoundError",
    "DuplicateReferralError",
    "AdapterError",
]