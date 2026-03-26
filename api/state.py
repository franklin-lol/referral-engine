"""Shared mutable application state. Populated during lifespan startup."""
from typing import Optional
import asyncpg
from referral_engine import EngineConfig

pool: Optional[asyncpg.Pool] = None
config: Optional[EngineConfig] = None
