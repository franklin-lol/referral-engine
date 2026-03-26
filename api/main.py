"""
api.main
=========
FastAPI demo application that wraps ReferralEngine.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api import state as app_state
from api.core.config import Settings
from api.v1.router import api_router
from referral_engine import EngineConfig, PostgresAdapter, ReferralEngine

_settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────
    app_state.pool = await asyncpg.create_pool(
        dsn=_settings.database_url,
        min_size=5,
        max_size=20,
        command_timeout=30,
    )
    app_state.config = EngineConfig.from_yaml(_settings.engine_config_path)

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────
    await app_state.pool.close()


app = FastAPI(
    title="Referral Engine API",
    description=(
        "REST interface for the referral-engine library.\n\n"
        "Handles user registration, referral tree management, "
        "and multi-level profit distribution."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api/v1")


@app.get("/health", tags=["meta"])
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/config", tags=["meta"])
async def get_config() -> JSONResponse:
    """Return the current engine configuration (read-only)."""
    return JSONResponse(app_state.config.to_dict())
