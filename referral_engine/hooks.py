"""
referral_engine.hooks
======================
Post-distribution hook system.

The engine is intentionally decoupled from side effects (notifications,
analytics events, audit writes).  Register async callables here instead
of patching the engine.

Usage::

    from referral_engine.hooks import HookRegistry, HookEvent

    registry = HookRegistry()

    @registry.on("accrual_created")
    async def notify_telegram(event: HookEvent) -> None:
        await bot.send_message(
            event.accrual.recipient_user_id,
            f"You earned {event.accrual.accrual_amount} USDT (level {event.accrual.level})"
        )

    @registry.on("distribution_complete")
    async def push_to_analytics(event: HookEvent) -> None:
        await analytics.track("distribution", event.result.summary())

    # Wire into engine by wrapping distribute():
    engine_with_hooks = HookedEngine(engine, registry)

    async with adapter:
        result = await engine_with_hooks.distribute(
            source_user_id=user_id,
            base_amount=Decimal("150.00"),
            source_tag="dep_7_profit",
        )
    # Hooks fire AFTER the transaction commits — no DB coupling.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Awaitable, Callable, Dict, List, Optional

from referral_engine.engine import ReferralEngine
from referral_engine.models import AccrualRecord, DistributionResult

logger = logging.getLogger(__name__)

# All supported hook event types
HookEventType = str   # "accrual_created" | "distribution_complete" | "distribution_empty"

HookHandler = Callable[["HookEvent"], Awaitable[None]]


@dataclass
class HookEvent:
    event_type: HookEventType
    result: Optional[DistributionResult] = None
    accrual: Optional[AccrualRecord] = None
    source_user_id: Optional[int] = None
    base_amount: Optional[Decimal] = None
    source_tag: Optional[str] = None
    fired_at: datetime = field(default_factory=datetime.utcnow)


class HookRegistry:
    """
    Lightweight async hook registry.

    Multiple handlers per event type are supported.
    Handlers are called sequentially (not concurrently) to preserve ordering.
    Exceptions in handlers are logged and swallowed — they never affect
    the distribution result.
    """

    def __init__(self) -> None:
        self._handlers: Dict[HookEventType, List[HookHandler]] = {}

    def on(self, event_type: HookEventType) -> Callable[[HookHandler], HookHandler]:
        """Decorator to register a handler for *event_type*."""
        def decorator(fn: HookHandler) -> HookHandler:
            self._handlers.setdefault(event_type, []).append(fn)
            return fn
        return decorator

    def register(self, event_type: HookEventType, handler: HookHandler) -> None:
        """Register *handler* programmatically (non-decorator form)."""
        self._handlers.setdefault(event_type, []).append(handler)

    async def fire(self, event: HookEvent) -> None:
        """Fire all handlers registered for ``event.event_type``."""
        handlers = self._handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                await handler(event)
            except Exception as exc:
                logger.error(
                    "Hook handler %s raised for event '%s': %s",
                    handler.__name__,
                    event.event_type,
                    exc,
                )


class HookedEngine:
    """
    Thin wrapper around ``ReferralEngine`` that fires hooks
    **after** each distribution call.

    Hooks fire outside the adapter transaction (after commit).
    This means hook failures never roll back the distribution.

    All other engine methods are proxied transparently.
    """

    def __init__(self, engine: ReferralEngine, registry: HookRegistry) -> None:
        self._engine = engine
        self._registry = registry

    def __getattr__(self, name: str):
        return getattr(self._engine, name)

    async def distribute(
        self,
        source_user_id: int,
        base_amount: Decimal,
        source_tag: str,
        accrual_date: Optional[datetime] = None,
    ) -> DistributionResult:
        result = await self._engine.distribute(
            source_user_id=source_user_id,
            base_amount=base_amount,
            source_tag=source_tag,
            accrual_date=accrual_date,
        )

        # Fire per-accrual hooks
        for accrual in result.accruals:
            await self._registry.fire(
                HookEvent(
                    event_type="accrual_created",
                    result=result,
                    accrual=accrual,
                    source_user_id=source_user_id,
                    base_amount=base_amount,
                    source_tag=source_tag,
                )
            )

        # Fire distribution-level hooks
        event_type = (
            "distribution_complete" if result.levels_reached > 0
            else "distribution_empty"
        )
        await self._registry.fire(
            HookEvent(
                event_type=event_type,
                result=result,
                source_user_id=source_user_id,
                base_amount=base_amount,
                source_tag=source_tag,
            )
        )

        return result
