"""Router configuration (env prefix ``KAIROS_``)."""

from __future__ import annotations

from kairos_core.config import CoreSettings
from pydantic import Field


class RouterSettings(CoreSettings):
    service_name: str = "kairos-router"

    # Hysteresis thresholds (see spec, Layer 2).
    conflict_threshold: int = 4  # consecutive conflict ticks before ROUTE_GPT
    calm_threshold: int = 10  # consecutive calm ticks before falling back to ROUTE_PRO

    # A sentiment is considered directional only beyond this magnitude.
    sentiment_deadband: float = 0.25
    # How long (seconds) a sentiment signal stays relevant for a symbol.
    sentiment_ttl_s: float = 600.0
    # Bounds in-memory replay suppression; Redis remains the durable source of truth.
    processed_cache_size: int = Field(10_000, ge=1)
