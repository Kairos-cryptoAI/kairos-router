"""Router configuration (env prefix ``KAIROS_``)."""

from __future__ import annotations

from kairos_core.config import CoreSettings
from pydantic import Field


class RouterSettings(CoreSettings):
    service_name: str = "kairos-router"

    # Hysteresis thresholds (see spec, Layer 2).
    conflict_threshold: int = Field(4, ge=1)  # consecutive conflict ticks before ROUTE_GPT
    calm_threshold: int = Field(10, ge=1)  # consecutive agreements before falling back to ROUTE_PRO

    # A sentiment is considered directional only beyond this magnitude.
    sentiment_deadband: float = Field(0.25, ge=0.0, le=1.0)
    # How long (seconds) a sentiment signal stays relevant for a symbol.
    sentiment_ttl_s: float = Field(600.0, gt=0.0)
    # Low-confidence text abstains rather than creating directional evidence.
    sentiment_min_confidence: float = Field(0.25, ge=0.0, le=1.0)
    # Match the compact Aggregator context and provenance bound.
    max_sentiment_evidence: int = Field(5, ge=1)
    # Tolerate only small producer clock skew; farther future events are rejected.
    event_future_tolerance_s: float = Field(5.0, ge=0.0)
    # Reject stale snapshots and ignore out-of-order snapshots per symbol.
    snapshot_ttl_s: float = Field(120.0, gt=0.0)
    # Bounds in-memory replay suppression; Redis remains the durable source of truth.
    processed_cache_size: int = Field(10_000, ge=1)
