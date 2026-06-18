"""Router configuration (env prefix ``KAIROS_``)."""
from __future__ import annotations

from kairos_core.config import CoreSettings


class RouterSettings(CoreSettings):
    service_name: str = "kairos-router"

    # Hysteresis thresholds (see spec, Layer 2).
    conflict_threshold: int = 4    # consecutive conflict ticks before USE_HIGH
    calm_threshold: int = 10       # consecutive calm ticks before falling back to USE_MEDIUM

    # A sentiment is considered directional only beyond this magnitude.
    sentiment_deadband: float = 0.25
    # How long (seconds) a sentiment signal stays relevant for a symbol.
    sentiment_ttl_s: float = 600.0
