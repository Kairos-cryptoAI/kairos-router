"""Router configuration (env prefix ``KAIROS_``)."""

from __future__ import annotations

from kairos_core.config import CoreSettings
from kairos_core.enums import TradingMode
from pydantic import Field, field_validator, model_validator


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
    max_sentiment_evidence: int = Field(5, ge=1, le=64)
    # Tolerate only small producer clock skew; farther future events are rejected.
    event_future_tolerance_s: float = Field(5.0, ge=0.0)
    # Reject stale snapshots and ignore out-of-order snapshots per symbol.
    snapshot_ttl_s: float = Field(120.0, gt=0.0)
    # Bounds in-memory replay suppression; Redis remains the durable source of truth.
    processed_cache_size: int = Field(10_000, ge=1)

    # Candidate-specific Strategy Parity -> PAPER route.  PAPER defaults to an
    # empty exact revision allowlist (REJECT_ALL) until alpha promotion.
    trading_mode: TradingMode = TradingMode.DRY_RUN
    paper_strategy_allowlist: list[str] = Field(default_factory=list)
    candidate_ttl_s: float = Field(120.0, gt=0.0)
    candidate_review_timeout_ms: int = Field(20_000, ge=1, le=60_000)
    max_candidate_evidence_ids: int = Field(16, ge=1, le=64)

    @field_validator("paper_strategy_allowlist")
    @classmethod
    def validate_paper_strategy_allowlist(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for item in value:
            if not isinstance(item, str) or item != item.strip() or item.count("@") != 1:
                raise ValueError(
                    "paper strategy allowlist entries must be normalized '<strategy_id>@<revision>'"
                )
            strategy_id, revision = item.split("@", maxsplit=1)
            if not strategy_id or not revision:
                raise ValueError("paper strategy allowlist entries require strategy_id and revision")
            normalized.append(item)
        if len(normalized) != len(set(normalized)):
            raise ValueError("paper strategy allowlist entries must be unique")
        return sorted(normalized)

    @model_validator(mode="after")
    def validate_candidate_evidence_bound(self) -> RouterSettings:
        if self.max_candidate_evidence_ids < self.max_sentiment_evidence:
            raise ValueError(
                "max_candidate_evidence_ids must cover every sentiment item used in classification"
            )
        return self

    def paper_strategy_allowed(self, strategy_id: str, strategy_revision: str) -> bool:
        return f"{strategy_id}@{strategy_revision}" in self.paper_strategy_allowlist
