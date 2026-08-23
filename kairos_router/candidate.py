"""Pure, deterministic policy for routing one immutable strategy candidate."""

from __future__ import annotations

from dataclasses import dataclass

from kairos_core.contracts import CandidateRouteV1, StrategyIntentV1
from kairos_core.enums import CandidateReviewTier, ReasoningEffort, Side

from .aggregation import TextAggregate

REJECTED_PAPER_STRATEGY_IDS = frozenset(
    {
        "trend_breakout_v1",
        "trend_pullback_reclaim_v1",
        "range_mean_reversion_v1",
        "orderflow_volatility_expansion_v1",
        "regime_veto_retest_reclaim_v1",
    }
)


def classify_candidate(intent_side: Side, text_side: Side) -> CandidateReviewTier:
    """Escalate only when fresh directional text opposes the strategy side."""

    if intent_side is Side.FLAT:
        raise ValueError("strategy candidates must be directional")
    if text_side is not Side.FLAT and text_side is not intent_side:
        return CandidateReviewTier.CONFLICT
    return CandidateReviewTier.NORMAL


@dataclass(frozen=True, slots=True)
class CandidateRouterPolicy:
    """Build a route without changing any field owned by Strategy Engine."""

    source: str
    review_timeout_ms: int = 20_000
    max_evidence_ids: int = 16

    def __post_init__(self) -> None:
        if not self.source or self.source != self.source.strip():
            raise ValueError("source must be a normalized non-empty string")
        if isinstance(self.review_timeout_ms, bool) or self.review_timeout_ms <= 0:
            raise ValueError("review_timeout_ms must be positive")
        if isinstance(self.max_evidence_ids, bool) or not 1 <= self.max_evidence_ids <= 64:
            raise ValueError("max_evidence_ids must be within [1, 64]")

    def build(self, intent: StrategyIntentV1, text: TextAggregate) -> CandidateRouteV1:
        tier = classify_candidate(intent.side, text.bias)
        deadline_ms = min(
            intent.entry_expires_ts_ms,
            intent.decision_ts_ms + self.review_timeout_ms,
        )
        # Strategy/bar provenance already lives inside the immutable intent.
        evidence_ids = self._evidence_ids(text)
        conflict_rationale = None
        if tier is CandidateReviewTier.CONFLICT:
            conflict_rationale = (
                f"strategy_side={intent.side.value} text_side={text.bias.value} "
                f"text_score={text.score:.6f} text_confidence={text.confidence:.6f}"
            )
        return CandidateRouteV1(
            correlation_id=intent.correlation_id or intent.intent_id,
            causation_id=intent.message_id,
            source=self.source,
            intent=intent,
            review_tier=tier,
            requested_reasoning_effort=(
                ReasoningEffort.HIGH if tier is CandidateReviewTier.CONFLICT else ReasoningEffort.MEDIUM
            ),
            routed_at_ms=intent.decision_ts_ms,
            review_deadline_ms=deadline_ms,
            evidence_ids=evidence_ids,
            conflict_rationale=conflict_rationale,
        )

    def _evidence_ids(self, text: TextAggregate) -> tuple[str, ...]:
        if len(text.sentiment_ids) > self.max_evidence_ids:
            raise ValueError("candidate evidence bound cannot truncate classification inputs")
        selected = text.sentiment_ids
        if any(not item or item != item.strip() or len(item) > 128 for item in selected):
            raise ValueError("text evidence IDs must be exact normalized IDs up to 128 characters")
        return selected
