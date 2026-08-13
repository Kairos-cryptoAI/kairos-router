"""Deterministic event-time aggregation of quant and text scout signals."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from kairos_core.enums import ImpactDirection, Side

from .conflict import sentiment_to_side


@dataclass(frozen=True, slots=True)
class SentimentPoint:
    """Validated sentiment evidence retained in a bounded replay-safe cache."""

    message_id: str
    produced_at: datetime
    score: float
    confidence: float


@dataclass(frozen=True, slots=True)
class TextAggregate:
    """Confidence-calibrated text result and its exact deterministic provenance."""

    bias: Side = Side.FLAT
    score: float = 0.0
    confidence: float = 0.0
    sentiment_ids: tuple[str, ...] = ()
    has_relevant_evidence: bool = False


@dataclass
class SignalWindow:
    """Maintain deduplicated text evidence and aggregate it at snapshot event time."""

    sentiment_ttl_s: float = 600.0
    deadband: float = 0.25
    min_confidence: float = 0.25
    max_evidence: int = 5
    max_points_per_topic: int = 10_000
    _sentiment: dict[str, dict[str, SentimentPoint]] = field(default_factory=dict)

    def add_sentiment(
        self,
        symbol: str,
        *,
        message_id: str,
        produced_at: datetime,
        sentiment: float,
        impact: ImpactDirection,
        confidence: float,
    ) -> None:
        """Record one signal by ID; impact and confidence calibrate its score."""
        if produced_at.utcoffset() is None:
            raise ValueError("sentiment produced_at must be timezone-aware")
        directional_score = sentiment
        if impact is ImpactDirection.NEUTRAL:
            directional_score = 0.0
        elif impact is ImpactDirection.BULLISH and sentiment <= 0.0:
            directional_score = 0.0
        elif impact is ImpactDirection.BEARISH and sentiment >= 0.0:
            directional_score = 0.0
        point = SentimentPoint(
            message_id=message_id,
            produced_at=produced_at.astimezone(UTC),
            score=directional_score,
            confidence=confidence,
        )
        bucket = self._sentiment.setdefault(symbol, {})
        # At-least-once replay must never mutate evidence already accepted under
        # the same identity, even if a malformed producer changes its payload.
        bucket.setdefault(message_id, point)
        while len(bucket) > self.max_points_per_topic:
            oldest = min(bucket.values(), key=lambda item: (item.produced_at, item.message_id))
            del bucket[oldest.message_id]

    def has_message(self, message_id: str) -> bool:
        """Return whether this exact identity is already retained in any topic."""
        return any(message_id in bucket for bucket in self._sentiment.values())

    def discard(self, symbol: str, message_id: str) -> None:
        """Evict evidence when its matching bounded identity entry expires."""
        bucket = self._sentiment.get(symbol)
        if bucket is None:
            return
        bucket.pop(message_id, None)
        if not bucket:
            self._sentiment.pop(symbol, None)

    def aggregate(self, symbol: str, *, as_of: datetime) -> TextAggregate:
        """Aggregate evidence in the causal event-time window ``[as_of - TTL, as_of]``.

        Ordering is newest-first by ``(-produced_at, message_id)``. Returned IDs are
        exactly the sufficiently confident points used in the score, including
        neutral zero contributions that affect its denominator. Confidence filtering
        precedes the evidence cap, so ineligible newer points cannot displace evidence.
        """
        if as_of.utcoffset() is None:
            raise ValueError("aggregate as_of must be timezone-aware")
        as_of = as_of.astimezone(UTC)
        cutoff = as_of - timedelta(seconds=self.sentiment_ttl_s)

        relevant = sorted(
            (
                point
                for point in self._sentiment.get(symbol, {}).values()
                if cutoff <= point.produced_at <= as_of
            ),
            key=lambda point: (-point.produced_at.timestamp(), point.message_id),
        )
        eligible = [point for point in relevant if point.confidence >= self.min_confidence]
        selected = eligible[: self.max_evidence]
        if not selected:
            return TextAggregate(has_relevant_evidence=bool(relevant))

        confidence = sum(point.confidence for point in selected) / len(selected)
        calibrated_score = sum(point.score * point.confidence for point in selected) / len(selected)
        return TextAggregate(
            bias=sentiment_to_side(calibrated_score, self.deadband),
            score=calibrated_score,
            confidence=confidence,
            sentiment_ids=tuple(point.message_id for point in selected),
            has_relevant_evidence=True,
        )
