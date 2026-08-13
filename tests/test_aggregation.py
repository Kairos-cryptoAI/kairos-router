"""Frozen, network-free tests for deterministic text aggregation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from kairos_core.enums import ImpactDirection, Side

from kairos_router.aggregation import SignalWindow

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


def _add(
    window: SignalWindow,
    message_id: str,
    *,
    age_s: float,
    sentiment: float,
    confidence: float,
    impact: ImpactDirection,
) -> None:
    window.add_sentiment(
        "BTCUSDT",
        message_id=message_id,
        produced_at=NOW - timedelta(seconds=age_s),
        sentiment=sentiment,
        impact=impact,
        confidence=confidence,
    )


def test_aggregation_is_confidence_calibrated_and_provenance_is_stable():
    window = SignalWindow(
        sentiment_ttl_s=60.0,
        deadband=0.25,
        min_confidence=0.25,
    )
    # Deliberately add in a different order from the deterministic event-time order.
    _add(
        window,
        "z-at-tie",
        age_s=10,
        sentiment=0.8,
        confidence=1.0,
        impact=ImpactDirection.BULLISH,
    )
    _add(
        window,
        "stale",
        age_s=61,
        sentiment=1.0,
        confidence=1.0,
        impact=ImpactDirection.BULLISH,
    )
    _add(
        window,
        "future",
        age_s=-3,
        sentiment=-1.0,
        confidence=1.0,
        impact=ImpactDirection.BEARISH,
    )
    _add(
        window,
        "neutral",
        age_s=5,
        sentiment=0.9,
        confidence=1.0,
        impact=ImpactDirection.NEUTRAL,
    )
    _add(
        window,
        "too-uncertain",
        age_s=5,
        sentiment=1.0,
        confidence=0.2,
        impact=ImpactDirection.BULLISH,
    )
    _add(
        window,
        "a-at-tie",
        age_s=10,
        sentiment=0.4,
        confidence=0.5,
        impact=ImpactDirection.BULLISH,
    )
    _add(
        window,
        "newest",
        age_s=5,
        sentiment=0.6,
        confidence=0.5,
        impact=ImpactDirection.BULLISH,
    )

    result = window.aggregate("BTCUSDT", as_of=NOW)

    # Calibrated score = mean(sentiment * confidence), not a normalized weighted
    # mean that would incorrectly turn one low-confidence item into full conviction.
    assert result.score == pytest.approx(1.3 / 4)
    assert result.confidence == pytest.approx(3.0 / 4)
    assert result.bias is Side.LONG
    assert result.sentiment_ids == ("neutral", "newest", "a-at-tie", "z-at-tie")


def test_low_confidence_attenuates_direction_to_abstention_but_retains_provenance():
    window = SignalWindow(deadband=0.25, min_confidence=0.25)
    _add(
        window,
        "weak",
        age_s=0,
        sentiment=0.8,
        confidence=0.3,
        impact=ImpactDirection.BULLISH,
    )

    result = window.aggregate("BTCUSDT", as_of=NOW)

    assert result.score == pytest.approx(0.24)
    assert result.bias is Side.FLAT
    assert result.sentiment_ids == ("weak",)


def test_duplicate_message_id_is_first_write_wins():
    window = SignalWindow()
    _add(
        window,
        "same-id",
        age_s=0,
        sentiment=0.8,
        confidence=1.0,
        impact=ImpactDirection.BULLISH,
    )
    _add(
        window,
        "same-id",
        age_s=0,
        sentiment=-0.8,
        confidence=1.0,
        impact=ImpactDirection.BEARISH,
    )

    result = window.aggregate("BTCUSDT", as_of=NOW)

    assert result.bias is Side.LONG
    assert result.sentiment_ids == ("same-id",)


def test_contradictory_impact_and_score_abstain():
    window = SignalWindow()
    _add(
        window,
        "contradiction",
        age_s=0,
        sentiment=-0.9,
        confidence=1.0,
        impact=ImpactDirection.BULLISH,
    )

    result = window.aggregate("BTCUSDT", as_of=NOW)

    assert result.has_relevant_evidence is True
    assert result.bias is Side.FLAT
    assert result.sentiment_ids == ("contradiction",)


def test_zero_confidence_is_safe_when_minimum_is_disabled():
    window = SignalWindow(min_confidence=0.0)
    _add(
        window,
        "zero-confidence",
        age_s=0,
        sentiment=1.0,
        confidence=0.0,
        impact=ImpactDirection.BULLISH,
    )

    result = window.aggregate("BTCUSDT", as_of=NOW)

    assert result.score == 0.0
    assert result.bias is Side.FLAT
    assert result.sentiment_ids == ("zero-confidence",)


def test_ttl_boundary_is_inclusive_but_future_evidence_is_causal():
    window = SignalWindow(sentiment_ttl_s=60.0)
    _add(
        window,
        "at-ttl",
        age_s=60,
        sentiment=0.8,
        confidence=1.0,
        impact=ImpactDirection.BULLISH,
    )
    _add(
        window,
        "at-future-limit",
        age_s=-2,
        sentiment=0.8,
        confidence=1.0,
        impact=ImpactDirection.BULLISH,
    )

    result = window.aggregate("BTCUSDT", as_of=NOW)

    assert result.sentiment_ids == ("at-ttl",)

    later = window.aggregate("BTCUSDT", as_of=NOW + timedelta(seconds=2))

    assert later.sentiment_ids == ("at-future-limit",)


def test_evidence_cap_is_applied_after_confidence_filtering():
    window = SignalWindow(min_confidence=0.5, max_evidence=2)
    _add(
        window,
        "newest-but-ineligible",
        age_s=0,
        sentiment=-1.0,
        confidence=0.49,
        impact=ImpactDirection.BEARISH,
    )
    _add(
        window,
        "newest-eligible",
        age_s=1,
        sentiment=0.8,
        confidence=0.5,
        impact=ImpactDirection.BULLISH,
    )
    _add(
        window,
        "second-eligible",
        age_s=2,
        sentiment=0.8,
        confidence=0.5,
        impact=ImpactDirection.BULLISH,
    )
    _add(
        window,
        "outside-cap",
        age_s=3,
        sentiment=-1.0,
        confidence=1.0,
        impact=ImpactDirection.BEARISH,
    )

    result = window.aggregate("BTCUSDT", as_of=NOW)

    assert result.sentiment_ids == ("newest-eligible", "second-eligible")
    assert result.score == pytest.approx(0.4)
    assert result.bias is Side.LONG
