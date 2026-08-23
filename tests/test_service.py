"""Network-free tests for acknowledgement, degradation, and lifecycle semantics."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from kairos_core.bus import BusEnvelope, MessageBus
from kairos_core.contracts import (
    DerivativesMetrics,
    MarketSnapshot,
    OrderBookSummary,
    SentimentSignal,
    TechnicalIndicators,
)
from kairos_core.enums import ImpactDirection, RouterMode, Side, SystemMode
from kairos_core.topics import Topics

from kairos_router.config import RouterSettings
from kairos_router.service import RouterService

FROZEN_NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


class _FakeBus(MessageBus):
    def __init__(self, messages: dict[str, list[BusEnvelope]] | None = None) -> None:
        self.messages = messages or {}
        self.operations: list[tuple[str, str, str]] = []
        self.published: list[tuple[str, dict]] = []
        self.fail_publish = False
        self.fail_ack = False
        self.fail_subscribe_topic: str | None = None
        self.closed = False

    async def publish(self, topic, message):
        self.operations.append(("publish", topic, ""))
        if self.fail_publish:
            raise RuntimeError("publish failed")
        payload = self._to_payload(message)
        self.published.append((topic, payload))
        return "published-1"

    async def subscribe(
        self,
        topic: str,
        *,
        group: str | None = None,
        consumer: str | None = None,
    ) -> AsyncIterator[BusEnvelope]:
        if topic == self.fail_subscribe_topic:
            raise RuntimeError("subscription failed")
        for envelope in self.messages.get(topic, []):
            yield envelope

    async def ack(self, topic, envelope, *, group=None):
        self.operations.append(("ack", topic, envelope.id))
        if self.fail_ack:
            raise RuntimeError("ack failed")

    async def close(self):
        self.closed = True


def _settings(**changes) -> RouterSettings:
    return RouterSettings(
        bus_backend="memory",
        trading_symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        conflict_threshold=1,
        calm_threshold=2,
        **changes,
    )


def _snapshot(
    symbol: str,
    bias: Side,
    *,
    message_id: str,
    produced_at: datetime | None = None,
) -> MarketSnapshot:
    return MarketSnapshot(
        message_id=message_id,
        produced_at=produced_at or FROZEN_NOW,
        source="quant-scouts",
        symbol=symbol,
        mid_price=100.0,
        volume_usd=1_000.0,
        order_book=OrderBookSummary(
            best_bid=99.0,
            best_ask=101.0,
            spread_bps=200.0,
            imbalance=0.0,
            depth_usd=5_000.0,
        ),
        derivatives=DerivativesMetrics(funding_rate=0.0, open_interest=10_000.0),
        indicators=TechnicalIndicators(rsi_14=50.0, macd=0.0, macd_signal=0.0, macd_hist=0.0),
        quant_bias=bias,
    )


def _envelope(topic: str, payload: dict, *, envelope_id: str) -> BusEnvelope:
    return BusEnvelope(id=envelope_id, topic=topic, payload=payload)


def _sentiment(
    topic: str,
    side: Side,
    *,
    message_id: str | None = None,
    produced_at: datetime | None = None,
    confidence: float = 0.8,
) -> SentimentSignal:
    score = 0.8 if side is Side.LONG else -0.8
    impact = ImpactDirection.BULLISH if side is Side.LONG else ImpactDirection.BEARISH
    values = {
        "source": "text-scouts",
        "topic": topic,
        "sentiment": score,
        "impact": impact,
        "produced_at": produced_at or FROZEN_NOW,
        "confidence": confidence,
    }
    if message_id is not None:
        values["message_id"] = message_id
    return SentimentSignal.model_validate(values)


def _service(bus: _FakeBus, **settings_changes) -> RouterService:
    return RouterService(
        _settings(**settings_changes),
        bus=bus,
        clock=lambda: FROZEN_NOW,
    )


def _seed_sentiment(service: RouterService, signal: SentimentSignal) -> None:
    service._process_sentiment(
        _envelope(Topics.SENTIMENT_SIGNAL, signal.to_payload(), envelope_id=f"bus:{signal.message_id}")
    )


async def test_snapshot_ack_happens_only_after_publish():
    snapshot = _snapshot("BTCUSDT", Side.LONG, message_id="snapshot-1")
    envelope = _envelope(Topics.MARKET_SNAPSHOT, snapshot.to_payload(), envelope_id="bus-1")
    bus = _FakeBus({Topics.MARKET_SNAPSHOT: [envelope]})
    service = _service(bus)
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="sentiment-publish"))

    await service._consume_snapshots()

    assert [operation[0] for operation in bus.operations] == ["publish", "ack"]
    decision = bus.published[0][1]
    assert decision["message_id"] == "router:snapshot-1"
    assert decision["causation_id"] == "snapshot-1"
    assert decision["produced_at"] == "2026-08-13T12:00:00Z"


async def test_legacy_snapshot_route_is_disabled_outside_dry_run():
    snapshot = _snapshot("BTCUSDT", Side.LONG, message_id="paper-legacy")
    envelope = _envelope(Topics.MARKET_SNAPSHOT, snapshot.to_payload(), envelope_id="paper-bus")
    bus = _FakeBus({Topics.MARKET_SNAPSHOT: [envelope]})
    service = _service(bus, trading_mode="PAPER")
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="paper-text"))

    await service._consume_snapshots()

    assert bus.published == []
    assert bus.operations == [("ack", Topics.MARKET_SNAPSHOT, "paper-bus")]


async def test_failed_publish_is_not_acked_or_committed():
    snapshot = _snapshot("BTCUSDT", Side.LONG, message_id="snapshot-2")
    envelope = _envelope(Topics.MARKET_SNAPSHOT, snapshot.to_payload(), envelope_id="bus-2")
    bus = _FakeBus({Topics.MARKET_SNAPSHOT: [envelope]})
    bus.fail_publish = True
    service = _service(bus)
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="sentiment-failure"))

    await service._consume_snapshots()

    assert [operation[0] for operation in bus.operations] == ["publish"]
    assert "BTCUSDT" not in service.fsm._states


async def test_snapshot_replay_after_failed_ack_does_not_republish_or_advance_fsm():
    snapshot = _snapshot("BTCUSDT", Side.LONG, message_id="snapshot-replay")
    envelope = _envelope(Topics.MARKET_SNAPSHOT, snapshot.to_payload(), envelope_id="bus-replay")
    bus = _FakeBus({Topics.MARKET_SNAPSHOT: [envelope]})
    bus.fail_ack = True
    service = _service(bus)
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="sentiment-replay-base"))

    await service._consume_snapshots()
    first_state = service.fsm.state("BTCUSDT")
    assert first_state.calm_streak == 1

    bus.fail_ack = False
    await service._consume_snapshots()

    assert len(bus.published) == 1
    assert service.fsm.state("BTCUSDT").calm_streak == 1


async def test_sentiment_replay_after_failed_ack_is_not_counted_twice():
    signal = _sentiment("BTCUSDT", Side.LONG)
    envelope = _envelope(Topics.SENTIMENT_SIGNAL, signal.to_payload(), envelope_id="sentiment-replay")
    bus = _FakeBus({Topics.SENTIMENT_SIGNAL: [envelope]})
    bus.fail_ack = True
    service = _service(bus)

    await service._consume_sentiment()
    bus.fail_ack = False
    await service._consume_sentiment()

    assert len(service.window._sentiment["BTCUSDT"]) == 1


def test_reused_sentiment_id_cannot_cross_topic_boundaries():
    service = _service(_FakeBus())
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="reused"))

    with pytest.raises(ValueError, match="reused across topics"):
        _seed_sentiment(service, _sentiment("ETHUSDT", Side.LONG, message_id="reused"))


def test_sentiment_evidence_expires_with_its_bounded_identity_entry():
    service = _service(_FakeBus(), processed_cache_size=1)
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="first"))
    _seed_sentiment(service, _sentiment("ETHUSDT", Side.LONG, message_id="second"))

    assert service.window.aggregate("BTCUSDT", as_of=FROZEN_NOW).sentiment_ids == ()
    assert service.window.aggregate("ETHUSDT", as_of=FROZEN_NOW).sentiment_ids == ("second",)


async def test_reused_snapshot_id_cannot_cross_symbol_boundaries():
    bus = _FakeBus()
    service = _service(bus)
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="btc-text"))
    _seed_sentiment(service, _sentiment("ETHUSDT", Side.LONG, message_id="eth-text"))
    first = _snapshot("BTCUSDT", Side.LONG, message_id="same-snapshot")
    second = _snapshot("ETHUSDT", Side.LONG, message_id="same-snapshot")

    await service._process_snapshot(
        _envelope(Topics.MARKET_SNAPSHOT, first.to_payload(), envelope_id="bus:first")
    )
    with pytest.raises(ValueError, match="reused across symbols"):
        await service._process_snapshot(
            _envelope(Topics.MARKET_SNAPSHOT, second.to_payload(), envelope_id="bus:second")
        )


async def test_invalid_sentiment_and_control_are_not_acked():
    sentiment = _envelope(Topics.SENTIMENT_SIGNAL, {"not": "valid"}, envelope_id="bad-sentiment")
    control = _envelope(Topics.SYSTEM_CONTROL, {"mode": "UNKNOWN"}, envelope_id="bad-control")
    bus = _FakeBus(
        {
            Topics.SENTIMENT_SIGNAL: [sentiment],
            Topics.SYSTEM_CONTROL: [control],
        }
    )
    service = _service(bus)

    await asyncio.gather(service._consume_sentiment(), service._consume_control())

    assert bus.operations == []
    assert service.system_mode is SystemMode.NORMAL


async def test_valid_control_is_applied_before_ack():
    control = _envelope(Topics.SYSTEM_CONTROL, {"mode": "CONFLICT_SAFE"}, envelope_id="control")
    bus = _FakeBus({Topics.SYSTEM_CONTROL: [control]})
    service = _service(bus)

    await service._consume_control()

    assert service.system_mode is SystemMode.CONFLICT_SAFE
    assert bus.operations == [("ack", Topics.SYSTEM_CONTROL, "control")]


def test_configured_usdt_topic_is_symbol_specific():
    service = _service(_FakeBus())
    signal = _sentiment(" btcusdt ", Side.LONG)

    service._process_sentiment(
        _envelope(Topics.SENTIMENT_SIGNAL, signal.to_payload(), envelope_id="sentiment")
    )

    assert service.window.aggregate("BTCUSDT", as_of=FROZEN_NOW).bias is Side.LONG
    assert service.window.aggregate("*", as_of=FROZEN_NOW).bias is Side.FLAT


async def test_snapshot_symbol_is_normalized_in_decision():
    bus = _FakeBus()
    service = _service(bus)
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="normalized-text"))

    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot(" btcusdt ", Side.LONG, message_id="normalized-snapshot").to_payload(),
            envelope_id="bus:normalized",
        )
    )

    assert bus.published[0][1]["symbol"] == "BTCUSDT"


async def test_conflict_safe_blocks_only_gpt_escalation():
    bus = _FakeBus()
    service = _service(bus)
    service._apply_control(
        _envelope(Topics.SYSTEM_CONTROL, {"mode": "CONFLICT_SAFE"}, envelope_id="control-1")
    )
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="btc-conflict"))
    conflicting = _snapshot("BTCUSDT", Side.SHORT, message_id="snapshot-conflict")

    await service._process_snapshot(
        _envelope(Topics.MARKET_SNAPSHOT, conflicting.to_payload(), envelope_id="bus-conflict")
    )

    assert service.fsm.state("BTCUSDT").mode is RouterMode.ROUTE_GPT
    assert bus.published == []

    _seed_sentiment(service, _sentiment("ETHUSDT", Side.LONG, message_id="eth-agreement"))
    calm = _snapshot("ETHUSDT", Side.LONG, message_id="snapshot-calm")
    await service._process_snapshot(
        _envelope(Topics.MARKET_SNAPSHOT, calm.to_payload(), envelope_id="bus-calm")
    )

    assert bus.published[0][1]["mode"] == "ROUTE_PRO"


async def test_local_quant_blocks_analytical_decisions_but_text_mode_does_not():
    bus = _FakeBus()
    service = _service(bus)
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="local-text"))
    _seed_sentiment(service, _sentiment("ETHUSDT", Side.LONG, message_id="normal-text"))
    service.system_mode = SystemMode.LOCAL_QUANT_MODE

    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot("BTCUSDT", Side.LONG, message_id="local").to_payload(),
            envelope_id="bus-local",
        )
    )
    assert bus.published == []

    service.system_mode = SystemMode.TEXT_LOCAL_FILTER
    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot("ETHUSDT", Side.LONG, message_id="text-local").to_payload(),
            envelope_id="bus-text-local",
        )
    )
    assert bus.published[0][1]["mode"] == "ROUTE_PRO"


async def test_missing_or_low_confidence_direction_abstains_without_publishing():
    bus = _FakeBus()
    service = _service(bus)
    _seed_sentiment(
        service,
        _sentiment(
            "BTCUSDT",
            Side.LONG,
            message_id="too-uncertain",
            confidence=0.2,
        ),
    )

    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot("BTCUSDT", Side.LONG, message_id="abstain").to_payload(),
            envelope_id="bus-abstain",
        )
    )

    assert bus.published == []
    state = service.fsm.state("BTCUSDT")
    assert state.conflict_streak == 0
    assert state.calm_streak == 0


async def test_stale_future_and_out_of_order_snapshots_do_not_advance_hysteresis():
    bus = _FakeBus()
    service = _service(bus, snapshot_ttl_s=60.0, event_future_tolerance_s=2.0)
    _seed_sentiment(service, _sentiment("BTCUSDT", Side.LONG, message_id="fresh-text"))

    stale = _snapshot(
        "BTCUSDT",
        Side.LONG,
        message_id="stale",
        produced_at=FROZEN_NOW - timedelta(seconds=61),
    )
    future = _snapshot(
        "BTCUSDT",
        Side.LONG,
        message_id="future",
        produced_at=FROZEN_NOW + timedelta(seconds=3),
    )
    tolerated_at_ingestion_but_noncausal = _snapshot(
        "BTCUSDT",
        Side.LONG,
        message_id="future-within-skew",
        produced_at=FROZEN_NOW + timedelta(seconds=1),
    )
    current = _snapshot("BTCUSDT", Side.LONG, message_id="current")
    older = _snapshot(
        "BTCUSDT",
        Side.LONG,
        message_id="older",
        produced_at=FROZEN_NOW - timedelta(seconds=1),
    )

    for snapshot in (stale, future, tolerated_at_ingestion_but_noncausal, current, older):
        await service._process_snapshot(
            _envelope(
                Topics.MARKET_SNAPSHOT,
                snapshot.to_payload(),
                envelope_id=f"bus:{snapshot.message_id}",
            )
        )

    assert [payload["snapshot_id"] for _, payload in bus.published] == ["current"]
    assert service.fsm.state("BTCUSDT").calm_streak == 1


async def test_decision_provenance_is_exact_stable_and_event_time_bounded():
    bus = _FakeBus()
    service = _service(
        bus,
        sentiment_ttl_s=60.0,
        sentiment_min_confidence=0.25,
        max_sentiment_evidence=2,
        event_future_tolerance_s=2.0,
    )
    signals = [
        _sentiment(
            "BTCUSDT",
            Side.LONG,
            message_id="z-second-at-tie",
            produced_at=FROZEN_NOW - timedelta(seconds=10),
            confidence=1.0,
        ),
        _sentiment(
            "BTCUSDT",
            Side.LONG,
            message_id="stale",
            produced_at=FROZEN_NOW - timedelta(seconds=61),
            confidence=1.0,
        ),
        _sentiment(
            "BTCUSDT",
            Side.SHORT,
            message_id="future",
            produced_at=FROZEN_NOW + timedelta(seconds=3),
            confidence=1.0,
        ),
        _sentiment(
            "BTCUSDT",
            Side.LONG,
            message_id="low-confidence",
            produced_at=FROZEN_NOW - timedelta(seconds=5),
            confidence=0.2,
        ),
        _sentiment(
            "BTCUSDT",
            Side.LONG,
            message_id="a-first-at-tie",
            produced_at=FROZEN_NOW - timedelta(seconds=10),
            confidence=0.5,
        ),
        _sentiment(
            "BTCUSDT",
            Side.LONG,
            message_id="newest",
            produced_at=FROZEN_NOW - timedelta(seconds=4),
            confidence=0.5,
        ),
    ]
    for signal in signals:
        _seed_sentiment(service, signal)

    stored = service.window._sentiment["BTCUSDT"]
    assert "stale" not in stored
    assert "future" not in stored

    snapshot = _snapshot("BTCUSDT", Side.LONG, message_id="with-provenance")
    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            snapshot.to_payload(),
            envelope_id="bus:with-provenance",
        )
    )

    decision = bus.published[0][1]
    assert decision["sentiment_ids"] == ["newest", "a-first-at-tie"]
    assert decision["text_bias"] == "LONG"


async def test_neutral_symbol_evidence_does_not_fall_back_to_broadcast_direction():
    bus = _FakeBus()
    service = _service(bus)
    global_signal = _sentiment("SEC ETF", Side.LONG, message_id="global-long")
    neutral = SentimentSignal(
        message_id="btc-neutral",
        produced_at=FROZEN_NOW,
        source="text-scouts",
        topic="BTCUSDT",
        sentiment=0.0,
        impact=ImpactDirection.NEUTRAL,
        confidence=1.0,
    )
    _seed_sentiment(service, global_signal)
    _seed_sentiment(service, neutral)

    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot("BTCUSDT", Side.LONG, message_id="neutral-override").to_payload(),
            envelope_id="bus:neutral-override",
        )
    )

    assert bus.published == []


async def test_future_sentiment_does_not_leak_into_earlier_snapshot():
    bus = _FakeBus()
    service = _service(bus, event_future_tolerance_s=2.0)
    _seed_sentiment(
        service,
        _sentiment(
            "BTCUSDT",
            Side.LONG,
            message_id="one-second-later",
            produced_at=FROZEN_NOW + timedelta(seconds=1),
        ),
    )

    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot("BTCUSDT", Side.LONG, message_id="before-text").to_payload(),
            envelope_id="bus:before-text",
        )
    )
    assert bus.published == []

    service._clock = lambda: FROZEN_NOW + timedelta(seconds=1)
    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot(
                "BTCUSDT",
                Side.LONG,
                message_id="at-text-time",
                produced_at=FROZEN_NOW + timedelta(seconds=1),
            ).to_payload(),
            envelope_id="bus:at-text-time",
        )
    )

    assert bus.published[0][1]["snapshot_id"] == "at-text-time"
    assert bus.published[0][1]["sentiment_ids"] == ["one-second-later"]


async def test_run_closes_bus_after_all_taskgroup_consumers_finish():
    bus = _FakeBus()
    service = _service(bus)

    await service.run()

    assert bus.closed is True


async def test_run_closes_bus_when_taskgroup_consumer_fails():
    bus = _FakeBus()
    bus.fail_subscribe_topic = Topics.MARKET_SNAPSHOT
    service = _service(bus)

    with pytest.raises(ExceptionGroup):
        await service.run()

    assert bus.closed is True
