"""Network-free tests for acknowledgement, degradation, and lifecycle semantics."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

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


def _snapshot(symbol: str, bias: Side, *, message_id: str) -> MarketSnapshot:
    return MarketSnapshot(
        message_id=message_id,
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


def _sentiment(topic: str, side: Side) -> SentimentSignal:
    score = 0.8 if side is Side.LONG else -0.8
    impact = ImpactDirection.BULLISH if side is Side.LONG else ImpactDirection.BEARISH
    return SentimentSignal(source="text-scouts", topic=topic, sentiment=score, impact=impact)


async def test_snapshot_ack_happens_only_after_publish():
    snapshot = _snapshot("BTCUSDT", Side.LONG, message_id="snapshot-1")
    envelope = _envelope(Topics.MARKET_SNAPSHOT, snapshot.to_payload(), envelope_id="bus-1")
    bus = _FakeBus({Topics.MARKET_SNAPSHOT: [envelope]})
    service = RouterService(_settings(), bus=bus)

    await service._consume_snapshots()

    assert [operation[0] for operation in bus.operations] == ["publish", "ack"]
    decision = bus.published[0][1]
    assert decision["message_id"] == "router:snapshot-1"
    assert decision["causation_id"] == "snapshot-1"


async def test_failed_publish_is_not_acked_or_committed():
    snapshot = _snapshot("BTCUSDT", Side.LONG, message_id="snapshot-2")
    envelope = _envelope(Topics.MARKET_SNAPSHOT, snapshot.to_payload(), envelope_id="bus-2")
    bus = _FakeBus({Topics.MARKET_SNAPSHOT: [envelope]})
    bus.fail_publish = True
    service = RouterService(_settings(), bus=bus)

    await service._consume_snapshots()

    assert [operation[0] for operation in bus.operations] == ["publish"]
    assert "BTCUSDT" not in service.fsm._states


async def test_snapshot_replay_after_failed_ack_does_not_republish_or_advance_fsm():
    snapshot = _snapshot("BTCUSDT", Side.LONG, message_id="snapshot-replay")
    envelope = _envelope(Topics.MARKET_SNAPSHOT, snapshot.to_payload(), envelope_id="bus-replay")
    bus = _FakeBus({Topics.MARKET_SNAPSHOT: [envelope]})
    bus.fail_ack = True
    service = RouterService(_settings(), bus=bus)

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
    service = RouterService(_settings(), bus=bus)

    await service._consume_sentiment()
    bus.fail_ack = False
    await service._consume_sentiment()

    assert len(service.window._sentiment["BTCUSDT"]) == 1


async def test_invalid_sentiment_and_control_are_not_acked():
    sentiment = _envelope(Topics.SENTIMENT_SIGNAL, {"not": "valid"}, envelope_id="bad-sentiment")
    control = _envelope(Topics.SYSTEM_CONTROL, {"mode": "UNKNOWN"}, envelope_id="bad-control")
    bus = _FakeBus(
        {
            Topics.SENTIMENT_SIGNAL: [sentiment],
            Topics.SYSTEM_CONTROL: [control],
        }
    )
    service = RouterService(_settings(), bus=bus)

    await asyncio.gather(service._consume_sentiment(), service._consume_control())

    assert bus.operations == []
    assert service.system_mode is SystemMode.NORMAL


async def test_valid_control_is_applied_before_ack():
    control = _envelope(Topics.SYSTEM_CONTROL, {"mode": "CONFLICT_SAFE"}, envelope_id="control")
    bus = _FakeBus({Topics.SYSTEM_CONTROL: [control]})
    service = RouterService(_settings(), bus=bus)

    await service._consume_control()

    assert service.system_mode is SystemMode.CONFLICT_SAFE
    assert bus.operations == [("ack", Topics.SYSTEM_CONTROL, "control")]


def test_configured_usdt_topic_is_symbol_specific():
    service = RouterService(_settings(), bus=_FakeBus())
    signal = _sentiment("BTCUSDT", Side.LONG)

    service._process_sentiment(
        _envelope(Topics.SENTIMENT_SIGNAL, signal.to_payload(), envelope_id="sentiment")
    )

    assert service.window.text_bias("BTCUSDT") is Side.LONG
    assert service.window.text_bias("*") is Side.FLAT


async def test_conflict_safe_blocks_only_gpt_escalation():
    bus = _FakeBus()
    service = RouterService(_settings(), bus=bus)
    service._apply_control(
        _envelope(Topics.SYSTEM_CONTROL, {"mode": "CONFLICT_SAFE"}, envelope_id="control-1")
    )
    service.window.add_sentiment("BTCUSDT", 0.8)
    conflicting = _snapshot("BTCUSDT", Side.SHORT, message_id="snapshot-conflict")

    await service._process_snapshot(
        _envelope(Topics.MARKET_SNAPSHOT, conflicting.to_payload(), envelope_id="bus-conflict")
    )

    assert service.fsm.state("BTCUSDT").mode is RouterMode.ROUTE_GPT
    assert bus.published == []

    service.window.add_sentiment("ETHUSDT", 0.8)
    calm = _snapshot("ETHUSDT", Side.LONG, message_id="snapshot-calm")
    await service._process_snapshot(
        _envelope(Topics.MARKET_SNAPSHOT, calm.to_payload(), envelope_id="bus-calm")
    )

    assert bus.published[0][1]["mode"] == "ROUTE_PRO"


async def test_local_quant_blocks_analytical_decisions_but_text_mode_does_not():
    bus = _FakeBus()
    service = RouterService(_settings(), bus=bus)
    service.system_mode = SystemMode.LOCAL_QUANT_MODE

    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot("BTCUSDT", Side.FLAT, message_id="local").to_payload(),
            envelope_id="bus-local",
        )
    )
    assert bus.published == []

    service.system_mode = SystemMode.TEXT_LOCAL_FILTER
    await service._process_snapshot(
        _envelope(
            Topics.MARKET_SNAPSHOT,
            _snapshot("ETHUSDT", Side.FLAT, message_id="text-local").to_payload(),
            envelope_id="bus-text-local",
        )
    )
    assert bus.published[0][1]["mode"] == "ROUTE_PRO"


async def test_run_closes_bus_after_all_taskgroup_consumers_finish():
    bus = _FakeBus()
    service = RouterService(_settings(), bus=bus)

    await service.run()

    assert bus.closed is True


async def test_run_closes_bus_when_taskgroup_consumer_fails():
    bus = _FakeBus()
    bus.fail_subscribe_topic = Topics.MARKET_SNAPSHOT
    service = RouterService(_settings(), bus=bus)

    with pytest.raises(ExceptionGroup):
        await service.run()

    assert bus.closed is True
