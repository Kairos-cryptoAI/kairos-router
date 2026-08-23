"""Candidate-specific Strategy Parity -> PAPER routing tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from kairos_core.bus import BusEnvelope, MessageBus
from kairos_core.contracts import (
    CandidateRouteV1,
    ExitPlanV1,
    SentimentSignal,
    StrategyIntentV1,
    StrategyProvenanceV1,
)
from kairos_core.enums import (
    CandidateReviewTier,
    ImpactDirection,
    ReasoningEffort,
    Side,
    SystemMode,
    TradingMode,
)
from kairos_core.topics import Topics

from kairos_router.candidate import CandidateRouterPolicy, classify_candidate
from kairos_router.config import RouterSettings
from kairos_router.service import RouterService

NOW = datetime(2026, 8, 23, 12, 0, 10, tzinfo=UTC)
DECISION_MS = int(datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC).timestamp() * 1_000) - 1
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


class _Bus(MessageBus):
    def __init__(self, messages: list[BusEnvelope] | None = None) -> None:
        self.messages = messages or []
        self.operations: list[tuple[str, str]] = []
        self.published: list[tuple[str, dict]] = []
        self.fail_publish = False

    async def publish(self, topic: str, message) -> str:
        self.operations.append(("publish", topic))
        if self.fail_publish:
            raise RuntimeError("publish failed")
        self.published.append((topic, self._to_payload(message)))
        return "published"

    async def subscribe(
        self,
        topic: str,
        *,
        group: str | None = None,
        consumer: str | None = None,
    ) -> AsyncIterator[BusEnvelope]:
        for message in self.messages:
            if message.topic == topic:
                yield message

    async def ack(self, topic: str, envelope: BusEnvelope, *, group: str | None = None) -> None:
        self.operations.append(("ack", topic))


def _settings(**overrides: object) -> RouterSettings:
    values: dict[str, object] = {
        "bus_backend": "memory",
        "trading_symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"],
        "trading_mode": TradingMode.DRY_RUN,
        "candidate_ttl_s": 120.0,
        "candidate_review_timeout_ms": 20_000,
    }
    values.update(overrides)
    return RouterSettings(**values)


def _intent(**overrides: object) -> StrategyIntentV1:
    values: dict[str, object] = {
        "source": "strategy-engine",
        "strategy_id": "test-canary",
        "strategy_revision": "v1",
        "symbol": "BTCUSDT",
        "side": Side.LONG,
        "decision_ts_ms": DECISION_MS,
        "entry_eligible_ts_ms": DECISION_MS + 1,
        "entry_expires_ts_ms": DECISION_MS + 60_001,
        "reference_price": 100.0,
        "signal_strength": 0.8,
        "gross_reward_bps": 500.0,
        "exit_plan": ExitPlanV1(stop_price=95.0, target_price=105.0, max_holding_ms=180_000),
        "provenance": StrategyProvenanceV1(
            strategy_code_sha256=SHA_A,
            config_sha256=SHA_B,
            input_window_sha256=SHA_C,
            features_sha256=SHA_D,
            input_bar_sha256s=(SHA_A, SHA_B),
        ),
    }
    values.update(overrides)
    return StrategyIntentV1(**values)


def _sentiment(side: Side, *, message_id: str, seconds_before_decision: int = 1) -> SentimentSignal:
    produced_at = datetime.fromtimestamp(DECISION_MS / 1_000, tz=UTC) - timedelta(
        seconds=seconds_before_decision
    )
    return SentimentSignal(
        source="text-scouts",
        message_id=message_id,
        produced_at=produced_at,
        topic="BTCUSDT",
        sentiment=0.8 if side is Side.LONG else -0.8,
        impact=ImpactDirection.BULLISH if side is Side.LONG else ImpactDirection.BEARISH,
        confidence=0.8,
    )


def _envelope(intent: StrategyIntentV1, *, envelope_id: str = "bus-intent") -> BusEnvelope:
    return BusEnvelope(
        id=envelope_id,
        topic=Topics.STRATEGY_INTENT,
        payload=intent.to_payload(),
    )


def _service(bus: _Bus, **settings: object) -> RouterService:
    return RouterService(_settings(**settings), bus=bus, clock=lambda: NOW)


def _seed_text(service: RouterService, signal: SentimentSignal) -> None:
    service._process_sentiment(
        BusEnvelope(
            id=f"bus:{signal.message_id}",
            topic=Topics.SENTIMENT_SIGNAL,
            payload=signal.to_payload(),
        )
    )


def test_classification_is_candidate_specific_and_direction_preserving() -> None:
    assert classify_candidate(Side.LONG, Side.LONG) is CandidateReviewTier.NORMAL
    assert classify_candidate(Side.LONG, Side.FLAT) is CandidateReviewTier.NORMAL
    assert classify_candidate(Side.LONG, Side.SHORT) is CandidateReviewTier.CONFLICT
    assert classify_candidate(Side.SHORT, Side.LONG) is CandidateReviewTier.CONFLICT
    with pytest.raises(ValueError, match="must be directional"):
        classify_candidate(Side.FLAT, Side.LONG)


async def test_no_text_emits_normal_route_with_unchanged_intent() -> None:
    bus = _Bus()
    service = _service(bus)
    intent = _intent()

    await service._process_intent(_envelope(intent))

    topic, payload = bus.published[0]
    route = CandidateRouteV1.model_validate(payload)
    assert topic == Topics.STRATEGY_ROUTE
    assert route.review_tier is CandidateReviewTier.NORMAL
    assert route.requested_reasoning_effort is ReasoningEffort.MEDIUM
    assert route.evidence_ids == ()
    assert route.intent == intent
    assert route.intent.intent_id == intent.intent_id
    assert route.intent.side is Side.LONG
    assert route.review_deadline_ms == intent.decision_ts_ms + 20_000


async def test_opposite_text_uses_conflict_tier_and_exact_evidence_ids() -> None:
    bus = _Bus()
    service = _service(bus)
    _seed_text(service, _sentiment(Side.SHORT, message_id="exact-text-id"))

    await service._process_intent(_envelope(_intent()))

    route = CandidateRouteV1.model_validate(bus.published[0][1])
    assert route.review_tier is CandidateReviewTier.CONFLICT
    assert route.requested_reasoning_effort is ReasoningEffort.HIGH
    assert route.evidence_ids == ("exact-text-id",)
    assert route.conflict_rationale == (
        "strategy_side=LONG text_side=SHORT text_score=-0.640000 text_confidence=0.800000"
    )


async def test_matching_text_remains_normal_but_retains_exact_evidence() -> None:
    bus = _Bus()
    service = _service(bus)
    _seed_text(service, _sentiment(Side.LONG, message_id="official-news-id"))

    await service._process_intent(_envelope(_intent()))

    route = CandidateRouteV1.model_validate(bus.published[0][1])
    assert route.review_tier is CandidateReviewTier.NORMAL
    assert route.evidence_ids == ("official-news-id",)
    assert route.conflict_rationale is None


async def test_route_is_byte_deterministic_for_the_same_inputs() -> None:
    intent = _intent()
    first_bus, second_bus = _Bus(), _Bus()
    first, second = _service(first_bus), _service(second_bus)
    for service in (first, second):
        _seed_text(service, _sentiment(Side.SHORT, message_id="same-text"))

    await first._process_intent(_envelope(intent, envelope_id="first"))
    await second._process_intent(_envelope(intent, envelope_id="second"))

    first_route = CandidateRouteV1.model_validate(first_bus.published[0][1])
    second_route = CandidateRouteV1.model_validate(second_bus.published[0][1])
    assert first_route.to_json() == second_route.to_json()
    assert first_route.route_id == second_route.route_id


@pytest.mark.parametrize(
    "settings,intent_overrides",
    [
        ({"trading_mode": TradingMode.PAPER}, {}),
        ({"trading_mode": TradingMode.LIVE}, {}),
        ({"candidate_ttl_s": 5.0}, {}),
        ({}, {"entry_expires_ts_ms": int(NOW.timestamp() * 1_000)}),
    ],
)
async def test_rejected_strategy_live_stale_and_expired_candidates_fail_closed(
    settings: dict[str, object],
    intent_overrides: dict[str, object],
) -> None:
    bus = _Bus()
    service = _service(bus, **settings)

    await service._process_intent(_envelope(_intent(**intent_overrides)))

    assert bus.published == []


async def test_paper_requires_exact_strategy_revision_allowlist() -> None:
    bus = _Bus()
    service = _service(
        bus,
        trading_mode=TradingMode.PAPER,
        paper_strategy_allowlist=["test-canary@v1"],
    )

    await service._process_intent(_envelope(_intent()))

    assert len(bus.published) == 1


async def test_known_rejected_sleeve_cannot_be_enabled_by_paper_allowlist() -> None:
    bus = _Bus()
    service = _service(
        bus,
        trading_mode=TradingMode.PAPER,
        paper_strategy_allowlist=["trend_breakout_v1@1"],
    )

    await service._process_intent(
        _envelope(
            _intent(strategy_id="trend_breakout_v1", strategy_revision="1"),
        )
    )

    assert bus.published == []


async def test_wrong_paper_revision_is_not_routed() -> None:
    bus = _Bus()
    service = _service(
        bus,
        trading_mode=TradingMode.PAPER,
        paper_strategy_allowlist=["test-canary@v1"],
    )
    wrong_revision = _intent(strategy_revision="v2")
    await service._process_intent(_envelope(wrong_revision, envelope_id="wrong-revision"))
    assert bus.published == []


async def test_conflict_safe_blocks_only_conflict_candidate_route() -> None:
    bus = _Bus()
    service = _service(bus)
    service.system_mode = SystemMode.CONFLICT_SAFE
    _seed_text(service, _sentiment(Side.SHORT, message_id="conflict"))
    await service._process_intent(_envelope(_intent()))
    assert bus.published == []

    second_bus = _Bus()
    normal = _service(second_bus)
    normal.system_mode = SystemMode.CONFLICT_SAFE
    _seed_text(normal, _sentiment(Side.LONG, message_id="agreement"))
    await normal._process_intent(_envelope(_intent()))
    assert len(second_bus.published) == 1


async def test_publish_failure_is_not_acked_or_marked_processed() -> None:
    intent = _intent()
    envelope = _envelope(intent)
    bus = _Bus([envelope])
    bus.fail_publish = True
    service = _service(bus)

    await service._consume_intents()

    assert bus.operations == [("publish", Topics.STRATEGY_ROUTE)]
    assert intent.intent_id not in service._processed_intents


async def test_successful_consume_publishes_before_ack_and_deduplicates() -> None:
    intent = _intent()
    envelope = _envelope(intent)
    bus = _Bus([envelope, envelope])
    service = _service(bus)

    await service._consume_intents()

    assert bus.operations == [
        ("publish", Topics.STRATEGY_ROUTE),
        ("ack", Topics.STRATEGY_INTENT),
        ("ack", Topics.STRATEGY_INTENT),
    ]
    assert len(bus.published) == 1


async def test_invalid_contract_is_never_published_or_acked() -> None:
    envelope = BusEnvelope(
        id="invalid",
        topic=Topics.STRATEGY_INTENT,
        payload={"source": "malformed", "unknown": True},
    )
    bus = _Bus([envelope])
    service = _service(bus)

    await service._consume_intents()

    assert bus.operations == []
    assert bus.published == []


def test_policy_rejects_unresolvable_or_overlong_exact_text_ids() -> None:
    policy = CandidateRouterPolicy(source="router")
    intent = _intent()
    from kairos_router.aggregation import TextAggregate

    with pytest.raises(ValueError, match="exact normalized IDs"):
        policy.build(
            intent,
            TextAggregate(
                bias=Side.LONG,
                sentiment_ids=("x" * 129,),
                has_relevant_evidence=True,
            ),
        )

    bounded = CandidateRouterPolicy(source="router", max_evidence_ids=1)
    with pytest.raises(ValueError, match="cannot truncate"):
        bounded.build(
            intent,
            TextAggregate(
                bias=Side.LONG,
                sentiment_ids=("first", "second"),
                has_relevant_evidence=True,
            ),
        )


def test_paper_allowlist_is_exact_normalized_and_unique() -> None:
    assert _settings(paper_strategy_allowlist=["test-canary@v1"]).paper_strategy_allowlist == [
        "test-canary@v1"
    ]
    with pytest.raises(ValueError):
        _settings(paper_strategy_allowlist=[" test-canary@v1"])
    with pytest.raises(ValueError):
        _settings(paper_strategy_allowlist=["test-canary@v1", "test-canary@v1"])
    with pytest.raises(ValueError, match="must cover every sentiment"):
        _settings(max_sentiment_evidence=5, max_candidate_evidence_ids=4)
