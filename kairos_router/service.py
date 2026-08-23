"""Async router service: bus inputs -> FSM -> routing decisions.

Messages are acknowledged only after their complete processing succeeds. System-mode
broadcasts prevent routing into unavailable analytical contours without blocking the
separate protective execution path.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from kairos_core.bus import BusEnvelope, MessageBus, build_bus
from kairos_core.contracts import (
    CandidateRouteV1,
    MarketSnapshot,
    RouterDecision,
    SentimentSignal,
    StrategyIntentV1,
)
from kairos_core.enums import CandidateReviewTier, RouterMode, Side, SystemMode, TradingMode
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics
from kairos_persistence import DurableMessageBus

from .aggregation import SignalWindow, TextAggregate
from .candidate import REJECTED_PAPER_STRATEGY_IDS, CandidateRouterPolicy
from .config import RouterSettings
from .fsm import RouterFSM, SymbolState

log = get_logger("router")


class RouterService:
    def __init__(
        self,
        settings: RouterSettings | None = None,
        *,
        bus: MessageBus | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings or RouterSettings()
        if bus is not None:
            self.bus = bus
        else:
            transport = build_bus(self.settings)
            self.bus = (
                transport
                if self.settings.bus_backend == "memory"
                else DurableMessageBus(transport, service_name=self.settings.service_name)
            )
        self._clock = clock or (lambda: datetime.now(UTC))
        self.fsm = RouterFSM(self.settings.conflict_threshold, self.settings.calm_threshold)
        self.candidate_policy = CandidateRouterPolicy(
            source=self.settings.service_name,
            review_timeout_ms=self.settings.candidate_review_timeout_ms,
            max_evidence_ids=self.settings.max_candidate_evidence_ids,
        )
        self.window = SignalWindow(
            sentiment_ttl_s=self.settings.sentiment_ttl_s,
            deadband=self.settings.sentiment_deadband,
            min_confidence=self.settings.sentiment_min_confidence,
            max_evidence=self.settings.max_sentiment_evidence,
            max_points_per_topic=self.settings.processed_cache_size,
        )
        self.system_mode = SystemMode.NORMAL
        self._processed_snapshots: OrderedDict[str, None] = OrderedDict()
        self._processed_sentiments: OrderedDict[str, None] = OrderedDict()
        self._processed_intents: OrderedDict[str, None] = OrderedDict()
        self._snapshot_symbols: OrderedDict[str, str] = OrderedDict()
        self._sentiment_topics: OrderedDict[str, str] = OrderedDict()
        self._intent_message_ids: OrderedDict[str, str] = OrderedDict()
        self._snapshot_watermarks: dict[str, datetime] = {}

    def _remember(self, cache: OrderedDict[str, None], message_id: str) -> None:
        cache[message_id] = None
        cache.move_to_end(message_id)
        while len(cache) > self.settings.processed_cache_size:
            cache.popitem(last=False)

    def _now(self) -> datetime:
        now = self._clock()
        if now.utcoffset() is None:
            raise ValueError("router clock must return a timezone-aware datetime")
        return now.astimezone(UTC)

    def _event_time_rejection(
        self,
        produced_at: datetime,
        *,
        now: datetime,
        ttl_s: float,
        label: str,
    ) -> str | None:
        if produced_at.utcoffset() is None:
            return f"{label} timestamp is not timezone-aware"
        produced_at = produced_at.astimezone(UTC)
        if produced_at < now - timedelta(seconds=ttl_s):
            return f"stale {label}"
        if produced_at > now + timedelta(seconds=self.settings.event_future_tolerance_s):
            return f"future-dated {label}"
        return None

    def _process_sentiment(self, envelope: BusEnvelope) -> None:
        signal = SentimentSignal.model_validate(envelope.payload)
        symbol = signal.topic.strip().upper() if self.settings.symbol_allowed(signal.topic) else "*"
        prior_topic = self._sentiment_topics.get(signal.message_id)
        if prior_topic is not None and prior_topic != symbol:
            raise ValueError(f"sentiment message_id {signal.message_id!r} was reused across topics")
        if signal.message_id in self._processed_sentiments:
            return
        if self.window.has_message(signal.message_id):
            raise ValueError(f"sentiment message_id {signal.message_id!r} is already retained")
        rejection = self._event_time_rejection(
            signal.produced_at,
            now=self._now(),
            ttl_s=self.settings.sentiment_ttl_s,
            label="sentiment",
        )
        if rejection is not None:
            log.warning(
                "router.sentiment_rejected",
                sentiment_id=signal.message_id,
                reason=rejection,
            )
            self._remember(self._processed_sentiments, signal.message_id)
            self._remember_sentiment_identity(signal.message_id, symbol)
            return
        # An exact configured market is symbol-specific; news topics remain broadcasts.
        self.window.add_sentiment(
            symbol,
            message_id=signal.message_id,
            produced_at=signal.produced_at,
            sentiment=signal.sentiment,
            impact=signal.impact,
            confidence=signal.confidence,
        )
        self._remember(self._processed_sentiments, signal.message_id)
        self._remember_sentiment_identity(signal.message_id, symbol)

    def _trim_identity_map(self, cache: OrderedDict[str, str]) -> None:
        while len(cache) > self.settings.processed_cache_size:
            cache.popitem(last=False)

    def _remember_sentiment_identity(self, message_id: str, symbol: str) -> None:
        self._sentiment_topics[message_id] = symbol
        while len(self._sentiment_topics) > self.settings.processed_cache_size:
            evicted_id, evicted_symbol = self._sentiment_topics.popitem(last=False)
            self.window.discard(evicted_symbol, evicted_id)

    def _snapshot_rejection(self, snapshot: MarketSnapshot, *, now: datetime) -> str | None:
        """Return why a snapshot must not advance hysteresis, if any."""
        rejection = self._event_time_rejection(
            snapshot.produced_at,
            now=now,
            ttl_s=self.settings.snapshot_ttl_s,
            label="snapshot",
        )
        if rejection is not None:
            return rejection
        produced_at = snapshot.produced_at.astimezone(UTC)
        # Clock-skew tolerance is an ingestion bound, not permission to make a
        # decision from evidence that postdates the decision evaluation time.
        if produced_at > now:
            return "snapshot postdates routing evaluation time"
        watermark = self._snapshot_watermarks.get(snapshot.symbol)
        if watermark is not None and produced_at <= watermark:
            return "out-of-order snapshot"
        return None

    def _text_aggregate(self, snapshot: MarketSnapshot) -> TextAggregate:
        """Prefer fresh symbol evidence; use broadcast only when none exists."""
        return self._text_aggregate_at(snapshot.symbol, as_of=snapshot.produced_at)

    def _text_aggregate_at(self, symbol: str, *, as_of: datetime) -> TextAggregate:
        """Prefer symbol evidence at a causal event time, then market-wide evidence."""
        specific = self.window.aggregate(symbol, as_of=as_of)
        if specific.has_relevant_evidence:
            return specific
        return self.window.aggregate("*", as_of=as_of)

    def _remember_intent_identity(self, message_id: str, intent_id: str) -> None:
        self._intent_message_ids[message_id] = intent_id
        self._intent_message_ids.move_to_end(message_id)
        self._trim_identity_map(self._intent_message_ids)

    def _candidate_rejection(
        self,
        intent: StrategyIntentV1,
        route: CandidateRouteV1,
        *,
        now_ms: int,
    ) -> str | None:
        """Return a fail-closed reason without altering or rerouting the intent."""

        if not self.settings.symbol_allowed(intent.symbol):
            return "unsupported candidate symbol"
        if self.settings.trading_mode is TradingMode.LIVE:
            return "candidate LIVE routing is not enabled"
        if (
            self.settings.trading_mode is TradingMode.PAPER
            and intent.strategy_id in REJECTED_PAPER_STRATEGY_IDS
        ):
            return "strategy is explicitly REJECTED for PAPER"
        if self.settings.trading_mode is TradingMode.PAPER and not self.settings.paper_strategy_allowed(
            intent.strategy_id,
            intent.strategy_revision,
        ):
            return "strategy revision is not PAPER-approved"
        future_tolerance_ms = int(self.settings.event_future_tolerance_s * 1_000)
        if intent.decision_ts_ms > now_ms + future_tolerance_ms:
            return "future-dated candidate"
        if intent.decision_ts_ms > now_ms:
            return "candidate postdates routing evaluation time"
        if intent.decision_ts_ms < now_ms - int(self.settings.candidate_ttl_s * 1_000):
            return "stale candidate"
        if now_ms >= intent.entry_expires_ts_ms:
            return "expired candidate"
        if now_ms >= route.review_deadline_ms:
            return "candidate review deadline has elapsed"
        if self.system_mode is SystemMode.LOCAL_QUANT_MODE:
            return "LOCAL_QUANT_MODE detaches the candidate review path"
        if self.system_mode is SystemMode.CONFLICT_SAFE and route.review_tier is CandidateReviewTier.CONFLICT:
            return "CONFLICT_SAFE suppresses unavailable conflict review"
        return None

    async def _process_intent(self, envelope: BusEnvelope) -> None:
        """Validate, classify and publish one immutable strategy candidate."""

        intent = StrategyIntentV1.model_validate(envelope.payload)
        if intent.intent_id is None:  # impossible after strict contract validation
            raise ValueError("strategy intent has no canonical identity")
        prior_intent_id = self._intent_message_ids.get(intent.message_id)
        if prior_intent_id is not None and prior_intent_id != intent.intent_id:
            raise ValueError(f"intent message_id {intent.message_id!r} was reused across candidates")
        if intent.intent_id in self._processed_intents:
            return

        event_time = datetime.fromtimestamp(intent.decision_ts_ms / 1_000, tz=UTC)
        text = self._text_aggregate_at(intent.symbol, as_of=event_time)
        route = self.candidate_policy.build(intent, text)
        now_ms = int(self._now().timestamp() * 1_000)
        rejection = self._candidate_rejection(intent, route, now_ms=now_ms)
        if rejection is not None:
            log.warning(
                "router.candidate_rejected",
                intent_id=intent.intent_id,
                strategy_id=intent.strategy_id,
                strategy_revision=intent.strategy_revision,
                symbol=intent.symbol,
                trading_mode=self.settings.trading_mode.value,
                reason=rejection,
            )
            self._remember(self._processed_intents, intent.intent_id)
            self._remember_intent_identity(intent.message_id, intent.intent_id)
            return

        await self.bus.publish(Topics.STRATEGY_ROUTE, route)
        self._remember(self._processed_intents, intent.intent_id)
        self._remember_intent_identity(intent.message_id, intent.intent_id)
        log.info(
            "router.candidate_routed",
            intent_id=intent.intent_id,
            route_id=route.route_id,
            symbol=intent.symbol,
            review_tier=route.review_tier.value,
            review_deadline_ms=route.review_deadline_ms,
        )

    async def _consume_intents(self) -> None:
        async for envelope in self.bus.subscribe(
            Topics.STRATEGY_INTENT,
            group="router",
            consumer="candidate-intents",
        ):
            try:
                await self._process_intent(envelope)
                await self.bus.ack(Topics.STRATEGY_INTENT, envelope, group="router")
            except Exception:
                log.exception("router.candidate_processing_failed", envelope_id=envelope.id)

    async def _consume_sentiment(self) -> None:
        async for envelope in self.bus.subscribe(
            Topics.SENTIMENT_SIGNAL, group="router", consumer="sentiment"
        ):
            try:
                self._process_sentiment(envelope)
                await self.bus.ack(Topics.SENTIMENT_SIGNAL, envelope, group="router")
            except Exception:
                log.exception("router.sentiment_processing_failed", envelope_id=envelope.id)

    def _apply_control(self, envelope: BusEnvelope) -> None:
        raw_mode = envelope.payload.get("mode")
        if not isinstance(raw_mode, str):
            raise ValueError(f"invalid system mode: {raw_mode!r}")
        try:
            mode = SystemMode(raw_mode)
        except ValueError as exc:
            raise ValueError(f"invalid system mode: {raw_mode!r}") from exc

        if mode is not self.system_mode:
            log.warning("router.mode_change", previous=self.system_mode.value, mode=mode.value)
            self.system_mode = mode

    async def _consume_control(self) -> None:
        async for envelope in self.bus.subscribe(Topics.SYSTEM_CONTROL, group="router", consumer="control"):
            try:
                self._apply_control(envelope)
                await self.bus.ack(Topics.SYSTEM_CONTROL, envelope, group="router")
            except Exception:
                log.exception("router.control_processing_failed", envelope_id=envelope.id)

    def _blocked_reason(self, state: SymbolState) -> str | None:
        if self.system_mode is SystemMode.LOCAL_QUANT_MODE:
            # RouterDecision has no CLOSE/REDUCE action. All its outputs invoke the
            # analytical path, so protective exits remain solely in execution.
            return "LOCAL_QUANT_MODE detaches the analytical path"
        if self.system_mode is SystemMode.CONFLICT_SAFE and state.mode is RouterMode.ROUTE_GPT:
            return "CONFLICT_SAFE suppresses unavailable GPT escalation"
        return None

    async def _process_snapshot(self, envelope: BusEnvelope) -> None:
        snapshot = MarketSnapshot.model_validate(envelope.payload)
        normalized_symbol = snapshot.symbol.strip().upper()
        prior_symbol = self._snapshot_symbols.get(snapshot.message_id)
        if prior_symbol is not None and prior_symbol != normalized_symbol:
            raise ValueError(f"snapshot message_id {snapshot.message_id!r} was reused across symbols")
        if snapshot.message_id in self._processed_snapshots:
            return
        if self.settings.trading_mode is not TradingMode.DRY_RUN:
            log.warning(
                "router.legacy_snapshot_rejected",
                snapshot_id=snapshot.message_id,
                trading_mode=self.settings.trading_mode.value,
                reason="legacy RouterDecision route is DRY_RUN-only",
            )
            self._remember(self._processed_snapshots, snapshot.message_id)
            self._snapshot_symbols[snapshot.message_id] = normalized_symbol
            self._trim_identity_map(self._snapshot_symbols)
            return
        if not self.settings.symbol_allowed(snapshot.symbol):
            log.warning("router.symbol_rejected", symbol=snapshot.symbol)
            self._remember(self._processed_snapshots, snapshot.message_id)
            self._snapshot_symbols[snapshot.message_id] = normalized_symbol
            self._trim_identity_map(self._snapshot_symbols)
            return
        snapshot = snapshot.model_copy(update={"symbol": normalized_symbol})

        now = self._now()
        rejection = self._snapshot_rejection(snapshot, now=now)
        if rejection is not None:
            log.warning(
                "router.snapshot_rejected",
                symbol=snapshot.symbol,
                snapshot_id=snapshot.message_id,
                reason=rejection,
            )
            self._remember(self._processed_snapshots, snapshot.message_id)
            self._snapshot_symbols[snapshot.message_id] = snapshot.symbol
            self._trim_identity_map(self._snapshot_symbols)
            return

        quant_bias = snapshot.quant_bias
        text = self._text_aggregate(snapshot)
        text_bias = text.bias

        # Keep the transition provisional until the outbound publish succeeds.
        state = self.fsm.preview(snapshot.symbol, quant_bias, text_bias)
        if quant_bias is Side.FLAT or text_bias is Side.FLAT:
            self.fsm.commit(snapshot.symbol, state)
            self._snapshot_watermarks[snapshot.symbol] = snapshot.produced_at.astimezone(UTC)
            log.warning(
                "router.decision_abstained",
                symbol=snapshot.symbol,
                quant_bias=quant_bias.value,
                text_bias=text_bias.value,
                sentiment_ids=list(text.sentiment_ids),
                reason="directional quant and confidence-calibrated text evidence are both required",
            )
            self._remember(self._processed_snapshots, snapshot.message_id)
            self._snapshot_symbols[snapshot.message_id] = snapshot.symbol
            self._trim_identity_map(self._snapshot_symbols)
            return
        blocked_reason = self._blocked_reason(state)
        if blocked_reason is not None:
            self.fsm.commit(snapshot.symbol, state)
            self._snapshot_watermarks[snapshot.symbol] = snapshot.produced_at.astimezone(UTC)
            log.warning(
                "router.decision_suppressed",
                symbol=snapshot.symbol,
                mode=state.mode.value,
                system_mode=self.system_mode.value,
                reason=blocked_reason,
            )
            self._remember(self._processed_snapshots, snapshot.message_id)
            self._snapshot_symbols[snapshot.message_id] = snapshot.symbol
            self._trim_identity_map(self._snapshot_symbols)
            return

        decision = RouterDecision(
            message_id=f"router:{snapshot.message_id}",
            produced_at=now,
            correlation_id=snapshot.correlation_id or snapshot.message_id,
            causation_id=snapshot.message_id,
            source=self.settings.service_name,
            symbol=snapshot.symbol,
            mode=state.mode,
            requested_effort=self.fsm.effort_for(state.mode),
            conflict_streak=state.conflict_streak,
            calm_streak=state.calm_streak,
            quant_bias=quant_bias,
            text_bias=text_bias,
            rationale=(
                f"conflict={state.conflict_streak} calm={state.calm_streak} "
                f"text_score={text.score:.3f} text_confidence={text.confidence:.3f} "
                f"system_mode={self.system_mode.value}"
            ),
            snapshot_id=snapshot.message_id,
            sentiment_ids=list(text.sentiment_ids),
        )
        await self.bus.publish(Topics.ROUTER_DECISION, decision)
        self.fsm.commit(snapshot.symbol, state)
        self._snapshot_watermarks[snapshot.symbol] = snapshot.produced_at.astimezone(UTC)
        self._remember(self._processed_snapshots, snapshot.message_id)
        self._snapshot_symbols[snapshot.message_id] = snapshot.symbol
        self._trim_identity_map(self._snapshot_symbols)
        log.info(
            "router.decision",
            symbol=snapshot.symbol,
            mode=state.mode.value,
            conflict=state.conflict_streak,
            calm=state.calm_streak,
        )

    async def _consume_snapshots(self) -> None:
        async for envelope in self.bus.subscribe(
            Topics.MARKET_SNAPSHOT, group="router", consumer="snapshots"
        ):
            try:
                await self._process_snapshot(envelope)
                await self.bus.ack(Topics.MARKET_SNAPSHOT, envelope, group="router")
            except Exception:
                log.exception("router.snapshot_processing_failed", envelope_id=envelope.id)

    async def close(self) -> None:
        await self.bus.close()

    async def run(self) -> None:  # pragma: no cover - production consumers are unbounded
        configure_logging(
            self.settings.log_level,
            json_logs=self.settings.log_json,
            service=self.settings.service_name,
        )
        log.info("router.start", system_mode=self.system_mode.value)
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._consume_snapshots(), name="market-snapshots")
                tasks.create_task(self._consume_sentiment(), name="sentiment-signals")
                tasks.create_task(self._consume_control(), name="system-control")
                tasks.create_task(self._consume_intents(), name="strategy-intents")
        finally:
            await self.close()


def main() -> None:  # pragma: no cover
    asyncio.run(RouterService().run())


if __name__ == "__main__":
    main()
