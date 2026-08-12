"""Async router service: bus inputs -> FSM -> routing decisions.

Messages are acknowledged only after their complete processing succeeds. System-mode
broadcasts prevent routing into unavailable analytical contours without blocking the
separate protective execution path.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict

from kairos_core.bus import BusEnvelope, MessageBus, build_bus
from kairos_core.contracts import MarketSnapshot, RouterDecision, SentimentSignal
from kairos_core.enums import ImpactDirection, RouterMode, SystemMode
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics

from .aggregation import SignalWindow
from .config import RouterSettings
from .fsm import RouterFSM, SymbolState

log = get_logger("router")


class RouterService:
    def __init__(
        self,
        settings: RouterSettings | None = None,
        *,
        bus: MessageBus | None = None,
    ) -> None:
        self.settings = settings or RouterSettings()
        self.bus = bus if bus is not None else build_bus(self.settings)
        self.fsm = RouterFSM(self.settings.conflict_threshold, self.settings.calm_threshold)
        self.window = SignalWindow(self.settings.sentiment_ttl_s, self.settings.sentiment_deadband)
        self.system_mode = SystemMode.NORMAL
        self._processed_snapshots: OrderedDict[str, None] = OrderedDict()
        self._processed_sentiments: OrderedDict[str, None] = OrderedDict()

    def _remember(self, cache: OrderedDict[str, None], message_id: str) -> None:
        cache[message_id] = None
        cache.move_to_end(message_id)
        while len(cache) > self.settings.processed_cache_size:
            cache.popitem(last=False)

    def _process_sentiment(self, envelope: BusEnvelope) -> None:
        signal = SentimentSignal.model_validate(envelope.payload)
        if signal.message_id in self._processed_sentiments:
            return
        # An exact configured market is symbol-specific; news topics remain broadcasts.
        symbol = signal.topic if self.settings.symbol_allowed(signal.topic) else "*"
        score = signal.sentiment if signal.impact is not ImpactDirection.NEUTRAL else 0.0
        self.window.add_sentiment(symbol, score)
        self._remember(self._processed_sentiments, signal.message_id)

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
        if snapshot.message_id in self._processed_snapshots:
            return
        if not self.settings.symbol_allowed(snapshot.symbol):
            log.warning("router.symbol_rejected", symbol=snapshot.symbol)
            self._remember(self._processed_snapshots, snapshot.message_id)
            return

        self.window.set_quant_bias(snapshot.symbol, snapshot.quant_bias)
        quant_bias = snapshot.quant_bias
        text_bias = self.window.text_bias(snapshot.symbol)
        if text_bias.value == "FLAT":
            text_bias = self.window.text_bias("*")

        # Keep the transition provisional until the outbound publish succeeds.
        state = self.fsm.preview(snapshot.symbol, quant_bias, text_bias)
        blocked_reason = self._blocked_reason(state)
        if blocked_reason is not None:
            self.fsm.commit(snapshot.symbol, state)
            log.warning(
                "router.decision_suppressed",
                symbol=snapshot.symbol,
                mode=state.mode.value,
                system_mode=self.system_mode.value,
                reason=blocked_reason,
            )
            self._remember(self._processed_snapshots, snapshot.message_id)
            return

        decision = RouterDecision(
            message_id=f"router:{snapshot.message_id}",
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
                f"system_mode={self.system_mode.value}"
            ),
            snapshot_id=snapshot.message_id,
        )
        await self.bus.publish(Topics.ROUTER_DECISION, decision)
        self.fsm.commit(snapshot.symbol, state)
        self._remember(self._processed_snapshots, snapshot.message_id)
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
        finally:
            await self.close()


def main() -> None:  # pragma: no cover
    asyncio.run(RouterService().run())


if __name__ == "__main__":
    main()
