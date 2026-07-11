"""Async router service: bus in -> FSM -> bus out.

Consumes ``MarketSnapshot`` (quant bias) and ``SentimentSignal`` (text bias),
runs the hysteresis FSM per symbol and publishes a ``RouterDecision`` whenever a
fresh snapshot arrives.
"""
from __future__ import annotations

import asyncio

from kairos_core.bus import build_bus
from kairos_core.contracts import MarketSnapshot, RouterDecision, SentimentSignal
from kairos_core.enums import ImpactDirection
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics

from .aggregation import SignalWindow
from .config import RouterSettings
from .fsm import RouterFSM

log = get_logger("router")


class RouterService:
    def __init__(self, settings: RouterSettings | None = None) -> None:
        self.settings = settings or RouterSettings()
        self.bus = build_bus(self.settings)
        self.fsm = RouterFSM(self.settings.conflict_threshold, self.settings.calm_threshold)
        self.window = SignalWindow(self.settings.sentiment_ttl_s, self.settings.sentiment_deadband)

    async def _consume_sentiment(self) -> None:
        async for env in self.bus.subscribe(Topics.SENTIMENT_SIGNAL, group="router", consumer="sentiment"):
            try:
                sig = SentimentSignal.model_validate(env.payload)
                # SentimentSignal.topic doubles as symbol routing when prefixed, else broadcast.
                symbol = sig.topic if sig.topic.isupper() and sig.topic.endswith("USD") else "*"
                score = sig.sentiment if sig.impact is not ImpactDirection.NEUTRAL else 0.0
                self.window.add_sentiment(symbol, score)
            finally:
                await self.bus.ack(Topics.SENTIMENT_SIGNAL, env, group="router")

    async def _consume_snapshots(self) -> None:
        async for env in self.bus.subscribe(Topics.MARKET_SNAPSHOT, group="router", consumer="snap"):
            try:
                snap = MarketSnapshot.model_validate(env.payload)
                if not self.settings.symbol_allowed(snap.symbol):
                    log.warning("router.symbol_rejected", symbol=snap.symbol)
                    continue
                self.window.set_quant_bias(snap.symbol, snap.quant_bias)
                qb = snap.quant_bias
                tb = self.window.text_bias(snap.symbol)
                if tb.value == "FLAT":
                    tb = self.window.text_bias("*")  # fall back to market-wide sentiment
                st = self.fsm.update(snap.symbol, qb, tb)
                decision = RouterDecision(
                    source=self.settings.service_name,
                    symbol=snap.symbol,
                    mode=st.mode,
                    requested_effort=self.fsm.effort_for(st.mode),
                    conflict_streak=st.conflict_streak,
                    calm_streak=st.calm_streak,
                    quant_bias=qb,
                    text_bias=tb,
                    rationale=f"conflict={st.conflict_streak} calm={st.calm_streak}",
                    snapshot_id=snap.message_id,
                )
                await self.bus.publish(Topics.ROUTER_DECISION, decision)
                log.info("router.decision", symbol=snap.symbol, mode=st.mode.value,
                        conflict=st.conflict_streak, calm=st.calm_streak)
            finally:
                await self.bus.ack(Topics.MARKET_SNAPSHOT, env, group="router")

    async def run(self) -> None:
        log.info("router.start", **self.settings.model_dump(include={"conflict_threshold", "calm_threshold"}))
        await asyncio.gather(self._consume_snapshots(), self._consume_sentiment())


def main() -> None:
    s = RouterSettings()
    configure_logging(s.log_level, json_logs=s.log_json, service=s.service_name)
    asyncio.run(RouterService(s).run())


if __name__ == "__main__":
    main()
