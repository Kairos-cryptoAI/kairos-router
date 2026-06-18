"""The deterministic router state machine with hysteresis.

The FSM is intentionally side-effect free and trivially unit-testable: feed it
ticks, read its mode. The async :mod:`kairos_router.service` wraps it around the
message bus.

DeepSeek-first routing:
  * ``ROUTE_PRO`` — routine flow handled by DeepSeek-V4-Pro.
  * ``ROUTE_GPT`` — escalation to GPT-5.5 when signals keep conflicting.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from kairos_core.enums import ReasoningEffort, RouterMode, Side

from .conflict import is_conflict


@dataclass
class SymbolState:
    mode: RouterMode = RouterMode.ROUTE_PRO
    conflict_streak: int = 0
    calm_streak: int = 0


@dataclass
class RouterFSM:
    """Per-symbol hysteresis machine.

    Transitions:
      * ROUTE_PRO -> ROUTE_GPT  after ``conflict_threshold`` consecutive conflict ticks.
      * ROUTE_GPT -> ROUTE_PRO  after ``calm_threshold`` consecutive calm ticks.
    """

    conflict_threshold: int = 4
    calm_threshold: int = 10
    _states: Dict[str, SymbolState] = field(default_factory=dict)

    def state(self, symbol: str) -> SymbolState:
        return self._states.setdefault(symbol, SymbolState())

    def update(self, symbol: str, quant_bias: Side, text_bias: Side) -> SymbolState:
        st = self.state(symbol)
        if is_conflict(quant_bias, text_bias):
            st.conflict_streak += 1
            st.calm_streak = 0
        else:
            st.calm_streak += 1
            st.conflict_streak = 0

        if st.mode is RouterMode.ROUTE_PRO and st.conflict_streak >= self.conflict_threshold:
            st.mode = RouterMode.ROUTE_GPT
        elif st.mode is RouterMode.ROUTE_GPT and st.calm_streak >= self.calm_threshold:
            st.mode = RouterMode.ROUTE_PRO
        return st

    @staticmethod
    def effort_for(mode: RouterMode) -> ReasoningEffort:
        # ROUTE_GPT -> GPT-5.5 (high); ROUTE_PRO -> DeepSeek-V4-Pro (medium).
        return ReasoningEffort.HIGH if mode is RouterMode.ROUTE_GPT else ReasoningEffort.MEDIUM
