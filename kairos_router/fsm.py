"""The deterministic router state machine with hysteresis.

The FSM is intentionally side-effect free and trivially unit-testable: feed it
ticks, read its mode. The async :mod:`kairos_router.service` wraps it around the
message bus.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from kairos_core.enums import ReasoningEffort, RouterMode, Side

from .conflict import is_conflict


@dataclass
class SymbolState:
    mode: RouterMode = RouterMode.USE_MEDIUM
    conflict_streak: int = 0
    calm_streak: int = 0


@dataclass
class RouterFSM:
    """Per-symbol hysteresis machine.

    Transitions:
      * USE_MEDIUM -> USE_HIGH  after ``conflict_threshold`` consecutive conflict ticks.
      * USE_HIGH   -> USE_MEDIUM after ``calm_threshold`` consecutive calm ticks.
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

        if st.mode is RouterMode.USE_MEDIUM and st.conflict_streak >= self.conflict_threshold:
            st.mode = RouterMode.USE_HIGH
        elif st.mode is RouterMode.USE_HIGH and st.calm_streak >= self.calm_threshold:
            st.mode = RouterMode.USE_MEDIUM
        return st

    @staticmethod
    def effort_for(mode: RouterMode) -> ReasoningEffort:
        return ReasoningEffort.HIGH if mode is RouterMode.USE_HIGH else ReasoningEffort.MEDIUM
