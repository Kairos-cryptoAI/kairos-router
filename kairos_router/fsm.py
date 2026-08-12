"""Deterministic per-symbol routing FSM with hysteresis."""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from kairos_core.enums import ReasoningEffort, RouterMode, Side

from .conflict import is_conflict


@dataclass
class SymbolState:
    mode: RouterMode = RouterMode.ROUTE_PRO
    conflict_streak: int = 0
    calm_streak: int = 0


@dataclass
class RouterFSM:
    """Escalate after sustained conflict and de-escalate after sustained calm."""

    conflict_threshold: int = 4
    calm_threshold: int = 10
    _states: dict[str, SymbolState] = field(default_factory=dict)

    def state(self, symbol: str) -> SymbolState:
        return self._states.setdefault(symbol, SymbolState())

    def update(self, symbol: str, quant_bias: Side, text_bias: Side) -> SymbolState:
        state = self.preview(symbol, quant_bias, text_bias)
        return self.commit(symbol, state)

    def preview(self, symbol: str, quant_bias: Side, text_bias: Side) -> SymbolState:
        """Calculate a transition without mutating committed state.

        The service publishes from this preview and commits only after the publish
        succeeds, so a failed delivery cannot advance hysteresis prematurely.
        """
        state = replace(self._states.get(symbol, SymbolState()))
        return self._advance(state, quant_bias, text_bias)

    def commit(self, symbol: str, state: SymbolState) -> SymbolState:
        self._states[symbol] = state
        return state

    def _advance(self, state: SymbolState, quant_bias: Side, text_bias: Side) -> SymbolState:
        if is_conflict(quant_bias, text_bias):
            state.conflict_streak += 1
            state.calm_streak = 0
        else:
            state.calm_streak += 1
            state.conflict_streak = 0

        if state.mode is RouterMode.ROUTE_PRO and state.conflict_streak >= self.conflict_threshold:
            state.mode = RouterMode.ROUTE_GPT
        elif state.mode is RouterMode.ROUTE_GPT and state.calm_streak >= self.calm_threshold:
            state.mode = RouterMode.ROUTE_PRO
        return state

    @staticmethod
    def effort_for(mode: RouterMode) -> ReasoningEffort:
        # ROUTE_GPT -> GPT-5.6 Sol (high); ROUTE_PRO -> DeepSeek-V4-Pro (medium).
        return ReasoningEffort.HIGH if mode is RouterMode.ROUTE_GPT else ReasoningEffort.MEDIUM
