"""Maintains the latest per-symbol quant bias and a TTL-windowed text bias."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from kairos_core.enums import Side

from .conflict import sentiment_to_side


@dataclass
class SignalWindow:
    """Keeps the most recent quant bias and a decaying set of sentiment points."""

    sentiment_ttl_s: float = 600.0
    deadband: float = 0.25
    _quant: dict[str, Side] = field(default_factory=dict)
    _sentiment: dict[str, list[tuple[float, float]]] = field(default_factory=dict)  # symbol -> [(ts, score)]

    def set_quant_bias(self, symbol: str, bias: Side) -> None:
        self._quant[symbol] = bias

    def add_sentiment(self, symbol: str, score: float, *, now: float | None = None) -> None:
        now = now if now is not None else time.time()
        self._sentiment.setdefault(symbol, []).append((now, score))

    def quant_bias(self, symbol: str) -> Side:
        return self._quant.get(symbol, Side.FLAT)

    def text_bias(self, symbol: str, *, now: float | None = None) -> Side:
        now = now if now is not None else time.time()
        fresh = [(ts, s) for (ts, s) in self._sentiment.get(symbol, []) if now - ts <= self.sentiment_ttl_s]
        self._sentiment[symbol] = fresh
        if not fresh:
            return Side.FLAT
        points = [s for (_ts, s) in fresh]
        return sentiment_to_side(sum(points) / len(points), self.deadband)
