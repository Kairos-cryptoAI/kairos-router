"""Pure helpers that turn raw signals into a directional bias and detect conflict."""

from __future__ import annotations

from kairos_core.enums import Side


def sentiment_to_side(net_sentiment: float, deadband: float = 0.25) -> Side:
    """Map an aggregate sentiment score in [-1, 1] to a directional bias."""
    if net_sentiment > deadband:
        return Side.LONG
    if net_sentiment < -deadband:
        return Side.SHORT
    return Side.FLAT


def is_conflict(quant_bias: Side, text_bias: Side) -> bool:
    """A tick is *conflicting* only when both biases are directional and opposite.

    Example from the spec: Quant Scouts SHORT while Text Scouts LONG.
    """
    if quant_bias is Side.FLAT or text_bias is Side.FLAT:
        return False
    return quant_bias is not text_bias
