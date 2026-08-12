"""Kairos Layer 2 — The Router.

A hard-coded, deterministic finite-state machine (no LLM) that watches the
agreement between Quant Scouts and Text Scouts and decides how much analytical
effort the Aggregator should spend: ``ROUTE_PRO`` normally, ``ROUTE_GPT`` when
signals conflict. Hysteresis prevents flag chatter ("дребезг").
"""

from __future__ import annotations

__version__ = "0.1.0"

from .conflict import is_conflict, sentiment_to_side
from .fsm import RouterFSM, SymbolState

__all__ = ["RouterFSM", "SymbolState", "sentiment_to_side", "is_conflict", "__version__"]
