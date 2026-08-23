"""Kairos Layer 2 — The Router.

A hard-coded, deterministic finite-state machine (no LLM) that watches the
agreement between Quant Scouts and Text Scouts and decides how much analytical
effort the Aggregator should spend: ``ROUTE_PRO`` normally, ``ROUTE_GPT`` when
signals conflict. Hysteresis prevents flag chatter ("дребезг").
"""

from __future__ import annotations

__version__ = "0.2.0"

from .candidate import CandidateRouterPolicy, classify_candidate
from .conflict import is_conflict, sentiment_to_side
from .fsm import RouterFSM, SymbolState

__all__ = [
    "CandidateRouterPolicy",
    "RouterFSM",
    "SymbolState",
    "classify_candidate",
    "sentiment_to_side",
    "is_conflict",
    "__version__",
]
