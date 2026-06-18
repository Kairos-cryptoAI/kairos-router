# kairos-router

**Layer 2 — The Router.** A hard-coded, deterministic finite-state machine (no LLM)
that decides how much analytical effort the Aggregator should spend.

## Why it exists
Calling a flagship LLM on every tick is expensive. The Router protects the budget:
it only escalates to expensive `high`-effort reasoning when Quant and Text signals
genuinely disagree, and it uses **hysteresis** so the flag does not chatter.

## Logic
- **Normal:** trends agree / news calm → emit `USE_MEDIUM`.
- **Conflict:** Quant says `SHORT` while Text says `LONG` (or vice-versa) → count a conflict tick.
- **Escalate:** after `conflict_threshold` (default **4**) consecutive conflict ticks → `USE_HIGH`.
- **De-escalate:** return to `USE_MEDIUM` only after `calm_threshold` (default **10**)
  consecutive calm ticks — this is the anti-chatter guard ("дребезг").

```
        4 conflict ticks
USE_MEDIUM ───────────────▶ USE_HIGH
     ▲                          │
     └──────────────────────────┘
        10 calm ticks
```

## Run
```bash
pip install -e ../kairos-core   # the shared contracts library
pip install -e ".[dev]"
make test
python -m kairos_router          # consumes the bus, emits RouterDecision
```

Consumes `kairos.market.snapshot` + `kairos.sentiment.signal`; emits `kairos.router.decision`.

---
Part of the [Kairos](https://github.com/TheLitis/kairos) system. MIT licensed.
