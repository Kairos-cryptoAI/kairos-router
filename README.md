# kairos-router

**Layer 2 — Router.** A deterministic finite-state machine with no LLM calls. It
decides how much analytical effort the Aggregator may spend.

## Routing policy

- Normal agreement or neutral signals emit `ROUTE_PRO` with medium effort.
- Opposite directional Quant and Text signals increment a conflict streak.
- Four consecutive conflicts escalate to `ROUTE_GPT` with high effort.
- Ten consecutive calm snapshots return the symbol to `ROUTE_PRO`.

Hysteresis is independent per configured market. Symbol-specific sentiment is used
when its topic exactly matches a configured symbol; all other news sentiment is
treated as a market-wide broadcast.

## Degraded system modes

The Router consumes `kairos.system.control` broadcasts:

- `NORMAL` and `TEXT_LOCAL_FILTER` keep normal routing. Text Scouts owns its local
  filtering fallback.
- `CONFLICT_SAFE` suppresses only decisions that would escalate to unavailable GPT.
  Calm `ROUTE_PRO` traffic remains available.
- `LOCAL_QUANT_MODE` detaches the analytical path and suppresses all Router decisions.
  `RouterDecision` contains no CLOSE or reduce-only action; protective exits remain
  available through the independent Execution Engine path.

Messages are acknowledged only after validation and all required output publishing
succeed. Router decisions derive a deterministic message ID from the source snapshot,
and FSM transitions are committed only after publication. A bounded replay cache keeps
an ACK retry from advancing hysteresis, republishing a decision, or double-weighting the
same sentiment signal.

## Local development

Install [uv](https://docs.astral.sh/uv/) once. The repository pins uv 0.12.3,
Python 3.11, all transitive dependencies, and its compatible `kairos-core` revision:

```powershell
winget install --id astral-sh.uv --exact
uv sync --locked
uv run --locked python -m kairos_router
```

Inputs:

- `kairos.market.snapshot`
- `kairos.sentiment.signal`
- `kairos.system.control`

Output: `kairos.router.decision`.

## Checks

```powershell
uv run --locked ruff check kairos_router tests
uv run --locked ruff format --check kairos_router tests
uv run --locked mypy kairos_router
uv run --locked bandit -q -r kairos_router -x tests
uv run --locked pytest -q --tb=short
uv build --no-sources
```

CI runs the blocking suite on Linux with Python 3.11 and 3.14, plus Windows with
Python 3.11.

---

Part of the [Kairos](https://github.com/Kairos-cryptoAI/kairos) system. MIT licensed.
