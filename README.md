# kairos-router

**Layer 2 — Router.** A deterministic service with no LLM calls. It preserves the
legacy DRY_RUN routing FSM and owns the separate candidate-specific route from the
Strategy Engine to the Aggregator.

## Legacy DRY_RUN routing policy

- Only directional Quant plus directional, confidence-calibrated Text evidence can
  emit a routing decision. Missing, neutral, contradictory, or insufficiently
  confident evidence is an explicit abstention: it publishes no analytical route.
- Opposite directional Quant and Text signals increment a conflict streak. Four
  consecutive conflicts escalate to `ROUTE_GPT` with high effort.
- Directional agreement increments a calm streak. Ten consecutive agreements return
  the symbol to `ROUTE_PRO`. An abstention resets both consecutive streaks but never
  de-escalates an already escalated symbol.

Hysteresis is independent per configured market. Text confidence calibrates direction
as `mean(sentiment * confidence)` before applying the deadband. Signals are deduplicated
by message ID and selected against the source snapshot's event time, not delivery time.
The inclusive scoring window is `[snapshot time - TTL, snapshot time]`; future text
never leaks into an earlier decision. A small wall-clock tolerance is used only when
ingesting events. Stale, far-future, and out-of-order snapshots cannot advance hysteresis.

Fresh symbol-specific evidence takes precedence over market-wide broadcasts. Neutral
or unreliable symbol evidence therefore abstains instead of silently inheriting a
broad market direction. `RouterDecision.sentiment_ids` lists exactly the signals used
in its text score, newest-first by `(-produced_at, message_id)` for reproducible downstream
compilation. Confidence eligibility is applied before the newest-first evidence cap
(`KAIROS_MAX_SENTIMENT_EVIDENCE`, default `5`), matching the Aggregator's compact context;
newer low-confidence noise therefore cannot evict usable evidence.

## Strategy candidate routing

The PAPER-safe route consumes an immutable `StrategyIntentV1` and emits a
`CandidateRouteV1`:

```text
kairos.strategy.intent.v1 -> Router -> kairos.strategy.route.v1
```

The Router never creates a side, stop, target, timeout or quantity. The complete
intent is nested unchanged in the route and its canonical hash is revalidated by
`kairos-core`.

- No eligible directional Text evidence, or Text agreement with the strategy side,
  selects `NORMAL` with `reasoning=medium`.
- Fresh directional Text evidence opposite to the strategy side selects `CONFLICT`
  with `reasoning=high` and a deterministic conflict rationale.
- `evidence_ids` contains only the exact `SentimentSignal.message_id` values used in
  that classification. Strategy, feature and bar provenance remains inside the intent.
- Routing time and review deadline are anchored to the intent decision timestamp, so
  identical causal inputs produce identical route bytes and IDs across retries and OSes.
- Unsupported, future, stale, expired, deadline-missed and degraded-mode candidates
  are terminal safe drops. A new closed bar must produce a new intent.

PAPER adds an exact `<strategy_id>@<revision>` allowlist. It is empty by default, and
the five Strategy Engine sleeves currently marked `REJECTED` are also hard-denied even
if accidentally added to the allowlist. `LIVE` candidate routing is unconditionally
disabled in this milestone. A canary/research fixture can be routed only after an
explicit configuration entry; this does not promote strategy alpha.

## Degraded system modes

The Router consumes `kairos.system.control` broadcasts:

- `NORMAL` and `TEXT_LOCAL_FILTER` keep routing enabled, subject to the same confidence
  and direction gates. Text Scouts owns its local filtering fallback.
- `CONFLICT_SAFE` suppresses decisions while a symbol remains on the unavailable GPT
  lane. Calm `ROUTE_PRO` traffic remains available.
- `LOCAL_QUANT_MODE` detaches the analytical path and suppresses all Router decisions.
  `RouterDecision` contains no CLOSE or reduce-only action; protective exits remain
  available through the independent Execution Engine path.

Messages are acknowledged only after validation and all required output publishing
succeed. Safe drops (unsupported, stale/future sentiment, and unsupported, stale,
future, out-of-order, or abstaining snapshots) are acknowledged without publishing.
Router decisions derive a deterministic message
ID from the source snapshot, and publishable FSM transitions are committed only after
publication. Bounded replay caches and event-time watermarks keep ACK retries from
advancing hysteresis, republishing a decision, or double-weighting sentiment.

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
- `kairos.strategy.intent.v1`
- `kairos.sentiment.signal`
- `kairos.system.control`

Outputs:

- `kairos.router.decision` (legacy DRY_RUN)
- `kairos.strategy.route.v1` (Strategy Parity / PAPER)

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

## Runtime delivery durability

With Redis, consumed IDs, handler outputs and completion are committed through
`kairos-persistence`; Redis is ACKed only after PostgreSQL commits. Configure
`KAIROS_PERSISTENCE_DATABASE_URL` through the deployment secret provider. The
in-memory backend intentionally bypasses persistence for local tests.

---

Part of the [Kairos](https://github.com/Kairos-cryptoAI/kairos) system. MIT licensed.
