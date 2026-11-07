# ADR-0004: Integer ticks and paise; no floating point for money

- **Status:** Accepted (2026-11-07, after review)
- **Date:** 2026-11-07
- **Related:** D3, D5, F4

## Context
`0.1 + 0.2 != 0.3` in IEEE-754 floating point. Money and price comparisons must be exact, deterministic across machines (D5), and fast to compare in the engine.

## Options considered

### Option A: `double`
- Pros: convenient.
- Cons: rounding errors accumulate; equality comparisons for price levels are unsafe; the ledger can drift by fractions of a paisa. **Disqualified.**

### Option B: Arbitrary-precision decimal (`BigDecimal`)
- Pros: exact; handles any scale.
- Cons: immutable objects, so every operation allocates; slow, and violates R2 on the hot path.

### Option C: Integer fixed-point (`long`)
- Prices in **ticks** (1 tick = ₹0.05). ₹1,500.50 = 30,010 ticks.
- Money in **paise** (1/100 ₹). Notional = qty × price_ticks × 5 paise.
- Pros: exact, allocation-free, single-instruction compares, deterministic.
- Cons: you must watch for overflow (`long` max ≈ 9.2 × 10¹⁸ paise ≈ ₹92 quadrillion, which is plenty), and conversion only happens at the API edge.

## Decision
**Option C.** Java has no zero-cost "newtype" (a `record Price(long ticks)` is a heap object, so it allocates, which violates R2 on the hot path). So:
- **Hot path (engine, risk math):** primitive `long`, with unit-suffixed names (`priceTicks`, `notionalPaise`, `qty`) and a static helper class `Money` (`Money.notionalPaise(qty, priceTicks)` using `Math.multiplyExact` for overflow checks).
- **Boundaries (APIs, events, ledger code):** small records (`Price`, `Paise`, `Qty`) for type safety where allocation doesn't matter.
- Conversion to and from decimal strings (`BigDecimal`) happens only in the gateway.
- When Project Valhalla value classes ship in a stable JDK, revisit: they'd give type safety without allocation.

## Consequences
- **Positive:** exactness and speed; types document units.
- **Negative:** primitive `long`s on the hot path rely on naming discipline rather than the compiler; a per-symbol tick size would require a symbol-specific scale (out of scope: one global tick).
- **Risks:** silent overflow in an unchecked multiply → mitigation: helper functions + fuzz tests at boundaries.

## Review outcomes (2026-11-07)

### 1. Why `double` is disqualified, concretely
- `0.1 + 0.2 == 0.30000000000000004`. In the ledger, tiny errors accumulate across millions of postings and D3 (the ledger balances to zero) fails by fractions of a paisa.
- In the book, price levels are looked up by exact price. A computed `1500.95 + 0.10` can be `1501.0499999…`, which creates a **second** level next to `1501.05`, breaking price-time priority. Integer ticks compare exactly.

### 2. Conversion at the edge: parse from strings, reject bad ticks, never round
- Prices arrive as **strings** (`"1501.05"`) and are parsed with `new BigDecimal(s)`, never via `double` (`Double.parseDouble("1501.05") * 20` can produce `30020.999…`, which a cast truncates to the wrong tick).
- `ticks = price.multiply(20).longValueExact()`: this throws if the result has a fractional part, so `"1501.03"` (not a multiple of ₹0.05) is **rejected** with `INVALID_TICK`. We never round a customer's price; rounding a buy up would spend money they didn't authorise.
- Outbound: `ticks / 20` formatted with exactly 2 decimals, only in the gateway.

### 3. Division needs an explicit rounding rule
- `notionalPaise = qty × priceTicks × 5` has no division, so it's exact.
- Division appears in fees (basis points), average price and pro-rata splits. Rule: integer math via `Money` helpers (e.g. `Money.applyBps(paise, bps, RoundingMode.HALF_EVEN)`), one place per rounding rule, documented. The **rounded** amount is what gets posted to both sides of the ledger, so it still balances.
- Average fill price is display-only and computed at the edge.

### 4. Overflow is a security issue, not just a correctness one
- Java `long` silently wraps: a huge `qty` from a malicious client can make `notionalPaise` **negative**, and a negative reservation would *increase* available cash in the risk check.
- Defences: gateway limits (`qty ≤ maxQty`, price within the band) **and** `Math.multiplyExact`/`addExact` in `Money`, which throw instead of wrapping. Fuzz the gateway parser and `Money` with Jazzer/jqwik (extreme values, negatives, zero).
- Unit mix-ups (`qty + priceTicks` compiles) are caught by naming discipline, code review and property tests until Valhalla value classes arrive.

## Revisit when
- Multi-currency or instruments with different tick sizes are introduced.
