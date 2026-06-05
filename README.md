# faro-liquidation-density

Forward-looking BTC liquidation density pipeline for Hyperliquid.

Infers liquidation events from OI-delta on the `activeAssetCtx` WebSocket,
buckets them by price level ($500 increments), and outputs a density table
showing where forced position closures are most likely to concentrate next.

No third-party data required. Chain-native source only.

---

## What it produces

```
Bucket      Long $       Short $      Total $    Long%   Dist%
$60,500   $86,266.9K    $6,031.3K   $92,298.2K  93.5%  -0.47%  SPOT  HOT ZONE
$61,000   $26,056.4K   $15,323.3K   $41,379.6K  63.0%  +0.35%  SPOT  HOT ZONE
```

**Signal output (June 5, 2026, live run):**
```json
{
  "signal_type": "liquidation_cascade",
  "asset": "BTC",
  "trust_tier": 1,
  "direction": "bearish",
  "value_usd": 133677234.0,
  "confidence": 0.84,
  "cluster_low": 60500,
  "cluster_high": 61000,
  "long_pct": 84.0,
  "summary": "Liquidation cluster $133.7M below spot at $60,500-$61,000. Long-heavy: 84%. Signal: BEARISH."
}
```

2,425 events captured over 1 hour. $359.71M total notional. 74.8% long liquidations.

---

## Files

| File | Purpose |
|---|---|
| `faro_liquidation_density_dag.py` | Airflow DAG for production orchestration (5-min schedule) |
| `run.py` | Standalone runner with no Airflow dependency |
| `signal_event.json` | Live run output as an agent-ready SignalEvent payload |
| `density_summary.txt` | Live run summary in human-readable format |

---

## How to run

**Step 1.** Collect liquidation data using the streamer from
[hl-liquidation-heatmap](https://github.com/yodablocks/hl-liquidation-heatmap):

```bash
cd hl-liquidation-heatmap
python stream.py --coins BTC
# let it run 15 to 60 minutes, then Ctrl+C
```

**Step 2.** Run the density pipeline:

```bash
pip install requests
python run.py --db /path/to/liquidations.db
```

Output: `liquidation_density.db`, `signal_event.json`, `density_summary.txt`

---

## How it works

Hyperliquid's public API does not expose a native liquidation flag.
The workaround is OI-delta inference:

1. Subscribe to `activeAssetCtx` WebSocket at roughly 1-second ticks per coin
2. When OI drops meaningfully between ticks, a forced close occurred
3. Price at that tick is the liquidation price. OI drop times markPx equals notional
4. Side: price falling means long liquidated, price rising means short liquidated
5. Bucket events by $500 price bands and aggregate notional by side

This is the same data source the Hyperliquid UI uses for its own liquidation display.
Trust tier 1, chain-native, no intermediary.

The Airflow DAG wraps this into a production pipeline:
freshness gate, backfill, transform, validate, store, signal, alert.

---

## Signal pipeline integration

The `signal_event.json` output maps to the `SignalEvent` schema in
[signal-pipeline](https://github.com/yodablocks/signal-pipeline),
where it feeds the model layer directional scoring as a
`liquidation_cascade` signal at trust tier 1.

---

## Related repos

- [hl-liquidation-heatmap](https://github.com/yodablocks/hl-liquidation-heatmap): WebSocket streamer and heatmap visualizer
- [signal-pipeline](https://github.com/yodablocks/signal-pipeline): source-agnostic signal ingestion and agent context assembly
- [perp-liquidity](https://github.com/yodablocks/perp-liquidity): cross-venue perp data panel across 8 DEXes
