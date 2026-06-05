"""
run.py
Standalone runner for the liquidation density pipeline.
Executes the same logic as faro_liquidation_density_dag.py
without requiring an Airflow installation.

Usage:
    python run.py --db /path/to/liquidations.db

The liquidations.db is written by stream.py from:
    https://github.com/yodablocks/hl-liquidation-heatmap
"""

import argparse
import json
import math
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── config ────────────────────────────────────────────────────────────────────

COIN = "BTC"
BUCKET_SIZE = 500
LOOKBACK_HOURS = 72
MAX_STALENESS_S = 600
MIN_CLUSTER_USD = 50_000
PROXIMITY_PCT = 2.0
HL_REST_URL = "https://api.hyperliquid.xyz/info"


# ── helpers ───────────────────────────────────────────────────────────────────

def bucket(price: float) -> int:
    return int(math.floor(price / BUCKET_SIZE) * BUCKET_SIZE)


def mark_px() -> float:
    resp = requests.post(HL_REST_URL, json={"type": "metaAndAssetCtxs"}, timeout=10)
    resp.raise_for_status()
    meta, ctxs = resp.json()
    for i, asset in enumerate(meta.get("universe", [])):
        if asset.get("name") == COIN and i < len(ctxs):
            return float(ctxs[i].get("markPx", 0))
    raise ValueError(f"{COIN} not found in HL universe")


# ── pipeline ──────────────────────────────────────────────────────────────────

def run(db_path: Path):
    # freshness check
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT MAX(ts) FROM liquidations WHERE coin=?", (COIN,)
        ).fetchone()
        max_ts = row[0]
        if not max_ts:
            raise ValueError(f"No rows for {COIN}")
        age = (time.time() * 1000 - max_ts) / 1000
        if age > MAX_STALENESS_S:
            print(f"WARNING: data is {age:.0f}s old (threshold {MAX_STALENESS_S}s)")

        # fetch only the lookback window
        cutoff_ms = int((time.time() - LOOKBACK_HOURS * 3600) * 1000)
        rows = conn.execute(
            "SELECT px, notional, side, ts FROM liquidations "
            "WHERE coin=? AND ts>=? ORDER BY ts",
            (COIN, cutoff_ms)
        ).fetchall()

    # build + validate in one pass
    if not rows:
        raise ValueError(f"No rows for {COIN} in the last {LOOKBACK_HOURS}h — check DB")

    by_bucket: dict[int, dict] = defaultdict(lambda: {"long": 0.0, "short": 0.0})
    agg: dict[tuple, dict] = defaultdict(lambda: {"notional": 0.0, "count": 0})
    long_total = short_total = 0.0

    for price, notional, side, ts in rows:
        bkt = bucket(float(price))
        period = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:00")
        notional = float(notional)
        agg[(bkt, period, side)]["notional"] += notional
        agg[(bkt, period, side)]["count"] += 1
        by_bucket[bkt][side] += notional
        if side == "long":
            long_total += notional
        else:
            short_total += notional

    total = long_total + short_total
    if total >= 50_000_000_000:
        raise ValueError(f"Notional ${total:.0f} exceeds sanity bound — possible data corruption")

    density = [
        {
            "coin": COIN,
            "price_bucket": k[0],
            "period": k[1],
            "side": k[2],
            "notional_usd": round(v["notional"], 2),
            "event_count": v["count"],
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        for k, v in agg.items()
    ]

    # write store
    output_db = db_path.parent / "liquidation_density.db"
    with sqlite3.connect(output_db) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS liquidation_density (
                coin TEXT, price_bucket INTEGER, period TEXT, side TEXT,
                notional_usd REAL DEFAULT 0, event_count INTEGER DEFAULT 0,
                updated_at TEXT, PRIMARY KEY (coin, price_bucket, period, side)
            )
        """)
        conn.executemany("""
            INSERT INTO liquidation_density
              (coin, price_bucket, period, side, notional_usd, event_count, updated_at)
            VALUES (:coin, :price_bucket, :period, :side, :notional_usd, :event_count, :updated_at)
            ON CONFLICT (coin, price_bucket, period, side) DO UPDATE SET
              notional_usd = excluded.notional_usd,
              event_count  = excluded.event_count,
              updated_at   = excluded.updated_at
        """, density)

    # mark price + signal
    try:
        spot = mark_px()
    except Exception as e:
        if not by_bucket:
            raise RuntimeError("No bucket data and mark price fetch failed — cannot continue") from e
        print(f"Warning: mark price fetch failed ({e}). Using mean bucket boundary.")
        spot = float(sum(by_bucket) / len(by_bucket))

    nearby = {
        bkt: v for bkt, v in by_bucket.items()
        if abs(bkt - spot) <= spot * (PROXIMITY_PCT / 100)
        and (v["long"] + v["short"]) >= MIN_CLUSTER_USD
    }

    direction, strength, top_bkt = "neutral", 0.0, None
    if nearby:
        long_near  = sum(v["long"]  for v in nearby.values())
        short_near = sum(v["short"] for v in nearby.values())
        near_total = long_near + short_near
        long_pct   = long_near / near_total
        top_bkt    = max(nearby, key=lambda b: nearby[b]["long"] + nearby[b]["short"])
        below      = top_bkt < spot
        if   long_pct > 0.6 and below:     direction, strength = "bearish", long_pct
        elif long_pct < 0.4 and not below: direction, strength = "bullish", 1 - long_pct
    else:
        near_total = long_pct = 0.0

    event = {
        "signal_type": "liquidation_cascade",
        "asset": COIN, "trust_tier": 1,
        "direction": direction,
        "value_usd": round(near_total, 2),
        "confidence": round(strength, 3),
        "mark_px": spot,
        "cluster_low":  top_bkt,
        "cluster_high": top_bkt + BUCKET_SIZE if top_bkt else None,
        "long_pct": round(long_pct * 100, 1) if nearby else 0,
        "summary": (
            f"Liquidation cluster ${near_total/1e6:.1f}M "
            f"{'below' if top_bkt and top_bkt < spot else 'above'} spot "
            f"at ${top_bkt:,}-${top_bkt + BUCKET_SIZE:,}. "
            f"Long-heavy: {long_pct:.0%}. Signal: {direction.upper()}."
        ) if top_bkt else "No cluster within proximity threshold.",
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }

    # print summary — window count, not lifetime total
    print(f"\n{'='*60}")
    print(f"  Faro — Liquidation Density Pipeline")
    print(f"  {len(rows)} events (last {LOOKBACK_HOURS}h) · ${total/1e6:.2f}M notional · mark ${spot:,.0f}")
    print(f"{'='*60}")
    print(f"  Long:   ${long_total/1e6:.2f}M ({long_total/total*100:.1f}%)")
    print(f"  Short:  ${short_total/1e6:.2f}M ({short_total/total*100:.1f}%)")
    print(f"  Signal: {direction.upper()} · confidence {strength:.0%}")
    print(f"  {event['summary']}")
    print(f"{'='*60}\n")

    # write outputs
    signal_f  = db_path.parent / "signal_event.json"
    summary_f = db_path.parent / "density_summary.txt"
    signal_f.write_text(json.dumps(event, indent=2))
    summary_f.write_text("\n".join([
        "Faro Liquidation Density — Run Summary",
        f"Asset: {COIN}",
        f"Run at: {datetime.now(tz=timezone.utc).isoformat()}",
        f"Events (last {LOOKBACK_HOURS}h): {len(rows)}  |  Density rows: {len(density)}",
        f"Total: ${total/1e6:.2f}M  |  Long: ${long_total/1e6:.2f}M ({long_total/total*100:.1f}%)  |  Short: ${short_total/1e6:.2f}M",
        f"Mark price: ${spot:,.0f}",
        f"Signal: {direction.upper()} (confidence {strength:.1%})",
        f"Summary: {event['summary']}",
    ]))


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Faro liquidation density — standalone runner")
    parser.add_argument("--db", default="liquidations.db")
    run(Path(parser.parse_args().db))