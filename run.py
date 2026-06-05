"""
run.py
======
Standalone runner for the liquidation density pipeline.
Executes the same logic as faro_liquidation_density_dag.py
without requiring an Airflow installation.

Usage:
    python run.py --db /path/to/liquidations.db

The liquidations.db is written by stream.py from:
    https://github.com/yodablocks/hl-liquidation-heatmap

Output:
    - liquidation_density.db  (SQLite output table)
    - density_summary.txt     (human-readable report)
    - signal_event.json       (agent-ready SignalEvent payload)
"""

import argparse
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── config ────────────────────────────────────────────────────────────────────

COIN = "BTC"
BUCKET_SIZE = 500           # $500 price buckets
LOOKBACK_HOURS = 72
MAX_STALENESS_S = 600       # 10 minutes
MIN_CLUSTER_USD = 50_000
PROXIMITY_PCT = 2.0
HL_REST_URL = "https://api.hyperliquid.xyz/info"

# ── helpers ───────────────────────────────────────────────────────────────────

def bucket(price: float) -> int:
    return int(math.floor(price / BUCKET_SIZE) * BUCKET_SIZE)


def period_label(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:00")


def mark_px() -> float:
    resp = requests.post(HL_REST_URL, json={"type": "metaAndAssetCtxs"}, timeout=10)
    resp.raise_for_status()
    meta, ctxs = resp.json()
    for i, asset in enumerate(meta.get("universe", [])):
        if asset.get("name") == COIN and i < len(ctxs):
            return float(ctxs[i].get("markPx", 0))
    raise ValueError(f"{COIN} not found in HL universe")


# ── pipeline steps ────────────────────────────────────────────────────────────

def check_freshness(db_path: Path):
    print(f"\n[1/6] Freshness check — {db_path}")
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT MAX(ts), COUNT(*) FROM liquidations WHERE coin=?", (COIN,)
    ).fetchone()
    conn.close()
    max_ts, count = row
    if not max_ts:
        raise ValueError(f"No rows for {COIN} in {db_path}")
    age = (time.time() * 1000 - max_ts) / 1000
    print(f"    {count} events captured. Most recent: {age:.0f}s ago.")
    if age > MAX_STALENESS_S:
        print(f"    WARNING: data is {age:.0f}s old (threshold {MAX_STALENESS_S}s).")
    else:
        print(f"    Freshness OK.")
    return count


def build_density(db_path: Path) -> list[dict]:
    print(f"\n[2/6] Building density table from {LOOKBACK_HOURS}h of events...")
    cutoff_ms = int((time.time() - LOOKBACK_HOURS * 3600) * 1000)
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT px, notional, side, ts FROM liquidations "
        "WHERE coin=? AND ts>=? ORDER BY ts",
        (COIN, cutoff_ms)
    ).fetchall()
    conn.close()

    agg: dict[tuple, dict] = defaultdict(lambda: {"notional": 0.0, "count": 0})
    for px, notional, side, ts in rows:
        key = (bucket(float(px)), period_label(int(ts)), side)
        agg[key]["notional"] += float(notional)
        agg[key]["count"] += 1

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
    print(f"    {len(rows)} raw events -> {len(density)} density rows "
          f"across {len(set(r['price_bucket'] for r in density))} price buckets.")
    return density


def validate(density: list[dict]):
    print(f"\n[3/6] Validating density table...")
    assert density, "Empty density — check DB"
    for r in density:
        assert r["price_bucket"] is not None
        assert r["side"] in ("long", "short"), f"Bad side: {r['side']}"
        assert r["notional_usd"] >= 0, "Negative notional"
    total = sum(r["notional_usd"] for r in density)
    long_total  = sum(r["notional_usd"] for r in density if r["side"] == "long")
    short_total = sum(r["notional_usd"] for r in density if r["side"] == "short")
    assert total < 50_000_000_000, f"Notional ${total:.0f} exceeds sanity bound"
    print(f"    Total notional:  ${total/1e6:.2f}M")
    print(f"    Long liquidated: ${long_total/1e6:.2f}M  "
          f"({long_total/total*100:.1f}%)")
    print(f"    Short liquidated:${short_total/1e6:.2f}M  "
          f"({short_total/total*100:.1f}%)")
    print(f"    Validation passed.")
    return total, long_total, short_total


def write_store(density: list[dict], output_db: Path):
    print(f"\n[4/6] Writing to store -> {output_db}")
    conn = sqlite3.connect(output_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS liquidation_density (
            coin          TEXT    NOT NULL,
            price_bucket  INTEGER NOT NULL,
            period        TEXT    NOT NULL,
            side          TEXT    NOT NULL,
            notional_usd  REAL    NOT NULL DEFAULT 0,
            event_count   INTEGER NOT NULL DEFAULT 0,
            updated_at    TEXT,
            PRIMARY KEY (coin, price_bucket, period, side)
        )
    """)
    conn.commit()
    conn.executemany("""
        INSERT INTO liquidation_density
          (coin, price_bucket, period, side, notional_usd, event_count, updated_at)
        VALUES (:coin, :price_bucket, :period, :side, :notional_usd, :event_count, :updated_at)
        ON CONFLICT (coin, price_bucket, period, side) DO UPDATE SET
          notional_usd = excluded.notional_usd,
          event_count  = excluded.event_count,
          updated_at   = excluded.updated_at
    """, density)
    conn.commit()
    conn.close()
    print(f"    {len(density)} rows written.")


def print_heatmap(density: list[dict], px: float):
    print(f"\n[5/6] Density summary (current mark price: ${px:,.0f})")
    print(f"    {'Bucket':>10}  {'Long $':>12}  {'Short $':>12}  "
          f"{'Total $':>12}  {'Long%':>6}  {'Dist%':>7}")
    print(f"    {'-'*65}")

    by_bucket: dict[int, dict] = defaultdict(lambda: {"long": 0.0, "short": 0.0})
    for r in density:
        by_bucket[r["price_bucket"]][r["side"]] += r["notional_usd"]

    # Sort by total notional descending, show top 15
    ranked = sorted(
        by_bucket.items(),
        key=lambda x: x[1]["long"] + x[1]["short"],
        reverse=True
    )[:15]

    for bkt, v in ranked:
        total = v["long"] + v["short"]
        long_pct = v["long"] / total * 100 if total else 0
        dist_pct = (bkt - px) / px * 100
        marker = " <-- SPOT" if abs(dist_pct) < 1.0 else ""
        proximity = " ** HOT ZONE **" if (
            total >= MIN_CLUSTER_USD and abs(dist_pct) <= PROXIMITY_PCT
        ) else ""
        print(f"    ${bkt:>9,}  ${v['long']/1e3:>10.1f}K  "
              f"${v['short']/1e3:>10.1f}K  ${total/1e3:>10.1f}K  "
              f"{long_pct:>5.1f}%  {dist_pct:>+6.2f}%"
              f"{marker}{proximity}")


def publish_signal(density: list[dict], px: float) -> dict:
    print(f"\n[6/6] Publishing SignalEvent...")
    by_bucket: dict[int, dict] = defaultdict(lambda: {"long": 0.0, "short": 0.0})
    for r in density:
        by_bucket[r["price_bucket"]][r["side"]] += r["notional_usd"]

    nearby = {
        bkt: v for bkt, v in by_bucket.items()
        if abs(bkt - px) <= px * (PROXIMITY_PCT / 100)
        and (v["long"] + v["short"]) >= MIN_CLUSTER_USD
    }

    if not nearby:
        print(f"    No significant cluster within {PROXIMITY_PCT}% of spot.")
        return {}

    long_usd  = sum(v["long"]  for v in nearby.values())
    short_usd = sum(v["short"] for v in nearby.values())
    total     = long_usd + short_usd
    long_pct  = long_usd / total if total else 0.5

    top_bkt   = max(nearby, key=lambda b: nearby[b]["long"] + nearby[b]["short"])
    below     = top_bkt < px

    if   long_pct > 0.6 and below:     direction, strength = "bearish", long_pct
    elif long_pct < 0.4 and not below: direction, strength = "bullish", 1 - long_pct
    else:                               direction, strength = "neutral", 0.0

    event = {
        "signal_type":  "liquidation_cascade",
        "asset":        COIN,
        "trust_tier":   1,
        "direction":    direction,
        "value_usd":    round(total, 2),
        "confidence":   round(strength, 3),
        "mark_px":      px,
        "cluster_low":  top_bkt,
        "cluster_high": top_bkt + BUCKET_SIZE,
        "long_pct":     round(long_pct * 100, 1),
        "summary": (
            f"Liquidation cluster ${total/1e6:.1f}M "
            f"{'below' if below else 'above'} spot "
            f"at ${top_bkt:,}-${top_bkt + BUCKET_SIZE:,}. "
            f"Long-heavy: {long_pct:.0%}. "
            f"Signal: {direction.upper()}."
        ),
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }

    print(f"    Direction:  {direction.upper()}")
    print(f"    Confidence: {strength:.1%}")
    print(f"    Cluster:    ${top_bkt:,}-${top_bkt + BUCKET_SIZE:,} "
          f"({'below' if below else 'above'} spot)")
    print(f"    Notional:   ${total/1e6:.2f}M  (long {long_pct:.0%} / "
          f"short {1-long_pct:.0%})")
    print(f"    Summary:    {event['summary']}")
    return event


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Faro liquidation density pipeline — standalone runner"
    )
    parser.add_argument(
        "--db",
        default="liquidations.db",
        help="Path to liquidations.db written by stream.py (default: liquidations.db)"
    )
    args = parser.parse_args()

    db_path    = Path(args.db)
    output_db  = db_path.parent / "liquidation_density.db"
    summary_f  = db_path.parent / "density_summary.txt"
    signal_f   = db_path.parent / "signal_event.json"

    print("=" * 60)
    print("  Faro — Liquidation Density Pipeline")
    print(f"  Asset: {COIN}  |  Bucket: ${BUCKET_SIZE}  |  "
          f"Lookback: {LOOKBACK_HOURS}h")
    print("=" * 60)

    # Step 1: freshness
    count = check_freshness(db_path)

    # Step 2: build
    density = build_density(db_path)

    # Step 3: validate
    total, long_total, short_total = validate(density)

    # Step 4: write store
    write_store(density, output_db)

    # Step 5: fetch mark price and print heatmap
    print(f"\n    Fetching current mark price from Hyperliquid...")
    try:
        px = mark_px()
    except Exception as e:
        print(f"    Warning: could not fetch mark price ({e}). Using midpoint estimate.")
        all_buckets = [r["price_bucket"] for r in density]
        px = float(sum(all_buckets) / len(all_buckets))

    print_heatmap(density, px)

    # Step 6: signal event
    event = publish_signal(density, px)

    # Write outputs
    with open(signal_f, "w") as f:
        json.dump(event, f, indent=2)

    summary_lines = [
        "Faro Liquidation Density — Run Summary",
        f"Asset: {COIN}",
        f"Run at: {datetime.now(tz=timezone.utc).isoformat()}",
        f"Source DB: {db_path}",
        f"Events captured: {count}",
        f"Density rows: {len(density)}",
        f"Total notional: ${total/1e6:.2f}M",
        f"Long liquidated: ${long_total/1e6:.2f}M ({long_total/total*100:.1f}%)",
        f"Short liquidated: ${short_total/1e6:.2f}M ({short_total/total*100:.1f}%)",
        f"Mark price at run: ${px:,.0f}",
        f"Signal: {event.get('direction', 'n/a').upper()} "
        f"(confidence {event.get('confidence', 0):.1%})",
        f"Summary: {event.get('summary', 'No cluster within proximity threshold')}",
    ]
    with open(summary_f, "w") as f:
        f.write("\n".join(summary_lines))

    print(f"\n{'=' * 60}")
    print(f"  Output files:")
    print(f"    {output_db}")
    print(f"    {signal_f}")
    print(f"    {summary_f}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
