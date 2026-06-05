"""
faro_liquidation_density_dag.py
Faro Head of Data — Candidate Challenge

Forward-looking liquidation density pipeline for Hyperliquid.
Transforms OI-delta inferred liquidation events into a price-bucketed
density table, validates it, and publishes a SignalEvent to the agent layer.

Architecture
------------
Lane A (persistent service, outside Airflow):
    stream.py  — subscribes to HL activeAssetCtx WebSocket (~1s ticks),
                 infers liquidations from OI-delta, writes to SQLite.

Lane B (this DAG, every 5 minutes):
    data_freshness_check
        └─► backfill_snapshot
                └─► build_density_table
                        └─► validate_density
                                └─► write_to_store
                                        ├─► publish_signal_event
                                        └─► alert_on_proximity

Output table: liquidation_density
    (coin, price_bucket, period, side, notional_usd, event_count)
    PoC: SQLite. Production target: ClickHouse or Postgres + TimescaleDB.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────

BUCKET_SIZE = {"BTC": 500, "ETH": 25}
COINS = list(BUCKET_SIZE)

LOOKBACK_HOURS   = 72
MAX_STALENESS_S  = 600       # freshness gate: 10 minutes
MIN_CLUSTER_USD  = 50_000    # minimum notional to register a cluster
PROXIMITY_PCT    = 2.0       # proximity alert threshold (% of spot price)
SANITY_BOUND_USD = {"BTC": 50_000_000_000}  # per-coin total notional sanity ceiling

HL_REST_URL = Variable.get("HL_REST_URL",        default_var="https://api.hyperliquid.xyz/info")
DB_PATH     = Path(Variable.get("LIQUIDATION_DB", default_var="liquidations.db"))
OUTPUT_DB   = Path(Variable.get("OUTPUT_DB",      default_var="liquidation_density.db"))


# ── helpers ───────────────────────────────────────────────────────────────────

def _period_label(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:00")


def _fetch_mark_prices(coins: list[str]) -> dict[str, float]:
    """Fetch mark prices for all requested coins in one API call."""
    resp = requests.post(HL_REST_URL, json={"type": "metaAndAssetCtxs"}, timeout=10)
    resp.raise_for_status()
    meta, ctxs = resp.json()
    coin_set = set(coins)
    prices: dict[str, float] = {}
    for i, asset in enumerate(meta.get("universe", [])):
        name = asset.get("name")
        if name in coin_set and i < len(ctxs):
            prices[name] = float(ctxs[i].get("markPx", 0))
    missing = coin_set - prices.keys()
    if missing:
        raise ValueError(f"Coins not found in HL universe: {missing}")
    return prices


def _mark_px(coin: str) -> float:
    """Fetch current mark price from HL REST."""
    return _fetch_mark_prices([coin])[coin]


# ── tasks ─────────────────────────────────────────────────────────────────────

def data_freshness_check(**context):
    """
    Gate: assert stream.py is alive and recent.
    Raises if newest liquidation row is older than MAX_STALENESS_S.
    Prevents stale density from being written silently.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Liquidation DB not found at {DB_PATH}. Is stream.py running?"
        )
    with sqlite3.connect(DB_PATH) as conn:
        for coin in COINS:
            row = conn.execute(
                "SELECT MAX(ts) FROM liquidations WHERE coin=?", (coin,)
            ).fetchone()
            max_ts = row[0]
            if max_ts is None:
                log.warning("%s: no rows yet — streamer may be starting up", coin)
                continue
            age = (time.time() * 1000 - max_ts) / 1000
            if age > MAX_STALENESS_S:
                raise ValueError(
                    f"{coin} data is {age:.0f}s old (threshold {MAX_STALENESS_S}s). "
                    "Check stream.py WebSocket."
                )
            log.info("Freshness OK: %s — last event %.1fs ago", coin, age)


def backfill_snapshot(**context):
    """
    Cold-start safety: if DB has <100 rows for a coin, seed one synthetic
    row from the HL REST snapshot at current mark price.
    Marks source='rest_snapshot' — lower fidelity than streamed OI-delta.
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS liquidations "
            "(tid TEXT PRIMARY KEY, coin TEXT, px REAL, sz REAL, "
            " notional REAL, side TEXT, ts INTEGER, raw TEXT)"
        )
        conn.commit()
        for coin in COINS:
            count = conn.execute(
                "SELECT COUNT(*) FROM liquidations WHERE coin=?", (coin,)
            ).fetchone()[0]
            if count >= 100:
                continue
            try:
                px = _mark_px(coin)
            except Exception as e:
                log.warning("REST snapshot failed for %s: %s", coin, e)
                continue
            ts = int(time.time() * 1000)
            # Seed a single baseline event — notional is approximate
            conn.execute(
                "INSERT OR IGNORE INTO liquidations (tid,coin,px,sz,notional,side,ts,raw) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (f"snapshot_{coin}_{ts}", coin, px, 0.01, px * 0.01, "long", ts,
                 json.dumps({"source": "rest_snapshot", "px": px}))
            )
            conn.commit()
            log.info("Seeded REST snapshot for %s at px=%.1f", coin, px)


def build_density_table(**context):
    """
    Core transform: read raw fills → bucket by (coin, price, period, side)
    → aggregate notional → push to XCom.
    """
    cutoff_ms = int((time.time() - LOOKBACK_HOURS * 3600) * 1000)
    density: dict[str, list[dict]] = {}
    with sqlite3.connect(DB_PATH) as conn:
        for coin in COINS:
            rows = conn.execute(
                "SELECT px, notional, side, ts FROM liquidations "
                "WHERE coin=? AND ts>=?",
                (coin, cutoff_ms)
            ).fetchall()

            size = BUCKET_SIZE.get(coin, 500)
            agg: dict[tuple, dict] = defaultdict(lambda: {"notional": 0.0, "count": 0})
            for px, notional, side, ts in rows:
                key = (int(float(px) / size) * size, _period_label(int(ts)), side)
                agg[key]["notional"] += float(notional)
                agg[key]["count"]    += 1

            updated_at = datetime.now(tz=timezone.utc).isoformat()
            density[coin] = [
                {
                    "coin":         coin,
                    "price_bucket": k[0],
                    "period":       k[1],
                    "side":         k[2],
                    "notional_usd": v["notional"],
                    "event_count":  v["count"],
                    "updated_at":   updated_at,
                }
                for k, v in agg.items()
            ]
            log.info("Built density: %s — %d buckets from %d events",
                     coin, len(density[coin]), len(rows))

    context["ti"].xcom_push(key="density", value=density)


def validate_density(**context):
    """
    Quality gate before write.
    Checks: non-empty, no nulls, non-negative notional, total within sane range.
    """
    density = context["ti"].xcom_pull(key="density")
    for coin, rows in density.items():
        if not rows:
            raise ValueError(f"Empty density for {coin} — check streamer and backfill")
        for r in rows:
            if r["price_bucket"] is None:
                raise ValueError("Null price_bucket")
            if r["side"] not in ("long", "short"):
                raise ValueError(f"Bad side: {r['side']}")
            if r["notional_usd"] < 0:
                raise ValueError("Negative notional")
        total = sum(r["notional_usd"] for r in rows)
        bound = SANITY_BOUND_USD.get(coin)
        if bound and total > bound:
            raise ValueError(f"{coin} total notional ${total:.0f} exceeds sanity bound")
        log.info("Validation OK: %s — %d rows, $%.2fM total", coin, len(rows), total / 1e6)


def write_to_store(**context):
    """
    Upsert density rows into liquidation_density table.
    PoC uses SQLite. Production: ClickHouse or Postgres+TimescaleDB.
    Schema is production-identical — swap the connection string only.
    """
    density = context["ti"].xcom_pull(key="density")
    all_rows = [r for rows in density.values() for r in rows]
    with sqlite3.connect(OUTPUT_DB) as conn:
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
        """, all_rows)
        conn.commit()
    log.info("Wrote %d rows to liquidation_density", len(all_rows))


def publish_signal_event(**context):
    """
    Find the nearest hot zone (within 5% of spot) and emit a SignalEvent
    to the signal-pipeline store (trust_tier=1, signal_type=liquidation_cascade).
    Makes the density actionable for the Faro AI agent's model layer.
    """
    density = context["ti"].xcom_pull(key="density")
    try:
        prices = _fetch_mark_prices(COINS)
    except Exception as e:
        log.warning("mark_px fetch failed: %s", e)
        return

    for coin in COINS:
        rows = density.get(coin, [])
        if not rows:
            continue
        px = prices[coin]

        nearby = [
            r for r in rows
            if abs(r["price_bucket"] - px) <= px * 0.05
            and r["notional_usd"] >= MIN_CLUSTER_USD
        ]
        if not nearby:
            log.info("No significant cluster within 5%% of spot for %s", coin)
            continue

        total    = sum(r["notional_usd"] for r in nearby)
        long_usd = sum(r["notional_usd"] for r in nearby if r["side"] == "long")
        long_pct = long_usd / total if total else 0.5

        top   = max(nearby, key=lambda r: r["notional_usd"])
        below = top["price_bucket"] < px

        if   long_pct > 0.6 and below: direction, strength = "bearish", long_pct
        elif long_pct < 0.4 and not below: direction, strength = "bullish", 1 - long_pct
        else:                           direction, strength = "neutral",  0.0

        event = {
            "signal_type": "liquidation_cascade",
            "asset":       coin,
            "trust_tier":  1,
            "direction":   direction,
            "value":       total,
            "confidence":  round(strength, 3),
            "summary": (
                f"Liquidation cluster ${total/1e6:.1f}M "
                f"{'below' if below else 'above'} spot at "
                f"${top['price_bucket']:,}. "
                f"Long-heavy: {long_pct:.0%}. Direction: {direction}."
            ),
        }
        log.info("SignalEvent: %s", json.dumps(event))
        # production: signal_pipeline_store.save(SignalEvent(**event))


def alert_on_proximity(**context):
    """
    Emit alert when price is within PROXIMITY_PCT of a cluster >= MIN_CLUSTER_USD.
    Production: push to Faro AI context injection queue so the agent surfaces it
    as a position-aware warning without the trader switching widgets.
    """
    density = context["ti"].xcom_pull(key="density")
    try:
        prices = _fetch_mark_prices(COINS)
    except Exception:
        return

    for coin in COINS:
        rows = density.get(coin, [])
        if not rows:
            continue
        px        = prices[coin]
        threshold = px * (PROXIMITY_PCT / 100)
        hot = [
            r for r in rows
            if r["notional_usd"] >= MIN_CLUSTER_USD
            and abs(r["price_bucket"] - px) <= threshold
        ]
        if not hot:
            continue

        total = sum(r["notional_usd"] for r in hot)
        top   = max(hot, key=lambda r: r["notional_usd"])
        size  = BUCKET_SIZE.get(coin, 500)
        log.warning(
            "PROXIMITY ALERT %s — $%.1fM cluster at $%d–$%d within %.1f%% of spot (%.1f)",
            coin, total / 1e6, top["price_bucket"], top["price_bucket"] + size,
            PROXIMITY_PCT, px,
        )
        # production: faro_context_queue.push({...})


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner":          "data-eng",
    "retries":        2,
    "retry_delay":    timedelta(minutes=1),
    "email_on_failure": True,
    "email":          ["data-alerts@faro.io"],
}

with DAG(
    dag_id="faro_liquidation_density",
    description="Forward-looking liquidation density — OI-delta inference on Hyperliquid",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["faro", "liquidation", "hyperliquid", "tier-1"],
) as dag:

    t1 = PythonOperator(task_id="data_freshness_check", python_callable=data_freshness_check)
    t2 = PythonOperator(task_id="backfill_snapshot",    python_callable=backfill_snapshot)
    t3 = PythonOperator(task_id="build_density_table",  python_callable=build_density_table)
    t4 = PythonOperator(task_id="validate_density",     python_callable=validate_density)
    t5 = PythonOperator(task_id="write_to_store",       python_callable=write_to_store)
    t6 = PythonOperator(task_id="publish_signal_event", python_callable=publish_signal_event)
    t7 = PythonOperator(task_id="alert_on_proximity",   python_callable=alert_on_proximity)

    t1 >> t2 >> t3 >> t4 >> t5 >> [t6, t7]
