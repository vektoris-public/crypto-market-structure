#!/usr/bin/env python3


"""
CoinGecko Pro — Full Universe Ingest (all tokens from /coins/list)

What it does
- Fetches the full token universe from /coins/list
- For each token (optionally limited with --limit):
  - GET /coins/{id}
  - GET /coins/{id}/market_chart/range
  - GET /coins/{id}/ohlc/range
- Writes batch parquet files under --data-dir:
  - coins/
  - market_chart/
  - ohlc/
- Writes:
  - metadata.json (checkpoint for resume)
  - failed_tokens.json (audit trail of failures/unavailable OHLC)
  - data_fetch.log

API key
- Read from environment variable (default: COINGECKO_API_KEY)
- Do NOT hardcode your key into the script.

Smoke test (10 tokens)
  export COINGECKO_API_KEY="CG-xxxx..."
  python3 coingecko_universe_ingest.py \
    --data-dir ../data/coingecko \
    --limit 10 \
    --batch-size 10 \
    --workers 2 \
    --rpm 20 \
    --days 30

Full run
  python3 coingecko_universe_ingest.py --data-dir ../data/coingecko
"""

from __future__ import annotations

import os
import json
import time
import math
import logging
import argparse
import threading
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


# -----------------------------
# Utilities
# -----------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def mkdirp(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def read_json(path: str, default: Any) -> Any:
    """Read JSON from path, or return default if file doesn't exist."""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def write_json(path: str, obj: Any) -> None:
    """Atomic JSON write to avoid corrupting files on crash."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


# -----------------------------
# Global rate limiter (RPM)
# -----------------------------

class RateLimiterRPM:
    """
    Shared global limiter: enforces <= rpm requests per minute across threads.

    Implementation:
    - schedules a monotonic "next allowed time"
    - each request waits its turn
    """
    def __init__(self, rpm: int):
        self.rpm = max(1, int(rpm))
        self.min_interval = 60.0 / float(self.rpm)
        self._lock = threading.Lock()
        self._next_time = time.monotonic()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next_time:
                time.sleep(self._next_time - now)
                now = time.monotonic()
            self._next_time = now + self.min_interval


# -----------------------------
# Flattening / schema helpers
# -----------------------------

NUMERIC_COLUMNS = {
    # coin flatten
    "price_usd", "price_btc",
    "mcap_to_tvl_ratio", "fdv_to_tvl_ratio", "roi",
    "ath_usd", "ath_change_percentage_usd",
    "market_cap_usd", "fully_diluted_valuation_usd",
    "market_cap_fdv_ratio", "total_volume_usd",
    "price_change_percentage_7d", "price_change_percentage_14d",
    "price_change_percentage_30d", "price_change_percentage_60d",
    "price_change_percentage_200d", "price_change_percentage_1y",
    "total_supply", "max_supply", "circulating_supply",
    "facebook_likes", "twitter_followers",
    "reddit_average_posts_48h", "reddit_average_comments_48h",
    "reddit_subscribers", "reddit_accounts_active_48h",
    "telegram_channel_user_count",
    "forks", "stars", "subscribers",
    "total_issues", "closed_issues",
    "pull_requests_merged", "pull_request_contributors",
    "commit_count_4_weeks",
    "total_value_locked_usd", "total_value_locked_btc",
    # market_chart
    "price", "total_volume", "market_cap",
    # ohlc
    "open", "high", "low", "close",
}

def _get_nested(d: Dict[str, Any], path: List[str]) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
        if cur is None:
            return None
    return cur

def flatten_coin_data(coin: Dict[str, Any], token_id: str) -> Dict[str, Any]:
    """
    Flatten a /coins/{id} payload into a single row.
    token_id is included for reliable joins.
    """
    flat: Dict[str, Any] = {"token_id": token_id}

    top_level_fields = [
        "id", "symbol", "name", "asset_platform_id", "block_time_in_minutes",
        "hashing_algorithm", "country_origin", "genesis_date",
        "sentiment_votes_up_percentage", "sentiment_votes_down_percentage",
        "watchlist_portfolio_users", "market_cap_rank",
    ]
    for field in top_level_fields:
        flat[field] = coin.get(field)

    flat["web_slug"] = coin.get("id")

    market_data = coin.get("market_data") or {}
    nested_market_fields = {
        "price_usd": ["current_price", "usd"],
        "price_btc": ["current_price", "btc"],
        "mcap_to_tvl_ratio": ["mcap_to_tvl_ratio"],
        "fdv_to_tvl_ratio": ["fdv_to_tvl_ratio"],
        "roi": ["roi"],
        "ath_usd": ["ath", "usd"],
        "ath_change_percentage_usd": ["ath_change_percentage", "usd"],
        "ath_date_usd": ["ath_date", "usd"],
        "atl_usd": ["atl", "usd"],
        "atl_change_percentage_usd": ["atl_change_percentage", "usd"],
        "atl_date_usd": ["atl_date", "usd"],
        "market_cap_usd": ["market_cap", "usd"],
        "market_cap_rank": ["market_cap_rank"],
        "fully_diluted_valuation_usd": ["fully_diluted_valuation", "usd"],
        "market_cap_fdv_ratio": ["market_cap_/_fully_diluted_valuation"],
        "total_volume_usd": ["total_volume", "usd"],
        "price_change_percentage_7d": ["price_change_percentage_7d"],
        "price_change_percentage_14d": ["price_change_percentage_14d"],
        "price_change_percentage_30d": ["price_change_percentage_30d"],
        "price_change_percentage_60d": ["price_change_percentage_60d"],
        "price_change_percentage_200d": ["price_change_percentage_200d"],
        "price_change_percentage_1y": ["price_change_percentage_1y"],
        "total_supply": ["total_supply"],
        "max_supply": ["max_supply"],
        "circulating_supply": ["circulating_supply"],
        "last_updated": ["last_updated"],
    }

    for new_field, path in nested_market_fields.items():
        if len(path) > 1:
            val = _get_nested(market_data, path)
        else:
            val = market_data.get(path[0])
        if val == "-":
            val = None
        flat[new_field] = val

    tvl = market_data.get("total_value_locked")
    if isinstance(tvl, dict):
        flat["total_value_locked_usd"] = tvl.get("usd")
        flat["total_value_locked_btc"] = tvl.get("btc")
    else:
        flat["total_value_locked_usd"] = None
        flat["total_value_locked_btc"] = None

    community = coin.get("community_data") or {}
    for field in [
        "facebook_likes", "twitter_followers", "reddit_average_posts_48h",
        "reddit_average_comments_48h", "reddit_subscribers",
        "reddit_accounts_active_48h", "telegram_channel_user_count"
    ]:
        val = community.get(field)
        if val == "-":
            val = None
        flat[field] = val

    developer = coin.get("developer_data") or {}
    for field in [
        "forks", "stars", "subscribers", "total_issues", "closed_issues",
        "pull_requests_merged", "pull_request_contributors", "commit_count_4_weeks"
    ]:
        val = developer.get(field)
        if val == "-":
            val = None
        flat[field] = val

    return flat

def convert_known_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col in NUMERIC_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# -----------------------------
# Client
# -----------------------------

@dataclass
class Config:
    base_url: str
    api_key: str
    data_dir: str
    days: int
    batch_size: int
    workers: int
    rpm: int
    max_retries: int
    timeout_s: int
    resume: bool
    limit: Optional[int]


class CoinGeckoClient:
    def __init__(self, cfg: Config, limiter: RateLimiterRPM, logger: logging.Logger):
        self.cfg = cfg
        self.limiter = limiter
        self.log = logger

        self.session = requests.Session()

        retry = Retry(
            total=cfg.max_retries,
            connect=cfg.max_retries,
            read=cfg.max_retries,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=max(10, cfg.workers * 2),
            pool_maxsize=max(20, cfg.workers * 4),
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.headers = {
            "Accept": "application/json",
            "x-cg-pro-api-key": cfg.api_key,
        }

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        self.limiter.acquire()
        try:
            r = self.session.get(url, headers=self.headers, params=params, timeout=self.cfg.timeout_s)
        except requests.RequestException as e:
            self.log.warning("Request exception for %s: %s", url, e)
            return None

        # OHLC may return 422 for some tokens/time ranges
        if r.status_code == 422 and "/ohlc/" in url:
            return None

        if r.status_code >= 400:
            self.log.warning("HTTP %s for %s (params=%s). Body: %s", r.status_code, url, params, r.text[:300])
            return None

        try:
            return r.json()
        except ValueError:
            self.log.warning("Non-JSON response for %s: %s", url, r.text[:300])
            return None

    def list_all_coins(self) -> List[Dict[str, Any]]:
        url = f"{self.cfg.base_url}/coins/list"
        data = self.get_json(url)
        return data if isinstance(data, list) else []

    def fetch_coin(self, token_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.cfg.base_url}/coins/{token_id}"
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "true",
            "developer_data": "true",
            "sparkline": "false",
        }
        data = self.get_json(url, params=params)
        return data if isinstance(data, dict) else None

    def fetch_market_chart_range(self, token_id: str, start_ts: int, end_ts: int) -> Optional[Dict[str, Any]]:
        url = f"{self.cfg.base_url}/coins/{token_id}/market_chart/range"
        params = {
            "vs_currency": "usd",
            "from": start_ts,
            "to": end_ts,
            "interval": "daily",
            "precision": "8",
        }
        data = self.get_json(url, params=params)
        return data if isinstance(data, dict) else None

    def fetch_ohlc_range(self, token_id: str, start_ts: int, end_ts: int) -> Optional[List[Any]]:
        url = f"{self.cfg.base_url}/coins/{token_id}/ohlc/range"
        params = {
            "vs_currency": "usd",
            "from": start_ts,
            "to": end_ts,
            "interval": "daily",
        }
        data = self.get_json(url, params=params)
        return data if isinstance(data, list) else None


# -----------------------------
# Persistence
# -----------------------------

def parquet_write(rows: List[Dict[str, Any]], out_path: str, logger: logging.Logger) -> None:
    if not rows:
        logger.info("No rows to write: %s", out_path)
        return
    df = pd.DataFrame(rows)
    df = convert_known_numeric(df)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path)
    logger.info("Wrote %d rows -> %s", len(df), out_path)

def load_failed(path: str) -> Dict[str, Any]:
    return read_json(path, {"failed_tokens": []})

def append_failed(path: str, token_id: str, reason: str) -> None:
    obj = load_failed(path)
    obj.setdefault("failed_tokens", []).append({
        "token_id": token_id,
        "reason": reason,
        "timestamp": iso_utc(utc_now()),
    })
    write_json(path, obj)


# -----------------------------
# Worker logic
# -----------------------------

def process_one_token(
    client: CoinGeckoClient,
    token_id: str,
    start_ts: int,
    end_ts: int,
    failed_path: str,
) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[List[Any]]]:
    coin = client.fetch_coin(token_id)
    if coin is None:
        append_failed(failed_path, token_id, "coin_fetch_failed")

    mc = client.fetch_market_chart_range(token_id, start_ts, end_ts)
    if mc is None:
        append_failed(failed_path, token_id, "market_chart_fetch_failed")

    ohlc = client.fetch_ohlc_range(token_id, start_ts, end_ts)
    if ohlc is None:
        append_failed(failed_path, token_id, "ohlc_unavailable_or_failed")

    return token_id, coin, mc, ohlc


# -----------------------------
# Logging
# -----------------------------

def build_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("coingecko_universe_ingest")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="Output directory (creates coins/, market_chart/, ohlc/)")
    ap.add_argument("--base-url", default="https://pro-api.coingecko.com/api/v3")
    ap.add_argument("--api-key-env", default="COINGECKO_API_KEY", help="Env var containing CoinGecko Pro API key")
    ap.add_argument("--days", type=int, default=180, help="Lookback window (days)")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--rpm", type=int, default=60, help="Global requests-per-minute limit across all threads")
    ap.add_argument("--max-retries", type=int, default=5)
    ap.add_argument("--timeout-s", type=int, default=30)
    ap.add_argument("--no-resume", action="store_true", help="Disable resuming from metadata.json")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of tokens processed (for testing)")
    args = ap.parse_args()

    api_key = os.environ.get(args.api_key_env, "").strip()
    if not api_key:
        print(f"Missing API key. Set env var {args.api_key_env}=YOUR_KEY")
        return 2

    cfg = Config(
        base_url=args.base_url,
        api_key=api_key,
        data_dir=args.data_dir,
        days=args.days,
        batch_size=args.batch_size,
        workers=args.workers,
        rpm=args.rpm,
        max_retries=args.max_retries,
        timeout_s=args.timeout_s,
        resume=(not args.no_resume),
        limit=args.limit,
    )

    mkdirp(cfg.data_dir)
    for sub in ["coins", "market_chart", "ohlc"]:
        mkdirp(os.path.join(cfg.data_dir, sub))

    log_path = os.path.join(cfg.data_dir, "data_fetch.log")
    metadata_path = os.path.join(cfg.data_dir, "metadata.json")
    failed_path = os.path.join(cfg.data_dir, "failed_tokens.json")

    logger = build_logger(log_path)
    limiter = RateLimiterRPM(cfg.rpm)
    client = CoinGeckoClient(cfg, limiter, logger)

    end_dt = utc_now()
    start_dt = end_dt - timedelta(days=cfg.days)
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    logger.info("Window: %s -> %s (days=%d)", start_dt.date(), end_dt.date(), cfg.days)

    meta_default = {"schema_version": 2, "last_processed_batch": -1, "updated_at": None}
    meta = read_json(metadata_path, meta_default)
    last_done = int(meta.get("last_processed_batch", -1)) if cfg.resume else -1

    coins = client.list_all_coins()
    if not coins:
        logger.error("Failed to fetch /coins/list")
        return 1

    token_ids = [c.get("id") for c in coins if isinstance(c, dict) and c.get("id")]

    if cfg.limit is not None:
        if cfg.limit < 0:
            logger.error("--limit must be >= 0")
            return 2
        token_ids = token_ids[: cfg.limit]
        logger.info("Token limit enabled: %d tokens", len(token_ids))
    else:
        logger.info("Total tokens: %d", len(token_ids))

    total_batches = int(math.ceil(len(token_ids) / cfg.batch_size)) if token_ids else 0
    logger.info("Batch size=%d => total batches=%d", cfg.batch_size, total_batches)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    for batch_idx in range(total_batches):
        if batch_idx <= last_done:
            continue

        lo = batch_idx * cfg.batch_size
        hi = min(len(token_ids), lo + cfg.batch_size)
        batch_tokens = token_ids[lo:hi]
        logger.info(
            "Batch %d/%d: tokens [%d:%d) count=%d",
            batch_idx + 1, total_batches, lo, hi, len(batch_tokens)
        )

        coins_rows: List[Dict[str, Any]] = []
        mc_rows: List[Dict[str, Any]] = []
        ohlc_rows: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
            futures = [
                ex.submit(process_one_token, client, tid, start_ts, end_ts, failed_path)
                for tid in batch_tokens
            ]

            for fut in tqdm(as_completed(futures), total=len(futures), desc=f"batch {batch_idx}", leave=False):
                try:
                    tid, coin, mc, ohlc = fut.result()
                except Exception as e:
                    logger.exception("Worker exception: %s", e)
                    continue

                if coin:
                    coins_rows.append(flatten_coin_data(coin, tid))

                if mc and isinstance(mc.get("prices"), list):
                    prices = mc.get("prices", [])
                    vols = mc.get("total_volumes", [])
                    mcaps = mc.get("market_caps", [])

                    for i, pr in enumerate(prices):
                        # pr = [timestamp_ms, price]
                        ts_ms = pr[0] if isinstance(pr, list) and len(pr) > 0 else None
                        price = pr[1] if isinstance(pr, list) and len(pr) > 1 else None

                        vol = None
                        if i < len(vols) and isinstance(vols[i], list) and len(vols[i]) > 1:
                            vol = vols[i][1]

                        mcap = None
                        if i < len(mcaps) and isinstance(mcaps[i], list) and len(mcaps[i]) > 1:
                            mcap = mcaps[i][1]

                        mc_rows.append({
                            "token_id": tid,
                            "timestamp": ts_ms,
                            "price": price,
                            "total_volume": vol,
                            "market_cap": mcap,
                        })

                if ohlc:
                    for rec in ohlc:
                        # rec = [timestamp_ms, open, high, low, close]
                        if isinstance(rec, list) and len(rec) == 5:
                            ohlc_rows.append({
                                "token_id": tid,
                                "timestamp": rec[0],
                                "open": rec[1],
                                "high": rec[2],
                                "low": rec[3],
                                "close": rec[4],
                            })

        stamp = utc_now().strftime("%Y%m%d%H%M%S")
        coins_path = os.path.join(cfg.data_dir, "coins", f"batch_{batch_idx:05d}_{stamp}.parquet")
        mc_path = os.path.join(cfg.data_dir, "market_chart", f"batch_{batch_idx:05d}_{stamp}.parquet")
        ohlc_path = os.path.join(cfg.data_dir, "ohlc", f"batch_{batch_idx:05d}_{stamp}.parquet")

        parquet_write(coins_rows, coins_path, logger)
        parquet_write(mc_rows, mc_path, logger)
        parquet_write(ohlc_rows, ohlc_path, logger)

        meta["last_processed_batch"] = batch_idx
        meta["updated_at"] = iso_utc(utc_now())
        write_json(metadata_path, meta)

        logger.info("Finished batch %d. Checkpoint saved.", batch_idx)

    logger.info("All done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
