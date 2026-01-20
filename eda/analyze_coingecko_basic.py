#!/usr/bin/env python3
"""
Basic EDA for CoinGecko full-universe ingest output.

Input:
  --data-dir <root>  (contains coins/, market_chart/, ohlc/)

Output (written to --out-dir):
  - report.md                         (human-readable summary)
  - coverage_by_token.csv             (rows per token, date ranges)
  - latest_snapshot_by_token.csv      (latest price/volume/mcap per token from market_chart)
  - coins_profile.csv                 (selected coin-level fields + missingness flags)

Goal:
  Minimal, useful sanity analysis (not feature engineering).

Usage (from crypto/ingest/):
  python3 analyze_coingecko_basic.py \
    --data-dir ../data/coingecko_full \
    --out-dir  ../data/coingecko_full_eda
"""

import os
import argparse
from datetime import datetime, timezone
import numpy as np
import pandas as pd


def mkdirp(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def ms_to_dt_utc(ms: pd.Series) -> pd.Series:
    # ms epoch -> timezone-aware UTC datetime
    return pd.to_datetime(ms, unit="ms", utc=True)


def safe_read_all_parquet(folder: str, columns=None) -> pd.DataFrame:
    """
    Reads all parquet files in folder into one dataframe.
    Uses pandas' parquet reader (pyarrow engine).
    """
    files = sorted([os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".parquet")])
    if not files:
        return pd.DataFrame()
    dfs = []
    for fp in files:
        dfs.append(pd.read_parquet(fp, columns=columns))
    return pd.concat(dfs, ignore_index=True)


def quantiles(s: pd.Series, qs=(0.01, 0.05, 0.5, 0.95, 0.99)) -> dict:
    s = pd.to_numeric(s, errors="coerce")
    s = s.replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return {f"q{int(q*100):02d}": np.nan for q in qs} | {"count": 0}
    out = {f"q{int(q*100):02d}": float(s.quantile(q)) for q in qs}
    out["count"] = int(s.shape[0])
    return out


def write_report(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="Root ingest dir (coins/, market_chart/, ohlc/)")
    ap.add_argument("--out-dir", required=True, help="Output dir for EDA artifacts")
    ap.add_argument("--max-tokens-sample", type=int, default=2000,
                    help="Max tokens to use for heavy operations (latest snapshot join).")
    args = ap.parse_args()

    data_dir = args.data_dir
    out_dir = args.out_dir
    mkdirp(out_dir)

    coins_dir = os.path.join(data_dir, "coins")
    mc_dir = os.path.join(data_dir, "market_chart")
    ohlc_dir = os.path.join(data_dir, "ohlc")

    # --- Load (light columns first) ---
    coins_cols = [
        "token_id", "symbol", "name", "asset_platform_id", "genesis_date",
        "market_cap_rank", "price_usd", "market_cap_usd", "total_volume_usd",
        "circulating_supply", "total_supply", "max_supply",
        "twitter_followers", "forks", "stars", "commit_count_4_weeks",
        "total_value_locked_usd", "last_updated",
    ]
    mc_cols = ["token_id", "timestamp", "price", "total_volume", "market_cap"]
    ohlc_cols = ["token_id", "timestamp", "open", "high", "low", "close"]

    print("[load] coins…")
    coins = safe_read_all_parquet(coins_dir, columns=None)  # keep all; coins isn't huge
    if coins.empty:
        print("No coins data found.")
        return 2

    # Ensure token_id string
    coins["token_id"] = coins["token_id"].astype(str)

    # Light profile for website / audit
    coins_profile = coins[[c for c in coins_cols if c in coins.columns]].copy()
    for col in ["price_usd", "market_cap_usd", "total_volume_usd", "circulating_supply", "total_supply", "max_supply"]:
        if col in coins_profile.columns:
            coins_profile[col] = pd.to_numeric(coins_profile[col], errors="coerce")

    print("[load] market_chart (light cols)…")
    mc = safe_read_all_parquet(mc_dir, columns=mc_cols)
    if mc.empty:
        print("No market_chart data found.")
        return 2

    mc["token_id"] = mc["token_id"].astype(str)
    mc["dt"] = ms_to_dt_utc(mc["timestamp"])
    mc["date"] = mc["dt"].dt.date

    print("[load] ohlc (light cols)…")
    ohlc = safe_read_all_parquet(ohlc_dir, columns=ohlc_cols)
    if ohlc.empty:
        print("No ohlc data found.")
        return 2

    ohlc["token_id"] = ohlc["token_id"].astype(str)
    ohlc["dt"] = ms_to_dt_utc(ohlc["timestamp"])
    ohlc["date"] = ohlc["dt"].dt.date

    # --- Coverage by token (market_chart) ---
    print("[eda] coverage_by_token…")
    cov = (
        mc.groupby("token_id", as_index=False)
          .agg(
              mc_rows=("timestamp", "size"),
              mc_days=("date", "nunique"),
              mc_min_date=("date", "min"),
              mc_max_date=("date", "max"),
              mc_missing_price=("price", lambda s: int(pd.to_numeric(s, errors="coerce").isna().sum())),
          )
    )

    # --- Coverage by token (ohlc) ---
    cov_ohlc = (
        ohlc.groupby("token_id", as_index=False)
            .agg(
                ohlc_rows=("timestamp", "size"),
                ohlc_days=("date", "nunique"),
                ohlc_min_date=("date", "min"),
                ohlc_max_date=("date", "max"),
                ohlc_missing_close=("close", lambda s: int(pd.to_numeric(s, errors="coerce").isna().sum())),
            )
    )

    coverage = cov.merge(cov_ohlc, on="token_id", how="outer")
    coverage = coverage.merge(coins[["token_id"]], on="token_id", how="right")  # keep all coin ids
    coverage = coverage.fillna({
        "mc_rows": 0, "mc_days": 0, "mc_missing_price": 0,
        "ohlc_rows": 0, "ohlc_days": 0, "ohlc_missing_close": 0
    })

    coverage["has_mc"] = coverage["mc_rows"] > 0
    coverage["has_ohlc"] = coverage["ohlc_rows"] > 0

    coverage_path = os.path.join(out_dir, "coverage_by_token.csv")
    coverage.to_csv(coverage_path, index=False)

    # --- Latest snapshot (market_chart) ---
    print("[eda] latest snapshot…")
    # Reduce to a sample for very large joins if needed
    tokens_all = coins["token_id"].unique()
    token_sample = tokens_all[: min(len(tokens_all), args.max_tokens_sample)]

    mc_s = mc[mc["token_id"].isin(token_sample)].copy()

    # Latest row per token by timestamp
    mc_s.sort_values(["token_id", "timestamp"], inplace=True)
    latest = mc_s.groupby("token_id", as_index=False).tail(1)
    latest = latest[["token_id", "dt", "price", "total_volume", "market_cap"]].copy()
    for col in ["price", "total_volume", "market_cap"]:
        latest[col] = pd.to_numeric(latest[col], errors="coerce")

    latest.rename(columns={"dt": "latest_dt_utc"}, inplace=True)

    latest_path = os.path.join(out_dir, "latest_snapshot_by_token.csv")
    latest.to_csv(latest_path, index=False)

    # --- Coins profile ---
    coins_profile_path = os.path.join(out_dir, "coins_profile.csv")
    coins_profile.to_csv(coins_profile_path, index=False)

    # --- Aggregate summaries ---
    print("[eda] aggregate summaries…")
    n_tokens = int(coins["token_id"].nunique())
    n_mc_tokens = int(mc["token_id"].nunique())
    n_ohlc_tokens = int(ohlc["token_id"].nunique())

    mc_min_dt = mc["dt"].min()
    mc_max_dt = mc["dt"].max()
    ohlc_min_dt = ohlc["dt"].min()
    ohlc_max_dt = ohlc["dt"].max()

    # Coverage distribution
    mc_days_q = quantiles(coverage["mc_days"])
    ohlc_days_q = quantiles(coverage["ohlc_days"])

    # Latest snapshot distributions (sample-based)
    price_q = quantiles(latest["price"])
    vol_q = quantiles(latest["total_volume"])
    mcap_q = quantiles(latest["market_cap"])

    # Missingness in coins table (selected columns)
    missing_cols = [c for c in ["price_usd", "market_cap_usd", "total_volume_usd", "circulating_supply", "max_supply", "twitter_followers", "stars"] if c in coins_profile.columns]
    miss = {c: int(pd.to_numeric(coins_profile[c], errors="coerce").isna().sum()) for c in missing_cols}

    # Top tokens by market cap from /coins/{id} snapshot (not time-series)
    top_by_mcap = []
    if "market_cap_usd" in coins_profile.columns:
        tmp = coins_profile[["token_id", "symbol", "name", "market_cap_usd"]].copy()
        tmp["market_cap_usd"] = pd.to_numeric(tmp["market_cap_usd"], errors="coerce")
        top_by_mcap = tmp.sort_values("market_cap_usd", ascending=False).head(20).to_dict("records")

    # --- Write report.md ---
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    report_lines = []
    report_lines.append(f"# CoinGecko Universe — Basic EDA\n")
    report_lines.append(f"- Generated at: **{now}**\n")
    report_lines.append(f"- Data dir: `{data_dir}`\n")
    report_lines.append(f"- Output dir: `{out_dir}`\n")

    report_lines.append("\n## Dataset size\n")
    report_lines.append(f"- Tokens (coins/): **{n_tokens:,}**\n")
    report_lines.append(f"- Tokens with market_chart: **{n_mc_tokens:,}**\n")
    report_lines.append(f"- Tokens with ohlc: **{n_ohlc_tokens:,}**\n")
    report_lines.append(f"- market_chart rows: **{len(mc):,}**\n")
    report_lines.append(f"- ohlc rows: **{len(ohlc):,}**\n")

    report_lines.append("\n## Time ranges (UTC)\n")
    report_lines.append(f"- market_chart: **{mc_min_dt}** → **{mc_max_dt}**\n")
    report_lines.append(f"- ohlc: **{ohlc_min_dt}** → **{ohlc_max_dt}**\n")

    report_lines.append("\n## Coverage distribution (days per token)\n")
    report_lines.append("### market_chart\n")
    report_lines.append(f"- count: {mc_days_q['count']:,}\n")
    report_lines.append(f"- q01/q05/q50/q95/q99: {mc_days_q['q01']:.0f} / {mc_days_q['q05']:.0f} / {mc_days_q['q50']:.0f} / {mc_days_q['q95']:.0f} / {mc_days_q['q99']:.0f}\n")
    report_lines.append("### ohlc\n")
    report_lines.append(f"- count: {ohlc_days_q['count']:,}\n")
    report_lines.append(f"- q01/q05/q50/q95/q99: {ohlc_days_q['q01']:.0f} / {ohlc_days_q['q05']:.0f} / {ohlc_days_q['q50']:.0f} / {ohlc_days_q['q95']:.0f} / {ohlc_days_q['q99']:.0f}\n")

    report_lines.append("\n## Latest snapshot (market_chart) distributions (sample-based)\n")
    report_lines.append(f"- Tokens in snapshot: **{len(latest):,}** (max-tokens-sample={args.max_tokens_sample})\n")
    report_lines.append("### price (USD)\n")
    report_lines.append(f"- q01/q05/q50/q95/q99: {price_q['q01']:.6g} / {price_q['q05']:.6g} / {price_q['q50']:.6g} / {price_q['q95']:.6g} / {price_q['q99']:.6g}\n")
    report_lines.append("### total_volume (USD)\n")
    report_lines.append(f"- q01/q05/q50/q95/q99: {vol_q['q01']:.6g} / {vol_q['q05']:.6g} / {vol_q['q50']:.6g} / {vol_q['q95']:.6g} / {vol_q['q99']:.6g}\n")
    report_lines.append("### market_cap (USD)\n")
    report_lines.append(f"- q01/q05/q50/q95/q99: {mcap_q['q01']:.6g} / {mcap_q['q05']:.6g} / {mcap_q['q50']:.6g} / {mcap_q['q95']:.6g} / {mcap_q['q99']:.6g}\n")

    report_lines.append("\n## Missingness (coins/ selected fields)\n")
    for k, v in miss.items():
        report_lines.append(f"- {k}: **{v:,}** missing\n")

    report_lines.append("\n## Top 20 by market_cap_usd (from /coins/{id} snapshot)\n")
    if top_by_mcap:
        for r in top_by_mcap:
            mc_val = r["market_cap_usd"]
            report_lines.append(f"- {r['token_id']} ({r.get('symbol','')}) — {mc_val:.6g}\n")
    else:
        report_lines.append("- market_cap_usd not available in coins profile.\n")

    report_lines.append("\n## Artifacts\n")
    report_lines.append(f"- `coverage_by_token.csv`\n")
    report_lines.append(f"- `latest_snapshot_by_token.csv`\n")
    report_lines.append(f"- `coins_profile.csv`\n")

    report_path = os.path.join(out_dir, "report.md")
    write_report(report_path, "".join(report_lines))

    print("\n[done] Wrote:")
    print(" -", report_path)
    print(" -", coverage_path)
    print(" -", latest_path)
    print(" -", coins_profile_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
