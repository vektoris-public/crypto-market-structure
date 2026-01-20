# Crypto market structure

This repository contains scripts and datasets for analyzing
crypto market structure and observable market dynamics.

The project is organized as a research codebase, starting from
raw data ingestion and extending toward exploratory analysis
and feature engineering.

The objective is to support universe-level, reproducible analysis
of crypto assets over long time horizons.

---

## Project structure

The repository is organized in layers:

### 1. Ingestion
Data collection pipelines used to build the raw datasets.

- Full-universe asset coverage
- Token metadata, market history, and OHLC data
- Batch-oriented, rate-limited, and resumable workflows
- Columnar outputs for downstream analysis

Scripts live under the `ingest/` directory.

---

### 2. Exploratory analysis
Exploratory and descriptive analysis of market-wide behavior, including:
- cross-sectional distributions
- liquidity and volatility regimes
- market structure observations

Scripts will live under the `eda/` directory.

---

### 3. Feature engineering (planned)
Construction of reusable features for quantitative analysis, including:
- volatility and drawdown metrics
- momentum and stability indicators
- cross-asset relationships

Scripts will live under the `features/` directory.

---

## Scope and intent

This repository focuses on **research infrastructure** rather than
end-user applications or trading systems.

The code is designed to:
- favor completeness over selectivity
- minimize survivorship bias
- enable reproducible analysis

---

## License

MIT License.
