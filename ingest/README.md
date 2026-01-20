# CoinGecko Pro — Full Universe Ingest

This script builds a full historical crypto asset universe using the 
CoinGecko API : https://docs.coingecko.com/

It retrieves all tokens listed by CoinGecko and collects, for each token:
- metadata and market information
- historical price, volume, and market capitalization data
- daily OHLC candles when available

Data is written in batch-oriented Parquet files for downstream analysis.

---

## What the script does

1. Fetches the complete token list from `/coins/list`
2. For each token:
   - calls `/coins/{id}` for metadata and market data
   - calls `/coins/{id}/market_chart/range` for historical time series
   - calls `/coins/{id}/ohlc/range` for OHLC candles
3. Processes tokens in batches with global rate limiting
4. Persists normalized datasets to disk
5. Maintains checkpoints and failure logs to allow resumable execution

The script is designed for long-running, full-universe ingestion jobs.

---

## Requirements

- Python 3.9+
- CoinGecko **Pro** API key (Analyst Plan at least)
- Python packages:
  - `requests`
  - `pandas`
  - `pyarrow`
  - `tqdm`
  - `urllib3`

Example setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install requests pandas pyarrow tqdm urllib3
````

---

## API key configuration

The CoinGecko Pro API key must be provided via an environment variable.

By default, the script reads:

```bash
COINGECKO_API_KEY
```

Example:

```bash
export COINGECKO_API_KEY="CG-xxxxxxxxxxxxxxxx"
```

A different variable name can be specified using `--api-key-env`.

---

## Output structure

Given an output directory specified by `--data-dir`, the script creates:

```
<data-dir>/
├── coins/
├── market_chart/
├── ohlc/
├── metadata.json
├── failed_tokens.json
└── data_fetch.log
```

### Datasets

* `coins/`
  Token-level metadata and market attributes
  (one row per token per batch)

* `market_chart/`
  Historical time series:

  * price
  * total volume
  * market capitalization

* `ohlc/`
  Daily OHLC candles when available

### Auxiliary files

* `metadata.json`
  Tracks the last completed batch to allow resuming

* `failed_tokens.json`
  Records tokens for which one or more endpoints failed or were unavailable

* `data_fetch.log`
  Execution log

---

## Usage (Terminal)

### Minimal run

```bash
python3 coingecko_universe_ingest.py --data-dir ../data/coingecko
```

This:

* processes all tokens
* uses default parameters
* resumes automatically if interrupted

---

### Smoke test (recommended before full run)

```bash
export COINGECKO_API_KEY="CG-xxxxxxxxxxxxxxxx"

python3 coingecko_universe_ingest.py \
  --data-dir ../data/coingecko \
  --limit 10 \
  --batch-size 10 \
  --workers 2 \
  --rpm 20 \
  --days 30
```

---

### Common options

* `--days N`
  Lookback window in days (default: `180`)

* `--batch-size N`
  Number of tokens processed per batch (default: `500`)

* `--workers N`
  Number of concurrent threads (default: `5`)

* `--rpm N`
  Global requests-per-minute limit across all threads (default: `60`)

* `--limit N`
  Process only the first `N` tokens (testing/debugging)

* `--no-resume`
  Disable resuming from `metadata.json`

* `--api-key-env VAR`
  Environment variable name for the API key

---

## Operational notes

* The script enforces a single global rate limit shared across threads.
* Transient HTTP errors are retried automatically.
* OHLC data may be unavailable for some tokens or time ranges.
* Missing or failed endpoints do not stop execution and are logged explicitly.

---

## License

MIT License.

```

