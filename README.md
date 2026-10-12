# Retail POS data (simulated Kafka dump)

## Setup
Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync                      # create .venv and install dependencies (incl. dev: pandas, Jupyter)
docker compose up -d         # start Postgres (credentials in .env)
uv run load_bronze.py        # upsert the CSV into bronze.pos_transactions_raw (safe to re-run)
```

`.env` needs `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB`.

## Data
`pos_transactions_raw.csv` is 49,496 rows × 36 columns, one row per receipt line, covering Sept 2026 across 18 German grocery stores of four store banners.
Regenerate with `uv run generate_kafka_data.py --rows 50000 --seed 42` (standard library only).

The generator reads its stores, products, payment variants and Kafka topic names from `reference/*.csv` (`stores.csv`, `products.csv`, `payment_variants.csv`, `settings.csv`). These files and the generated data are not committed.

## Columns
| Group | Columns |
|---|---|
| Kafka metadata | kafka_topic, kafka_partition (0-5), kafka_offset, kafka_timestamp (epoch ms), kafka_key (store_id), kafka_headers (JSON) |
| Event | event_id, event_type (SALE / RETURN / VOID / heartbeat junk), schema_version (1 or 2), event_time |
| Receipt | receipt_id, line_number, pos_terminal_id, cashier_id, payment_method, loyalty_card_id (v2 only), is_training_mode |
| Store | store_id, store_name, banner, city, postal_code, bundesland |
| Product | product_ean, product_name, brand, category |
| Amounts | quantity, unit, unit_price, discount_amount, line_total, vat_rate, currency (v2 only) |
| Ingestion | raw_payload (JSON, only when flattening failed), ingested_at (UTC) |

## Mess included
- **Duplicates**: the same event_id redelivered with the same partition+offset (consumer replays) or with a new offset (producer retries), about 3.6% of rows.
- **Timestamps**: event_time in 7 formats (ISO Z, ISO +02:00, `dd.mm.yyyy`, epoch ms, `yyyy/mm/dd`, naive UTC, US AM/PM); late events up to 3 days behind kafka_timestamp; a few future timestamps from clock skew; file not sorted by offset or event time.
- **Numbers**: decimal commas, `EUR` / `€` suffixes, padded spaces, quantities like `2 Stk`, kg weights, VAT as `7`, `7%`, `0.07`, `7,0`; negative, zero and 9999.99 prices; line_total off by ×10/×100 in ~1% of rows.
- **Text**: casing variants (SALE/Sale/sale), store name typos (dropped letters, odd casing, a legal-entity suffix), city spellings (Koeln, KÃ¶ln, Cologne), mojibake and dropped letters in product names, EANs in scientific notation or truncated, store_id like `r1001`.
- **Nulls**: mixed null tokens (`""`, NULL, null, N/A, None, -, nan).
- **Schema evolution**: v2 (rolled out mid-month) adds currency and loyalty_card_id; v1 rows have them empty.
- **Junk events**: HEARTBEAT/TEST rows from test stores, training-mode transactions, a `.retry` topic name.
- **Failed flattening**: ~1% of rows have only event_id plus raw_payload JSON, some of it truncated.
