"""
Load the raw POS CSV into bronze.pos_transactions_raw (idempotent upsert).

Each Kafka message (topic, partition, offset) is one row. The CSV is copied into a
temp table, redeliveries of the same message are collapsed to the latest one
(by ingested_at), then upserted: new messages are inserted, changed ones updated,
unchanged ones left alone. Re-running the same file changes nothing.

Usage:  uv run load_bronze.py [--file pos_transactions_raw.csv]
Connection details come from .env (POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB,
optional POSTGRES_HOST / POSTGRES_PORT).
"""
import argparse
import csv
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql

HERE = Path(__file__).parent
DDL_FILE = HERE / "sql" / "bronze" / "create_bronze.sql"
SCHEMA, TABLE = "bronze", "pos_transactions_raw"
KEY = ["kafka_topic", "kafka_partition", "kafka_offset"]


def connect():
    load_dotenv(HERE / ".env")
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def column_list(columns, prefix=None):
    return sql.SQL(", ").join(
        sql.Identifier(prefix, c) if prefix else sql.Identifier(c) for c in columns
    )


def upsert_sql(columns):
    non_key = [c for c in columns if c not in KEY]
    return sql.SQL("""
        insert into {target} ({cols})
        select distinct on ({key}) {cols}
        from staging_load
        order by {key}, ingested_at desc nulls last
        on conflict ({key}) do update
            set {assignments}, update_ts = now()
            where ({current}) is distinct from ({incoming})
        returning (xmax = 0) as inserted
    """).format(
        target=sql.Identifier(SCHEMA, TABLE),
        cols=column_list(columns),
        key=column_list(KEY),
        assignments=sql.SQL(", ").join(
            sql.SQL("{c} = excluded.{c}").format(c=sql.Identifier(c)) for c in non_key
        ),
        current=column_list(non_key, prefix=TABLE),
        incoming=column_list(non_key, prefix="excluded"),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default=str(HERE / "pos_transactions_raw.csv"))
    args = parser.parse_args()
    csv_path = Path(args.file)

    with csv_path.open(encoding="utf-8", newline="") as f:
        columns = next(csv.reader(f))

    # one transaction: the whole file lands or nothing does
    with connect() as conn, conn.cursor() as cur:
        cur.execute(DDL_FILE.read_text())

        col_defs = sql.SQL(", ").join(sql.SQL("{} text").format(sql.Identifier(c)) for c in columns)
        cur.execute(sql.SQL("create temp table staging_load ({}) on commit drop").format(col_defs))
        copy_sql = sql.SQL("copy staging_load ({}) from stdin (format csv, header true, encoding 'UTF8')")
        with csv_path.open("rb") as f, cur.copy(copy_sql.format(column_list(columns))) as copy:
            while chunk := f.read(1024 * 1024):
                copy.write(chunk)
        read = cur.rowcount

        cur.execute(upsert_sql(columns))
        flags = [row[0] for row in cur.fetchall()]
        inserted = sum(flags)
        updated = len(flags) - inserted

        cur.execute(sql.SQL("select count(*) from {}").format(sql.Identifier(SCHEMA, TABLE)))
        total = cur.fetchone()[0]

    print(f"{csv_path.name}: read {read:,} rows -> inserted {inserted:,}, updated {updated:,}, "
          f"skipped {read - inserted - updated:,} (unchanged or redelivered); "
          f"{SCHEMA}.{TABLE} has {total:,} rows")


if __name__ == "__main__":
    main()
