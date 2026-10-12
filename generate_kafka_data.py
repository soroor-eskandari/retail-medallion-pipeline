"""
Generate a deliberately messy CSV of retail POS transactions,
as if consumed from a Kafka topic and dumped to a file (one row per receipt line).

Stores, products, payment variants and topic names are read from reference/*.csv
(kept out of git), so the generator itself holds no company-specific data.

Usage:  uv run generate_kafka_data.py [--rows 50000] [--seed 42] [--out pos_transactions_raw.csv]
Only uses the standard library.
"""
import argparse
import csv
import json
import random
import uuid
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path

REFERENCE_DIR = Path(__file__).parent / "reference"

Store = namedtuple("Store", "store_id banner store_name city postal_code bundesland "
                            "weight open_sunday self_checkout name_token name_variants")


def read_reference(name):
    with open(REFERENCE_DIR / name, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def as_bool(s):
    return s.strip().lower() == "true"


# ---------------------------------------------------------------- reference data
STORES = [
    Store(r["store_id"], r["banner"], r["store_name"], r["city"], r["postal_code"], r["bundesland"],
          int(r["weight"]), as_bool(r["open_sunday"]), as_bool(r["self_checkout"]),
          r["name_token"], r["name_variants"].split("|"))
    for r in read_reference("stores.csv")
]

# ean, name, brand, category, base_price, vat, sold_by_weight
PRODUCTS = [
    (r["ean"], r["name"], r["brand"], r["category"], float(r["base_price"]), int(r["vat_rate"]),
     as_bool(r["sold_by_weight"]))
    for r in read_reference("products.csv")
]

PAYMENT_CANONICAL = ["girocard", "cash", "credit_card", "mobile"]
PAYMENT_VARIANTS = {k: [] for k in PAYMENT_CANONICAL}
for r in read_reference("payment_variants.csv"):
    PAYMENT_VARIANTS[r["payment_method"]].append(r["variant"])

SETTINGS = {r["key"]: r["value"] for r in read_reference("settings.csv")}
TOPIC = SETTINGS["kafka_topic"]
RETRY_TOPIC = SETTINGS["kafka_retry_topic"]

CITY_TYPOS = {
    "Köln": ["Koeln", "Koln", "KÃ¶ln", "Cologne", "köln", "KÖLN"],
    "München": ["Muenchen", "Munchen", "MÃ¼nchen", "Munich"],
    "Düsseldorf": ["Duesseldorf", "Dusseldorf", "DÃ¼sseldorf"],
    "Hürth": ["Huerth", "HÃ¼rth"],
    "Frankfurt am Main": ["Frankfurt", "Frankfurt a.M.", "Frankfurt/Main"],
    "Berlin": ["berlin", "BERLIN", "Berlin "],
    "Hamburg": ["hamburg", "HH"],
}
NULL_TOKENS = ["", "NULL", "null", "N/A", "None", "-", "nan"]

N_PARTITIONS = 6


def chance(p):
    return random.random() < p


def null_token():
    return random.choice(NULL_TOKENS)


# ---------------------------------------------------------------- formatting dirt
def fmt_event_time(dt):
    """Same instant, many formats (producers on different POS firmware)."""
    r = random.random()
    if r < 0.50:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if r < 0.65:
        return dt.strftime("%Y-%m-%dT%H:%M:%S%z")[:-2] + ":" + dt.strftime("%z")[-2:]  # +02:00
    if r < 0.78:
        return dt.strftime("%d.%m.%Y %H:%M:%S")  # German local time, no tz
    if r < 0.86:
        return str(int(dt.timestamp() * 1000))  # epoch millis
    if r < 0.92:
        return dt.strftime("%Y/%m/%d %H:%M")  # local, no seconds
    if r < 0.97:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # UTC, no marker
    return dt.strftime("%m/%d/%Y %I:%M %p")  # US style, ambiguous


def fmt_money(x):
    r = random.random()
    if r < 0.55:
        return f"{x:.2f}"
    if r < 0.80:
        return f"{x:.2f}".replace(".", ",")
    if r < 0.88:
        return f"{x:.2f} EUR"
    if r < 0.94:
        return "€ " + f"{x:.2f}".replace(".", ",")
    if r < 0.98:
        return f"{x:.4f}"
    return f"  {x:.2f} "


def fmt_qty(q, by_weight):
    if by_weight:
        s = f"{q:.3f}"
        return s.replace(".", ",") if chance(0.5) else s
    r = random.random()
    if r < 0.75:
        return str(q)
    if r < 0.88:
        return f"{q}.0"
    if r < 0.95:
        return f"{q} Stk"
    return f"{q},00"


def fmt_ean(ean):
    r = random.random()
    if r < 0.93:
        return ean
    if r < 0.96:
        return f"{int(ean):.5E}".replace("E+", "E+")  # Excel scientific notation
    if r < 0.98:
        return ean.lstrip("0") or ean
    return ean[:-1]  # truncated, invalid length


def dirty_text(s, p_case=0.08, p_space=0.05):
    if chance(p_case):
        s = random.choice([s.upper(), s.lower(), s.title()])
    if chance(p_space):
        s = random.choice([" " + s, s + " ", s + "  "])
    return s


def dirty_store_name(store):
    r = random.random()
    if r < 0.85:
        return dirty_text(store.store_name)
    return store.store_name.replace(store.name_token, random.choice(store.name_variants))


def dirty_product_name(name):
    r = random.random()
    if r < 0.88:
        return dirty_text(name, 0.05, 0.03)
    if r < 0.93:
        return name.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    if r < 0.97:
        return name.encode("utf-8").decode("latin-1")  # mojibake
    i = random.randrange(1, len(name) - 1)
    return name[:i] + name[i + 1:]  # dropped character typo


# ---------------------------------------------------------------- main generator
def build_receipts(target_rows, start, end):
    receipts = []
    rows = 0
    span = (end - start).total_seconds()
    store_weights = [s.weight for s in STORES]
    while rows < target_rows:
        store = random.choices(STORES, weights=store_weights)[0]
        # opening hours 07-22, busier around 17-19 and on Saturdays
        day = start + timedelta(seconds=random.random() * span)
        hour = int(min(21, max(7, random.gauss(15.5, 3.5))))
        ts = day.replace(hour=hour, minute=random.randrange(60), second=random.randrange(60),
                         microsecond=random.randrange(1000) * 1000)
        if ts.weekday() == 6 and not store.open_sunday:  # Sunday closed in DE
            ts -= timedelta(days=1)
        n_lines = max(1, min(25, int(random.expovariate(1 / 6))))
        receipts.append((store, ts, n_lines))
        rows += n_lines
    return receipts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=50000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="pos_transactions_raw.csv")
    args = ap.parse_args()
    random.seed(args.seed)

    berlin = timezone(timedelta(hours=2))  # CEST; deliberately naive about DST
    start = datetime(2026, 9, 1, tzinfo=berlin)
    end = datetime(2026, 10, 1, tzinfo=berlin)

    events = []
    for store, ts, n_lines in build_receipts(int(args.rows * 0.95), start, end):
        store_id, banner, city, plz, land = store.store_id, store.banner, store.city, store.postal_code, store.bundesland
        receipt_id = f"{store_id}-{ts:%Y%m%d}-{random.randrange(1, 99999):05d}"
        terminal = f"{store_id}-K{random.randint(1, 8):02d}"
        cashier = f"C{random.randint(100, 999)}" if not store.self_checkout or chance(0.5) else "SCO"
        payment = random.choices(PAYMENT_CANONICAL, weights=[45, 30, 12, 13])[0]
        loyalty = f"{random.randrange(10**15, 10**16)}" if chance(0.35) else None
        schema_version = "1" if ts.day < 15 or chance(0.1) else "2"  # v2 rollout mid-month
        is_return = chance(0.02)
        is_void = chance(0.01)
        # Kafka producer lag: usually ms-seconds, sometimes POS offline -> hours/days late
        lag = random.uniform(0.05, 4)
        if chance(0.03):
            lag = random.uniform(3600, 3 * 86400)
        kafka_ts_base = ts + timedelta(seconds=lag)
        displayed_city = random.choice(CITY_TYPOS[city]) if city in CITY_TYPOS and chance(0.12) else city

        for line_no in range(1, n_lines + 1):
            ean, pname, brand, cat, price, vat, by_weight = random.choice(PRODUCTS)
            # price drift + promotions
            unit_price = round(price * random.choice([1, 1, 1, 1, 0.9, 1.1]), 2)
            qty = round(random.uniform(0.2, 2.5), 3) if by_weight else random.choices(
                [1, 2, 3, 4, 6, 12], weights=[70, 15, 6, 4, 3, 2])[0]
            discount = round(unit_price * qty * random.choice([0.1, 0.2, 0.3]), 2) if chance(0.12) else 0.0
            total = round(unit_price * qty - discount, 2)
            event_type = "SALE"
            if is_return:
                event_type = "RETURN"
                qty, total = -qty, -total
            elif is_void and line_no == n_lines:
                event_type = "VOID"

            # bad prices / totals
            if chance(0.004):
                unit_price = random.choice([0.0, -unit_price, 9999.99, 99999.0])
            if chance(0.01):
                total = round(total * random.choice([10, 0.1, 100]), 2)  # misplaced decimal

            event_time = ts + timedelta(seconds=line_no * random.uniform(1, 4))
            kafka_ts = kafka_ts_base + timedelta(milliseconds=line_no * random.randint(1, 40))
            if chance(0.002):
                event_time = event_time + timedelta(days=random.choice([365, 30, 1]))  # clock skew -> future

            ev = {
                "event_id": str(uuid.UUID(int=random.getrandbits(128), version=4)),
                "event_type": dirty_text(event_type, 0.06, 0.0),
                "schema_version": schema_version,
                "event_time": fmt_event_time(event_time),
                "receipt_id": receipt_id,
                "line_number": str(line_no),
                "store_id": store_id if not chance(0.03) else store_id.lower().replace("-", ""),
                "store_name": dirty_store_name(store),
                "banner": dirty_text(banner, 0.1, 0.02),
                "city": displayed_city,
                "postal_code": plz if not chance(0.03) else plz.lstrip("0") or plz,
                "bundesland": land if chance(0.95) else null_token(),
                "pos_terminal_id": terminal,
                "cashier_id": cashier if chance(0.97) else null_token(),
                "product_ean": fmt_ean(ean),
                "product_name": dirty_product_name(pname),
                "brand": brand if chance(0.93) else null_token(),
                "category": dirty_text(cat, 0.05, 0.02) if chance(0.95) else null_token(),
                "quantity": fmt_qty(qty, by_weight),
                "unit": ("kg" if by_weight else random.choice(["ST", "Stk", "pcs", "st"])) if chance(0.96) else null_token(),
                "unit_price": fmt_money(unit_price) if chance(0.985) else null_token(),
                "discount_amount": fmt_money(discount) if discount or chance(0.5) else null_token(),
                "line_total": fmt_money(total) if chance(0.98) else null_token(),
                "vat_rate": random.choice([f"{vat}", f"{vat}%", f"0.{vat:02d}", f"{vat},0"]),
                "currency": ("EUR" if chance(0.9) else random.choice(["eur", "€", "Euro"])) if schema_version == "2" else null_token(),
                "payment_method": random.choice(PAYMENT_VARIANTS[payment]) if chance(0.97) else null_token(),
                "loyalty_card_id": (loyalty or "") if schema_version == "2" else "",
                "is_training_mode": "false" if chance(0.995) else "true",
            }
            events.append({"_kafka_ts": kafka_ts, "_key": store_id, "ev": ev})

    # heartbeat / test events that leaked into the topic
    for _ in range(int(args.rows * 0.004)):
        t = start + timedelta(seconds=random.random() * (end - start).total_seconds())
        ev = {k: "" for k in events[0]["ev"]}
        ev.update({"event_id": str(uuid.UUID(int=random.getrandbits(128), version=4)),
                   "event_type": random.choice(["HEARTBEAT", "heartbeat", "TEST"]),
                   "schema_version": "2", "event_time": fmt_event_time(t),
                   "store_id": random.choice(["TEST-0000", "R-9999", ""]),
                   "pos_terminal_id": "TEST-K00", "is_training_mode": "true"})
        events.append({"_kafka_ts": t, "_key": ev["store_id"], "ev": ev})

    # assign partitions by key, offsets by kafka timestamp (per partition)
    events.sort(key=lambda e: e["_kafka_ts"])
    next_offset = {p: random.randint(1_000_000, 5_000_000) for p in range(N_PARTITIONS)}
    out = []
    for e in events:
        p = sum(ord(c) for c in e["_key"]) % N_PARTITIONS
        e["partition"] = p
        e["offset"] = next_offset[p]
        next_offset[p] += 1
        out.append(e)

    # at-least-once delivery: consumer restarts replay a batch -> duplicates
    # (same payload, same offset) and producer retries (same payload, new offset)
    dupes = []
    for e in random.sample(out, int(len(out) * 0.025)):
        d = dict(e)
        if chance(0.6):  # producer retry: new offset, slightly later kafka ts
            d["offset"] = next_offset[e["partition"]]
            next_offset[e["partition"]] += 1
            d["_kafka_ts"] = e["_kafka_ts"] + timedelta(seconds=random.uniform(1, 600))
        dupes.append(d)
    # replay blocks: a consumer rebalance re-reads ~200 contiguous offsets
    for _ in range(3):
        i = random.randrange(len(out) - 300)
        dupes.extend(out[i:i + 200])
    out.extend(dupes)

    # dump order: roughly by consumption time, but partitions interleaved and
    # a few rows shuffled -> rows are not sorted by event_time or offset
    out.sort(key=lambda e: e["_kafka_ts"] + timedelta(seconds=random.uniform(0, 30)))

    cols = ["kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "kafka_key",
            "kafka_headers"] + list(events[0]["ev"].keys()) + ["raw_payload", "ingested_at"]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for e in out:
            ev = dict(e["ev"])
            consumed = e["_kafka_ts"] + timedelta(seconds=random.uniform(0.5, 120))
            headers = {"producer": f"pos-gateway-{random.randint(1, 4)}",
                       "content-type": "application/json",
                       "schema-version": ev["schema_version"]}
            raw = ""
            # ~1% of messages failed flattening upstream: only raw JSON kept
            if chance(0.01) and ev["store_id"]:
                payload = {k: v for k, v in ev.items() if v not in ("",)}
                raw = json.dumps(payload, ensure_ascii=False)
                if chance(0.3):
                    raw = raw[: random.randint(20, len(raw) - 5)]  # truncated message
                ev = {k: (v if k == "event_id" else "") for k, v in ev.items()}
            w.writerow([
                TOPIC if chance(0.995) else RETRY_TOPIC,
                e["partition"],
                e["offset"],
                int(e["_kafka_ts"].timestamp() * 1000),
                e["_key"],
                json.dumps(headers),
                *ev.values(),
                raw,
                consumed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
            ])
    print(f"wrote {len(out)} rows to {args.out}")


if __name__ == "__main__":
    main()
