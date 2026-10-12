-- Bronze layer: raw POS events exactly as received from the Kafka dump.
-- Every source column is TEXT so nothing is rejected or altered on load;
-- typing and cleaning happen in silver (dbt).
-- One row per Kafka message (topic, partition, offset): loads are upserts, so
-- re-running a load is idempotent and consumer redeliveries collapse to the
-- latest delivery. Producer retries (same event_id, new offset) are kept and
-- deduplicated in silver.

create schema if not exists bronze;

create table if not exists bronze.pos_transactions_raw (
    kafka_topic       text not null,
    kafka_partition   text not null,
    kafka_offset      text not null,
    kafka_timestamp   text,
    kafka_key         text,
    kafka_headers     text,
    event_id          text,
    event_type        text,
    schema_version    text,
    event_time        text,
    receipt_id        text,
    line_number       text,
    store_id          text,
    store_name        text,
    banner            text,
    city              text,
    postal_code       text,
    bundesland        text,
    pos_terminal_id   text,
    cashier_id        text,
    product_ean       text,
    product_name      text,
    brand             text,
    category          text,
    quantity          text,
    unit              text,
    unit_price        text,
    discount_amount   text,
    line_total        text,
    vat_rate          text,
    currency          text,
    payment_method    text,
    loyalty_card_id   text,
    is_training_mode  text,
    raw_payload       text,
    ingested_at       text,

    -- added by the pipeline
    insert_ts         timestamptz not null default now(),  -- first time this message was loaded
    update_ts         timestamptz not null default now(),  -- last time its content changed

    primary key (kafka_topic, kafka_partition, kafka_offset)
);
