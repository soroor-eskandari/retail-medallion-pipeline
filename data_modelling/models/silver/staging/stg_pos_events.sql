{{ config(materialized='ephemeral') }}

-- Shared cleaning for all silver tables: one row per real POS event (receipt
-- line), typed and deduplicated. Ephemeral: dbt inlines it as a CTE into the
-- models that ref() it, so no table or view is created.

{% set event_columns = [
    'event_id', 'event_type', 'schema_version', 'event_time', 'receipt_id', 'line_number',
    'store_id', 'store_name', 'banner', 'city', 'postal_code', 'bundesland',
    'pos_terminal_id', 'cashier_id', 'product_ean', 'product_name', 'brand', 'category',
    'quantity', 'unit', 'unit_price', 'discount_amount', 'line_total', 'vat_rate',
    'currency', 'payment_method', 'loyalty_card_id', 'is_training_mode'
] %}

with raw_rows as (
    select
        *,
        -- ~1% of messages failed flattening upstream: only event_id + raw JSON kept.
        -- Truncated JSON can't be parsed; those rows are lost.
        case
            when nullif(btrim(raw_payload), '') is not null and pg_input_is_valid(raw_payload, 'jsonb')
                then raw_payload::jsonb
        end as payload
    from {{ source('bronze', 'pos_transactions_raw') }}
),

recovered as (
    select
        {% for c in event_columns -%}
        case when payload is not null then payload ->> '{{ c }}' else {{ c }} end as {{ c }},
        {% endfor -%}
        payload is not null as is_recovered_from_payload,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        ingested_at,
        update_ts
    from raw_rows
),

cleaned as (
    select
        {{ clean_text('event_id') }}                              as event_id,
        upper({{ clean_text('event_type') }})                     as event_type,
        {{ clean_text('schema_version') }}::int                   as schema_version,
        {{ clean_text('event_time') }}                            as event_time_text,
        {{ clean_text('receipt_id') }}                            as receipt_id,
        {{ clean_text('line_number') }}::int                      as line_number,
        {{ clean_store_id('store_id') }}                          as store_id,
        {{ clean_text('store_name') }}                            as store_name,
        {{ clean_text('banner') }}                                as banner,
        {{ clean_text('city') }}                                  as city,
        lpad(nullif(regexp_replace({{ clean_text('postal_code') }}, '[^0-9]', '', 'g'), ''), 5, '0') as postal_code,
        upper({{ clean_text('bundesland') }})                     as bundesland,
        upper({{ clean_text('pos_terminal_id') }})                as pos_terminal_id,
        upper({{ clean_text('cashier_id') }})                     as cashier_id,
        upper({{ clean_text('product_ean') }})                    as product_ean_text,
        {{ clean_text('product_name') }}                          as product_name,
        {{ clean_text('brand') }}                                 as brand,
        {{ clean_text('category') }}                              as category,
        {{ parse_decimal('quantity') }}                           as quantity,
        case
            when lower({{ clean_text('unit') }}) = 'kg' then 'kg'
            when lower({{ clean_text('unit') }}) in ('st', 'stk', 'pcs') then 'piece'
        end                                                       as unit,
        {{ parse_decimal('unit_price') }}                         as unit_price,
        {{ parse_decimal('discount_amount') }}                    as discount_amount,
        {{ parse_decimal('line_total') }}                         as line_total,
        {{ parse_decimal('vat_rate') }}                           as vat_rate_number,
        -- every store is in Germany; v1 events have no currency field
        case
            when lower({{ clean_text('currency') }}) in ('eur', '€', 'euro') or {{ clean_text('currency') }} is null then 'EUR'
            else upper({{ clean_text('currency') }})
        end                                                       as currency,
        lower({{ clean_text('payment_method') }})                 as payment_method_text,
        {{ clean_text('loyalty_card_id') }}                       as loyalty_card_id,
        lower({{ clean_text('is_training_mode') }}) = 'true'      as is_training_mode,
        is_recovered_from_payload,
        btrim(kafka_topic)                                        as kafka_topic,
        kafka_partition::int                                      as kafka_partition,
        kafka_offset::bigint                                      as kafka_offset,
        to_timestamp(kafka_timestamp::bigint / 1000.0)            as kafka_timestamp,
        ingested_at::timestamp at time zone 'UTC'                 as ingested_at,
        update_ts
    from recovered
),

-- real sales only: drops heartbeat/test events, test stores, training mode and
-- rows that can't be placed on a receipt
events as (
    select *
    from cleaned
    where event_type in ('SALE', 'RETURN', 'VOID')
      and is_training_mode is not true
      and store_id ~ '^[A-Z]-[0-9]{4}$'
      and event_id is not null
      and receipt_id is not null
      and line_number is not null
),

-- the same event can arrive more than once (producer retries); keep the first delivery
deduplicated as (
    select distinct on (event_id) *
    from events
    order by event_id, is_recovered_from_payload, kafka_timestamp, kafka_offset
),

-- EAN repair: truncated (12 digits) or Excel scientific notation (4.38884E+12).
-- Candidates are the valid 13-digit EANs that fit; if several fit, the product
-- name decides.
valid_eans as (
    select distinct product_ean_text as product_ean, lower(product_name) as name_key
    from deduplicated
    where product_ean_text ~ '^\d{13}$'
),

ean_candidates as (
    select
        d.event_id,
        v.product_ean,
        bool_or(v.name_key = lower(d.product_name)) as name_matches
    from deduplicated d
    join valid_eans v
      on (d.product_ean_text ~ '^\d{12}$' and left(v.product_ean, 12) = d.product_ean_text)
      or (d.product_ean_text ~ '^\d\.\d+E\+\d+$'
          and round(v.product_ean::numeric,
                    length(split_part(split_part(d.product_ean_text, 'E', 1), '.', 2))
                    - split_part(d.product_ean_text, 'E+', 2)::int)
              = d.product_ean_text::numeric)
    group by d.event_id, v.product_ean
),

repaired_eans as (
    select event_id, product_ean
    from (
        select
            *,
            count(*) over (partition by event_id)                             as n_candidates,
            count(*) filter (where name_matches) over (partition by event_id) as n_name_matches
        from ean_candidates
    ) c
    where n_candidates = 1 or (name_matches and n_name_matches = 1)
),

typed as (
    select
        d.*,
        {{ parse_event_time('d.event_time_text') }} as event_time_reported,
        coalesce(case when d.product_ean_text ~ '^\d{13}$' then d.product_ean_text end, r.product_ean) as product_ean,
        -- VAT arrives as 7, 7%, 0.07 or 7,0
        round(case when d.vat_rate_number < 1 then d.vat_rate_number * 100 else d.vat_rate_number end) as vat_rate,
        p.payment_method
    from deduplicated d
    left join repaired_eans r on r.event_id = d.event_id
    left join {{ ref('payment_method_mapping') }} p on p.variant = d.payment_method_text
)

select
    event_id,
    event_type,
    schema_version,
    -- clock skew: an event can't happen after Kafka received it; fall back to the Kafka time
    case
        when event_time_reported is null or event_time_reported > kafka_timestamp + interval '1 hour'
            then kafka_timestamp
        else event_time_reported
    end                                                                   as event_time,
    event_time_reported is null
        or event_time_reported > kafka_timestamp + interval '1 hour'     as is_event_time_corrected,
    receipt_id,
    line_number,
    store_id,
    store_name,
    banner,
    city,
    postal_code,
    bundesland,
    pos_terminal_id,
    cashier_id,
    product_ean,
    product_name,
    brand,
    category,
    quantity,
    unit,
    unit_price,
    discount_amount,
    line_total,
    vat_rate,
    currency,
    payment_method,
    loyalty_card_id,
    is_recovered_from_payload,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at,
    update_ts
from typed
