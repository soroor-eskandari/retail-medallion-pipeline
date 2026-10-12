{{ config(unique_key=['receipt_id', 'line_number']) }}

-- One row per receipt line (= one POS event). Suspicious amounts are kept as
-- received and flagged, not corrected.

with events as (
    select * from {{ ref('stg_pos_events') }}
    {% if is_incremental() %}
    where update_ts > (select coalesce(max(bronze_updated_at), '-infinity') from {{ this }})
    {% endif %}
)

select
    receipt_id,
    line_number,
    event_id,
    event_type,
    event_time,
    is_event_time_corrected,
    product_ean,
    quantity,
    unit_price,
    discount_amount,
    line_total,
    -- zero, negative or placeholder prices (9999.99)
    unit_price <= 0 or unit_price >= 1000                                  as is_unit_price_suspect,
    -- line_total should be quantity x unit_price minus discount (sign follows quantity on returns)
    abs(line_total - sign(quantity) * (abs(quantity * unit_price) - coalesce(discount_amount, 0))) > 0.02
                                                                           as is_line_total_suspect,
    is_recovered_from_payload,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at,
    update_ts                                                              as bronze_updated_at,
    now()                                                                  as insert_ts,
    now()                                                                  as dbt_updated_at
from events
