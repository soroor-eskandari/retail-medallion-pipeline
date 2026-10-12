{{ config(unique_key='receipt_id') }}

-- One row per receipt. Receipt-level fields are repeated on every line (some
-- lines have them missing or in another spelling), so each is the most
-- frequent value across the receipt's lines.

with events as (
    select * from {{ ref('stg_pos_events') }}
)

{% if is_incremental() %}
-- receipts with new or changed lines are recomputed from all of their lines
, changed_receipts as (
    select distinct receipt_id
    from events
    where update_ts > (select coalesce(max(bronze_updated_at), '-infinity') from {{ this }})
)
{% endif %}

select
    receipt_id,
    mode() within group (order by store_id)        as store_id,
    mode() within group (order by pos_terminal_id) as pos_terminal_id,
    mode() within group (order by cashier_id)      as cashier_id,
    mode() within group (order by payment_method)  as payment_method,
    mode() within group (order by loyalty_card_id) as loyalty_card_id,
    mode() within group (order by currency)        as currency,
    max(schema_version)                            as schema_version,
    min(event_time)                                as receipt_time,
    count(*)                                       as line_count,
    max(update_ts)                                 as bronze_updated_at,
    now()                                          as insert_ts,
    now()                                          as dbt_updated_at
from events
{% if is_incremental() %}
where receipt_id in (select receipt_id from changed_receipts)
{% endif %}
group by receipt_id
