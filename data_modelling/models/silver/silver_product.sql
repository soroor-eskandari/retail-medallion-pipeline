{{ config(unique_key='product_ean') }}

-- One row per product. Names arrive with mojibake, umlaut spellings and dropped
-- letters, so each attribute is the most frequent cleaned value across all
-- events for that EAN. Events whose EAN couldn't be repaired are left out.

with events as (
    select * from {{ ref('stg_pos_events') }}
    where product_ean is not null
)

{% if is_incremental() %}
, changed_products as (
    select distinct product_ean
    from events
    where update_ts > (select coalesce(max(bronze_updated_at), '-infinity') from {{ this }})
)
{% endif %}

select
    product_ean,
    mode() within group (order by product_name) as product_name,
    mode() within group (order by brand)        as brand,
    mode() within group (order by category)     as category,
    mode() within group (order by vat_rate)     as vat_rate,
    mode() within group (order by unit)         as unit,
    max(update_ts)                              as bronze_updated_at,
    now()                                       as insert_ts,
    now()                                       as dbt_updated_at
from events
{% if is_incremental() %}
where product_ean in (select product_ean from changed_products)
{% endif %}
group by product_ean
