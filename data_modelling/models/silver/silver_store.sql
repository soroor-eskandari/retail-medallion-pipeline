{{ config(unique_key='store_id') }}

-- One row per store. Every event repeats the store's attributes, with typos,
-- casing variants and city spellings mixed in, so each attribute is the most
-- frequent cleaned value across all of the store's events.

with events as (
    select * from {{ ref('stg_pos_events') }}
)

{% if is_incremental() %}
-- stores with bronze rows loaded or changed since the last run; their attributes
-- are recomputed from their full history, not just the new rows
, changed_stores as (
    select distinct store_id
    from events
    where update_ts > (select coalesce(max(bronze_updated_at), '-infinity') from {{ this }})
)
{% endif %}

select
    store_id,
    mode() within group (order by store_name)  as store_name,
    mode() within group (order by banner)      as banner,
    mode() within group (order by city)        as city,
    mode() within group (order by postal_code) as postal_code,
    mode() within group (order by bundesland)  as bundesland,
    max(update_ts)                             as bronze_updated_at,
    now()                                      as insert_ts,
    now()                                      as dbt_updated_at
from events
{% if is_incremental() %}
where store_id in (select store_id from changed_stores)
{% endif %}
group by store_id
