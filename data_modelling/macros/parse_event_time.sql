-- The POS firmware sends event_time in 7 formats; formats without an offset
-- are German local time, except "yyyy-mm-dd hh:mi:ss" which is UTC.
-- Expects an already cleaned text column.
{% macro parse_event_time(col) -%}
    case
        when {{ col }} ~ '^\d{13}$'
            then to_timestamp({{ col }}::bigint / 1000.0)
        when {{ col }} ~ '^\d{4}-\d{2}-\d{2}T'
            then {{ col }}::timestamptz
        when {{ col }} ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            then {{ col }}::timestamp at time zone 'UTC'
        when {{ col }} ~ '^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$'
            then to_timestamp({{ col }}, 'DD.MM.YYYY HH24:MI:SS')::timestamp at time zone 'Europe/Berlin'
        when {{ col }} ~ '^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$'
            then to_timestamp({{ col }}, 'YYYY/MM/DD HH24:MI')::timestamp at time zone 'Europe/Berlin'
        when {{ col }} ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2} (AM|PM)$'
            then to_timestamp({{ col }}, 'MM/DD/YYYY HH12:MI AM')::timestamp at time zone 'Europe/Berlin'
    end
{%- endmacro %}
