-- Text amount to numeric: handles decimal commas, "EUR"/"€" suffixes, "2 Stk",
-- padding and null tokens. Anything that still isn't a number becomes NULL.
{% macro parse_decimal(col) -%}
    case
        when replace(regexp_replace({{ clean_text(col) }}, '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            then replace(regexp_replace({{ clean_text(col) }}, '[^0-9,.-]', '', 'g'), ',', '.')::numeric
    end
{%- endmacro %}
