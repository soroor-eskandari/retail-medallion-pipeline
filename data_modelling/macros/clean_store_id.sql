-- Normalise store ids to the canonical "R-1001" form (source also has "r1001").
{% macro clean_store_id(col) -%}
    regexp_replace(upper({{ clean_text(col) }}), '^([A-Z])-?([0-9]{4})$', '\1-\2')
{%- endmacro %}
