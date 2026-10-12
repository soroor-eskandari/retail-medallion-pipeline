-- Use the custom schema name as-is (e.g. "silver") instead of dbt's default
-- "<target_schema>_silver", so layers map 1:1 to Postgres schemas.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
