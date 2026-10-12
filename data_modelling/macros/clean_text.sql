-- Trim, collapse inner whitespace and turn the source's null tokens
-- ("", NULL, null, N/A, None, -, nan) into real NULLs.
{% macro clean_text(col) -%}
    case
        when lower(btrim({{ col }})) in ('', 'null', 'n/a', 'none', '-', 'nan') then null
        else regexp_replace(btrim({{ col }}), '\s+', ' ', 'g')
    end
{%- endmacro %}
