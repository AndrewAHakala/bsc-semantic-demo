{% macro normalize_text(column_name) %}
    {#-
    Snowflake-compatible text normalization for fuzzy search matching.
    Lowercases, strips non-alphanumeric characters, and collapses whitespace.
    -#}
    trim(
        regexp_replace(
            regexp_replace(
                lower({{ column_name }}),
                '[^a-z0-9\\s]', ' '
            ),
            '\\s+', ' '
        )
    )
{% endmacro %}
