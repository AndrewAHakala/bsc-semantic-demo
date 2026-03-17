{% macro normalize_text(column_name) %}
    {#-
    Snowflake-compatible text normalization for fuzzy search matching.
    Strips accents (via TRANSLATE), lowercases, removes punctuation,
    and collapses whitespace.
    -#}
    regexp_replace(
        regexp_replace(
            lower(
                translate(
                    {{ column_name }},
                    'ÀÁÂÃÄÅÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÝàáâãäåèéêëìíîïòóôõöùúûüý',
                    'AAAAAAEEEEIIIIOOOOOUUUUYaaaaaaeeeeiiiioooooouuuuy'
                )
            ),
            '[^a-z0-9\\s]', ' '
        ),
        '\\s+', ' '
    )
{% endmacro %}
