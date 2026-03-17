{% macro search_blob(fields) %}
    {#-
    Concatenates a list of column references into a single lowercased
    search blob for fallback token matching.
    -#}
    lower(
        {% for field in fields %}
        coalesce({{ field }}, '')
        {%- if not loop.last %} || ' ' || {% endif %}
        {% endfor %}
    )
{% endmacro %}
