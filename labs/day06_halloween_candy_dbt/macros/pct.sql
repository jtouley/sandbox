{% macro pct(numerator, denominator, precision=1) -%}
    round(100.0 * ({{ numerator }}) / nullif({{ denominator }}, 0), {{ precision }})
{%- endmacro %}
