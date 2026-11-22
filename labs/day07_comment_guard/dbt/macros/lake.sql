{#- Parquet path for a model: <data>/lake/<layer>/<name>.parquet -#}
{% macro lake(layer, name) -%}
    {{ var('data_dir') }}/lake/{{ layer }}/{{ name }}.parquet
{%- endmacro %}

{#- Raw landing zone (the "read-memories" pattern: read every JSONL line with its filename). -#}
{% macro landing_glob() -%}
    {{ var('data_dir') }}/landing/**/*.jsonl
{%- endmacro %}
