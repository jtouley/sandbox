{#- Append one row per node per dbt invocation to meta.dbt_node_runs (inside warehouse.duckdb).
    This is the build-side half of the metadata; the capture-side half is data/meta/runs/*.json. -#}
{% macro log_run_results(results) %}
  {% if execute %}
    {% set rows = [] %}
    {% for r in results %}
      {% set rows_affected = r.adapter_response.get('rows_affected') if r.adapter_response else none %}
      {% do rows.append("('" ~ invocation_id ~ "', '" ~ run_started_at ~ "', '" ~ r.node.unique_id ~ "', '"
          ~ r.node.resource_type ~ "', '" ~ r.status ~ "', " ~ (r.execution_time or 0) ~ ", "
          ~ (rows_affected if rows_affected is not none else 'null') ~ ", "
          ~ (r.failures if r.failures is not none else 'null') ~ ")") %}
    {% endfor %}
    create schema if not exists meta;
    create table if not exists meta.dbt_node_runs (
      invocation_id varchar, run_started_at timestamptz, unique_id varchar, resource_type varchar,
      status varchar, execution_time_s double, rows_affected bigint, failures bigint
    );
    {% if rows %}
    insert into meta.dbt_node_runs values {{ rows | join(', ') }};
    {% endif %}
  {% endif %}
{% endmacro %}
