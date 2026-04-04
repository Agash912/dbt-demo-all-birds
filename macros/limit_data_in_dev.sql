-- Limits data to the last N days in dev environments.
-- In production, returns a passthrough condition so all data is processed.
-- Saves compute during development by scanning less data.
--
-- Usage:
--   select * from {{ ref('stg_orders') }}
--   where {{ limit_data_in_dev('ordered_at', days=7) }}

{% macro limit_data_in_dev(column_name, days=7) %}
    {% if target.name == 'dev' or target.name == 'default' %}
        {{ column_name }} >= dateadd(day, -{{ days }}, current_date)
    {% else %}
        true
    {% endif %}
{% endmacro %}
