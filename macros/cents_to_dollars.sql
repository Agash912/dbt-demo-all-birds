-- Converts a column stored in cents to dollars.
-- Source systems often store monetary values as integers (e.g., 1500 = $15.00).
-- This keeps the conversion consistent across all models.

{% macro cents_to_dollars(column_name) -%}
    {{ return(adapter.dispatch('cents_to_dollars')(column_name)) }}
{%- endmacro %}

{% macro default__cents_to_dollars(column_name) -%}
    ({{ column_name }} / 100)::numeric(16, 2)
{%- endmacro %}
