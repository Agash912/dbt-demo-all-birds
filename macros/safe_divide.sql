-- Returns null-safe division. If the denominator is zero or null, returns 0.
-- Useful for metrics like average order value or conversion rate
-- where a zero denominator would otherwise throw an error.
-- Usage: {{ safe_divide('revenue', 'order_count') }}

{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then 0
        else round({{ numerator }}::numeric(16, 4) / {{ denominator }}::numeric(16, 4), 2)
    end
{% endmacro %}
