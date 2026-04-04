-- Custom generic test: checks that every non-null value in a column is > 0.
-- Catches negative prices, negative revenue, or zero-quantity line items.
--
-- Usage in YAML:
--   columns:
--     - name: order_total
--       data_tests:
--         - is_positive_value

{% test is_positive_value(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} <= 0

{% endtest %}
