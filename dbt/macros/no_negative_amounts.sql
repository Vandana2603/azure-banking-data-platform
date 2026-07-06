{% test no_negative_amounts(model, column_name) %}
-- Custom generic test: fails if any row has a negative amount.
-- Usage in a schema.yml file:
--   tests:
--     - no_negative_amounts:
--         column_name: amount

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
