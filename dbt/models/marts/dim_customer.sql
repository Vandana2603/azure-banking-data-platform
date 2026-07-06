-- dim_customer.sql
-- Surrogate key generated via row_number(); swap for dbt_utils.generate_surrogate_key
-- if the dbt_utils package is installed (see packages.yml).
select
    row_number() over (order by customer_id) as customer_sk,
    customer_id, full_name, email, phone, date_of_birth, gender,
    city, state, segment, kyc_status, age, age_group
from {{ ref('stg_customers') }}
