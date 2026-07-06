-- stg_customers.sql
-- Thin, 1:1 staging view over the raw customers staging table.
-- Renaming/casting only - no business logic here (that lives downstream).

with source as (
    select * from {{ source('stg_raw', 'customers') }}
)

select
    customer_id,
    trim(full_name)         as full_name,
    lower(trim(email))      as email,
    phone,
    date_of_birth,
    coalesce(gender, 'Unknown') as gender,
    city,
    state,
    segment,
    kyc_status,
    age,
    age_group
from source
