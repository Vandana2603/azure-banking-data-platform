-- stg_accounts.sql
with source as (
    select * from {{ source('stg_raw', 'accounts') }}
)

select
    account_id,
    customer_id,
    branch_id,
    product_id,
    account_status,
    opened_date,
    upper(currency)        as currency,
    account_tenure_days
from source
