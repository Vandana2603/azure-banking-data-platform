-- stg_savings_transactions.sql
with source as (
    select * from {{ source('stg_raw', 'savings_transactions') }}
)

select
    transaction_id,
    account_id,
    transaction_type,
    transaction_direction,
    round(amount, 2)   as amount,
    channel_id,
    transaction_timestamp
from source
