-- stg_payments.sql
with source as (
    select * from {{ source('stg_raw', 'payments') }}
)

select
    payment_id,
    from_account_id,
    to_account_id,
    round(amount, 2)       as amount,
    upper(currency)        as currency,
    payment_type,
    channel_id,
    status,
    payment_timestamp,
    payment_size_bucket
from source
where amount between {{ var('min_valid_amount') }} and {{ var('max_valid_amount') }}
