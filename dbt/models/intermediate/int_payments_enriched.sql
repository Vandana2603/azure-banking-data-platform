-- int_payments_enriched.sql
with payments as (
    select * from {{ ref('stg_payments') }}
)
select
    *,
    cast(format(payment_timestamp, 'yyyyMMdd') as int) as date_key
from payments
