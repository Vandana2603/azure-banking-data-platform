-- fact_payments.sql
select
    row_number() over (order by payment_id) as payment_sk,
    p.payment_id, p.from_account_id, p.to_account_id, p.date_key,
    p.amount, p.currency, p.payment_type, p.channel_id, p.status,
    p.payment_timestamp, p.payment_size_bucket
from {{ ref('int_payments_enriched') }} p
where p.status = 'SUCCESS'
