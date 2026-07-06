-- fact_savings_transactions.sql
select
    row_number() over (order by transaction_id) as transaction_sk,
    transaction_id, account_id, transaction_type, transaction_direction,
    amount, channel_id, transaction_timestamp,
    cast(format(transaction_timestamp, 'yyyyMMdd') as int) as date_key
from {{ ref('stg_savings_transactions') }}
