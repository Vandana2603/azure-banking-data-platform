-- dim_account.sql
select
    row_number() over (order by account_id) as account_sk,
    account_id, customer_id, branch_id, product_id, account_status,
    opened_date, currency, account_tenure_days
from {{ ref('int_account_customer_enriched') }}
