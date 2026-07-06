-- int_account_customer_enriched.sql
with accounts as (
    select * from {{ ref('stg_accounts') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
)
select
    a.account_id, a.branch_id, a.product_id, a.account_status,
    a.opened_date, a.currency, a.account_tenure_days,
    c.customer_id, c.full_name, c.segment, c.kyc_status
from accounts a
left join customers c on c.customer_id = a.customer_id
