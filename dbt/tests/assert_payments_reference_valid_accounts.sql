-- assert_payments_reference_valid_accounts.sql
-- Singular test: fails (returns rows) if any fact_payments row references
-- an account_id that does not exist in dim_account.

select f.payment_id, f.from_account_id
from {{ ref('fact_payments') }} f
left join {{ ref('dim_account') }} a on a.account_id = f.from_account_id
where a.account_id is null
