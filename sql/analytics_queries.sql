-- =============================================================================
-- analytics_queries.sql
-- Reporting / analytics SQL used by Power BI dashboards (Import or DirectQuery)
-- and demonstrable in interviews. Written against the dw.* star schema.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Daily Transaction Summary
-- -----------------------------------------------------------------------------
SELECT
    d.full_date,
    COUNT(*)                                    AS total_transactions,
    SUM(f.amount)                               AS total_amount,
    SUM(CASE WHEN f.transaction_direction = 'CREDIT' THEN f.amount ELSE 0 END) AS total_credits,
    SUM(CASE WHEN f.transaction_direction = 'DEBIT'  THEN f.amount ELSE 0 END) AS total_debits
FROM dw.FactSavingsTransactions f
JOIN dw.DimDate d ON d.date_sk = f.date_sk
GROUP BY d.full_date
ORDER BY d.full_date DESC;


-- -----------------------------------------------------------------------------
-- 2. Monthly Payment Trends
-- -----------------------------------------------------------------------------
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*)          AS payment_count,
    SUM(f.amount)      AS total_payment_amount,
    AVG(f.amount)      AS avg_payment_amount
FROM dw.FactPayments f
JOIN dw.DimDate d ON d.date_sk = f.date_sk
WHERE f.status = 'SUCCESS'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- -----------------------------------------------------------------------------
-- 3. Branch Performance (payments processed via accounts at each branch)
-- -----------------------------------------------------------------------------
SELECT
    b.branch_id,
    b.branch_name,
    b.region,
    COUNT(DISTINCT a.account_id)   AS num_accounts,
    COUNT(f.payment_sk)             AS num_payments,
    SUM(f.amount)                   AS total_payment_volume
FROM dw.FactPayments f
JOIN dw.DimAccount a ON a.account_sk = f.from_account_sk
JOIN dw.DimBranch b  ON b.branch_sk  = a.branch_sk
GROUP BY b.branch_id, b.branch_name, b.region
ORDER BY total_payment_volume DESC;


-- -----------------------------------------------------------------------------
-- 4. Customer Account Summary (CTE + window function)
-- -----------------------------------------------------------------------------
WITH account_balances AS (
    SELECT
        c.customer_id,
        c.full_name,
        a.account_id,
        a.currency,
        a.account_status,
        RANK() OVER (PARTITION BY c.customer_id ORDER BY a.opened_date ASC) AS account_rank
    FROM dw.DimCustomer c
    JOIN dw.DimAccount a ON a.customer_sk = c.customer_sk
)
SELECT
    customer_id,
    full_name,
    COUNT(account_id)                                            AS total_accounts,
    SUM(CASE WHEN account_status = 'Active' THEN 1 ELSE 0 END)   AS active_accounts
FROM account_balances
GROUP BY customer_id, full_name
ORDER BY total_accounts DESC;


-- -----------------------------------------------------------------------------
-- 5. Deposit vs Withdrawal Analysis
-- -----------------------------------------------------------------------------
SELECT
    d.year,
    d.month_name,
    SUM(CASE WHEN f.transaction_type = 'DEPOSIT'    THEN f.amount ELSE 0 END) AS total_deposits,
    SUM(CASE WHEN f.transaction_type = 'WITHDRAWAL' THEN f.amount ELSE 0 END) AS total_withdrawals,
    SUM(CASE WHEN f.transaction_type = 'DEPOSIT'    THEN f.amount ELSE 0 END)
      - SUM(CASE WHEN f.transaction_type = 'WITHDRAWAL' THEN f.amount ELSE 0 END) AS net_flow
FROM dw.FactSavingsTransactions f
JOIN dw.DimDate d ON d.date_sk = f.date_sk
GROUP BY d.year, d.month_name, d.month
ORDER BY d.year, d.month;


-- -----------------------------------------------------------------------------
-- 6. Top 20 Customers by Total Payment Volume (window function)
-- -----------------------------------------------------------------------------
SELECT TOP 20
    c.customer_id,
    c.full_name,
    c.segment,
    SUM(f.amount)                                    AS total_payment_volume,
    RANK() OVER (ORDER BY SUM(f.amount) DESC)         AS customer_rank
FROM dw.FactPayments f
JOIN dw.DimAccount a  ON a.account_sk = f.from_account_sk
JOIN dw.DimCustomer c ON c.customer_sk = a.customer_sk
WHERE f.status = 'SUCCESS'
GROUP BY c.customer_id, c.full_name, c.segment
ORDER BY total_payment_volume DESC;


-- -----------------------------------------------------------------------------
-- 7. Active Account Analysis
-- -----------------------------------------------------------------------------
SELECT
    p.product_name,
    a.account_status,
    COUNT(*)                          AS account_count,
    AVG(a.account_tenure_days)        AS avg_tenure_days
FROM dw.DimAccount a
JOIN dw.DimProduct p ON p.product_sk = a.product_sk
GROUP BY p.product_name, a.account_status
ORDER BY p.product_name, a.account_status;


-- -----------------------------------------------------------------------------
-- 8. Reusable Views for Power BI (thin wrappers over the above logic)
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW dw.vw_MonthlyPaymentTrends AS
SELECT
    d.year, d.month, d.month_name,
    COUNT(*) AS payment_count,
    SUM(f.amount) AS total_payment_amount
FROM dw.FactPayments f
JOIN dw.DimDate d ON d.date_sk = f.date_sk
WHERE f.status = 'SUCCESS'
GROUP BY d.year, d.month, d.month_name;
GO

CREATE OR ALTER VIEW dw.vw_BranchPerformance AS
SELECT
    b.branch_id, b.branch_name, b.region,
    COUNT(f.payment_sk) AS num_payments,
    SUM(f.amount) AS total_payment_volume
FROM dw.FactPayments f
JOIN dw.DimAccount a ON a.account_sk = f.from_account_sk
JOIN dw.DimBranch b ON b.branch_sk = a.branch_sk
GROUP BY b.branch_id, b.branch_name, b.region;
GO

CREATE OR ALTER VIEW dw.vw_DepositsVsWithdrawals AS
SELECT
    d.full_date,
    SUM(CASE WHEN f.transaction_type = 'DEPOSIT' THEN f.amount ELSE 0 END) AS deposits,
    SUM(CASE WHEN f.transaction_type = 'WITHDRAWAL' THEN f.amount ELSE 0 END) AS withdrawals
FROM dw.FactSavingsTransactions f
JOIN dw.DimDate d ON d.date_sk = f.date_sk
GROUP BY d.full_date;
GO
