-- =============================================================================
-- warehouse_load.sql
-- Loads dw.* star-schema tables from staging tables that ADF's "Copy Data"
-- activity lands from the gold Parquet layer (staging schema below).
--
-- In the real pipeline, ADF's Copy Data activity uses PolyBase / COPY INTO to
-- bulk-load the gold Parquet files from ADLS Gen2 into stg.* tables first;
-- this script then MERGEs staging into the final dw.* tables so re-runs are
-- idempotent (no duplicate rows on daily re-execution).
-- =============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

-- Staging tables mirror the gold layer schema (created once, truncated + reloaded daily by ADF)
IF OBJECT_ID('stg.customers', 'U') IS NULL
CREATE TABLE stg.customers (
    customer_id VARCHAR(20), full_name NVARCHAR(200), email NVARCHAR(200), phone VARCHAR(20),
    date_of_birth DATE, gender VARCHAR(10), city NVARCHAR(100), state NVARCHAR(100),
    segment VARCHAR(20), kyc_status VARCHAR(20), age INT, age_group VARCHAR(10)
);
GO

-- -----------------------------------------------------------------------------
-- MERGE staging into DimCustomer (upsert on business key = customer_id)
-- -----------------------------------------------------------------------------
MERGE dw.DimCustomer AS target
USING stg.customers AS source
    ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        full_name = source.full_name, email = source.email, phone = source.phone,
        date_of_birth = source.date_of_birth, gender = source.gender,
        city = source.city, state = source.state, segment = source.segment,
        kyc_status = source.kyc_status, age = source.age, age_group = source.age_group
WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, full_name, email, phone, date_of_birth, gender, city, state,
            segment, kyc_status, age, age_group)
    VALUES (source.customer_id, source.full_name, source.email, source.phone,
            source.date_of_birth, source.gender, source.city, source.state,
            source.segment, source.kyc_status, source.age, source.age_group);
GO

-- -----------------------------------------------------------------------------
-- MERGE staging into DimAccount (resolves customer/branch/product surrogate keys)
-- -----------------------------------------------------------------------------
IF OBJECT_ID('stg.accounts', 'U') IS NULL
CREATE TABLE stg.accounts (
    account_id VARCHAR(20), customer_id VARCHAR(20), branch_id VARCHAR(10),
    product_id VARCHAR(10), account_status VARCHAR(20), opened_date DATE,
    currency CHAR(3), account_tenure_days INT
);
GO

MERGE dw.DimAccount AS target
USING (
    SELECT
        s.account_id, c.customer_sk, b.branch_sk, p.product_sk,
        s.account_status, s.opened_date, s.currency, s.account_tenure_days
    FROM stg.accounts s
    LEFT JOIN dw.DimCustomer c ON c.customer_id = s.customer_id
    LEFT JOIN dw.DimBranch   b ON b.branch_id   = s.branch_id
    LEFT JOIN dw.DimProduct  p ON p.product_id  = s.product_id
) AS source
    ON target.account_id = source.account_id
WHEN MATCHED THEN
    UPDATE SET
        customer_sk = source.customer_sk, branch_sk = source.branch_sk,
        product_sk = source.product_sk, account_status = source.account_status,
        opened_date = source.opened_date, currency = source.currency,
        account_tenure_days = source.account_tenure_days
WHEN NOT MATCHED BY TARGET THEN
    INSERT (account_id, customer_sk, branch_sk, product_sk, account_status,
            opened_date, currency, account_tenure_days)
    VALUES (source.account_id, source.customer_sk, source.branch_sk, source.product_sk,
            source.account_status, source.opened_date, source.currency, source.account_tenure_days);
GO

-- -----------------------------------------------------------------------------
-- MERGE staging into FactPayments (idempotent on payment_id)
-- -----------------------------------------------------------------------------
IF OBJECT_ID('stg.payments', 'U') IS NULL
CREATE TABLE stg.payments (
    payment_id VARCHAR(50), from_account_id VARCHAR(20), to_account_id VARCHAR(20),
    amount DECIMAL(15,2), currency CHAR(3), payment_type VARCHAR(30),
    channel_id VARCHAR(10), status VARCHAR(20), payment_timestamp DATETIME2,
    payment_size_bucket VARCHAR(10)
);
GO

MERGE dw.FactPayments AS target
USING (
    SELECT
        s.payment_id, fa.account_sk AS from_account_sk, ta.account_sk AS to_account_sk,
        CAST(FORMAT(s.payment_timestamp, 'yyyyMMdd') AS INT) AS date_sk,
        s.amount, s.currency, s.payment_type, s.channel_id, s.status,
        s.payment_timestamp, s.payment_size_bucket
    FROM stg.payments s
    LEFT JOIN dw.DimAccount fa ON fa.account_id = s.from_account_id
    LEFT JOIN dw.DimAccount ta ON ta.account_id = s.to_account_id
) AS source
    ON target.payment_id = source.payment_id
WHEN MATCHED THEN
    UPDATE SET status = source.status, amount = source.amount
WHEN NOT MATCHED BY TARGET THEN
    INSERT (payment_id, from_account_sk, to_account_sk, date_sk, amount, currency,
            payment_type, channel_id, status, payment_timestamp, payment_size_bucket)
    VALUES (source.payment_id, source.from_account_sk, source.to_account_sk, source.date_sk,
            source.amount, source.currency, source.payment_type, source.channel_id,
            source.status, source.payment_timestamp, source.payment_size_bucket);
GO

-- -----------------------------------------------------------------------------
-- MERGE staging into FactSavingsTransactions (idempotent on transaction_id)
-- -----------------------------------------------------------------------------
IF OBJECT_ID('stg.savings_transactions', 'U') IS NULL
CREATE TABLE stg.savings_transactions (
    transaction_id VARCHAR(50), account_id VARCHAR(20), transaction_type VARCHAR(30),
    transaction_direction VARCHAR(10), amount DECIMAL(15,2), channel_id VARCHAR(10),
    transaction_timestamp DATETIME2
);
GO

MERGE dw.FactSavingsTransactions AS target
USING (
    SELECT
        s.transaction_id, a.account_sk,
        CAST(FORMAT(s.transaction_timestamp, 'yyyyMMdd') AS INT) AS date_sk,
        s.transaction_type, s.transaction_direction, s.amount, s.channel_id,
        s.transaction_timestamp
    FROM stg.savings_transactions s
    LEFT JOIN dw.DimAccount a ON a.account_id = s.account_id
) AS source
    ON target.transaction_id = source.transaction_id
WHEN NOT MATCHED BY TARGET THEN
    INSERT (transaction_id, account_sk, date_sk, transaction_type, transaction_direction,
            amount, channel_id, transaction_timestamp)
    VALUES (source.transaction_id, source.account_sk, source.date_sk, source.transaction_type,
            source.transaction_direction, source.amount, source.channel_id, source.transaction_timestamp);
GO
