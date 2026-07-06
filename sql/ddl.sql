-- =============================================================================
-- ddl.sql
-- Creates the star-schema data warehouse in Azure SQL Database.
-- Run this once during environment setup:
--   sqlcmd -S <server>.database.windows.net -d BankAnalyticsDW -U <user> -P <pwd> -i sql/ddl.sql
-- =============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');
GO

-- -----------------------------------------------------------------------------
-- Dimension: Customer
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.DimCustomer', 'U') IS NOT NULL DROP TABLE dw.DimCustomer;
CREATE TABLE dw.DimCustomer (
    customer_sk      BIGINT IDENTITY(1,1) PRIMARY KEY,   -- surrogate key
    customer_id      VARCHAR(20)  NOT NULL,               -- business/natural key
    full_name        NVARCHAR(200) NULL,
    email            NVARCHAR(200) NULL,
    phone            VARCHAR(20)  NULL,
    date_of_birth    DATE NULL,
    gender           VARCHAR(10) NULL,
    city             NVARCHAR(100) NULL,
    state            NVARCHAR(100) NULL,
    segment          VARCHAR(20) NULL,
    kyc_status       VARCHAR(20) NULL,
    age              INT NULL,
    age_group        VARCHAR(10) NULL,
    CONSTRAINT UQ_DimCustomer_customer_id UNIQUE (customer_id)
);
CREATE INDEX IX_DimCustomer_segment ON dw.DimCustomer(segment);
GO

-- -----------------------------------------------------------------------------
-- Dimension: Branch
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.DimBranch', 'U') IS NOT NULL DROP TABLE dw.DimBranch;
CREATE TABLE dw.DimBranch (
    branch_sk        BIGINT IDENTITY(1,1) PRIMARY KEY,
    branch_id        VARCHAR(10) NOT NULL,
    branch_name      NVARCHAR(150) NULL,
    region           VARCHAR(20) NULL,
    city             NVARCHAR(100) NULL,
    state            NVARCHAR(100) NULL,
    opened_date      DATE NULL,
    manager_name     NVARCHAR(150) NULL,
    CONSTRAINT UQ_DimBranch_branch_id UNIQUE (branch_id)
);
GO

-- -----------------------------------------------------------------------------
-- Dimension: Product
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.DimProduct', 'U') IS NOT NULL DROP TABLE dw.DimProduct;
CREATE TABLE dw.DimProduct (
    product_sk       BIGINT IDENTITY(1,1) PRIMARY KEY,
    product_id       VARCHAR(10) NOT NULL,
    product_name     NVARCHAR(100) NULL,
    product_category VARCHAR(30) NULL,
    interest_rate    DECIMAL(5,2) NULL,
    min_balance      DECIMAL(12,2) NULL,
    CONSTRAINT UQ_DimProduct_product_id UNIQUE (product_id)
);
GO

-- -----------------------------------------------------------------------------
-- Dimension: Account
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.DimAccount', 'U') IS NOT NULL DROP TABLE dw.DimAccount;
CREATE TABLE dw.DimAccount (
    account_sk           BIGINT IDENTITY(1,1) PRIMARY KEY,
    account_id           VARCHAR(20) NOT NULL,
    customer_sk           BIGINT NULL,
    branch_sk             BIGINT NULL,
    product_sk            BIGINT NULL,
    account_status        VARCHAR(20) NULL,
    opened_date           DATE NULL,
    currency              CHAR(3) NULL,
    account_tenure_days   INT NULL,
    CONSTRAINT UQ_DimAccount_account_id UNIQUE (account_id),
    CONSTRAINT FK_DimAccount_Customer FOREIGN KEY (customer_sk) REFERENCES dw.DimCustomer(customer_sk),
    CONSTRAINT FK_DimAccount_Branch   FOREIGN KEY (branch_sk)   REFERENCES dw.DimBranch(branch_sk),
    CONSTRAINT FK_DimAccount_Product  FOREIGN KEY (product_sk)  REFERENCES dw.DimProduct(product_sk)
);
CREATE INDEX IX_DimAccount_status ON dw.DimAccount(account_status);
GO

-- -----------------------------------------------------------------------------
-- Dimension: Date
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.DimDate', 'U') IS NOT NULL DROP TABLE dw.DimDate;
CREATE TABLE dw.DimDate (
    date_sk       INT PRIMARY KEY,          -- yyyyMMdd
    full_date     DATE NOT NULL,
    year          INT NOT NULL,
    quarter       INT NOT NULL,
    month         INT NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    day           INT NOT NULL,
    day_of_week   VARCHAR(20) NOT NULL,
    is_weekend    BIT NOT NULL
);
GO

-- -----------------------------------------------------------------------------
-- Fact: Payments
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.FactPayments', 'U') IS NOT NULL DROP TABLE dw.FactPayments;
CREATE TABLE dw.FactPayments (
    payment_sk         BIGINT IDENTITY(1,1) PRIMARY KEY,
    payment_id         VARCHAR(50) NOT NULL,
    from_account_sk    BIGINT NULL,
    to_account_sk      BIGINT NULL,
    date_sk            INT NULL,
    amount             DECIMAL(15,2) NOT NULL,
    currency           CHAR(3) NOT NULL,
    payment_type       VARCHAR(30) NULL,
    channel_id         VARCHAR(10) NULL,
    status             VARCHAR(20) NULL,
    payment_timestamp  DATETIME2 NULL,
    payment_size_bucket VARCHAR(10) NULL,
    CONSTRAINT UQ_FactPayments_payment_id UNIQUE (payment_id),
    CONSTRAINT FK_FactPayments_FromAccount FOREIGN KEY (from_account_sk) REFERENCES dw.DimAccount(account_sk),
    CONSTRAINT FK_FactPayments_ToAccount   FOREIGN KEY (to_account_sk)   REFERENCES dw.DimAccount(account_sk),
    CONSTRAINT FK_FactPayments_Date        FOREIGN KEY (date_sk)         REFERENCES dw.DimDate(date_sk)
);
CREATE INDEX IX_FactPayments_date ON dw.FactPayments(date_sk);
CREATE INDEX IX_FactPayments_status ON dw.FactPayments(status);
GO

-- -----------------------------------------------------------------------------
-- Fact: Savings Transactions
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.FactSavingsTransactions', 'U') IS NOT NULL DROP TABLE dw.FactSavingsTransactions;
CREATE TABLE dw.FactSavingsTransactions (
    transaction_sk       BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_id       VARCHAR(50) NOT NULL,
    account_sk           BIGINT NULL,
    date_sk               INT NULL,
    transaction_type     VARCHAR(30) NULL,
    transaction_direction VARCHAR(10) NULL,
    amount               DECIMAL(15,2) NOT NULL,
    channel_id           VARCHAR(10) NULL,
    transaction_timestamp DATETIME2 NULL,
    CONSTRAINT UQ_FactSavingsTxn_transaction_id UNIQUE (transaction_id),
    CONSTRAINT FK_FactSavingsTxn_Account FOREIGN KEY (account_sk) REFERENCES dw.DimAccount(account_sk),
    CONSTRAINT FK_FactSavingsTxn_Date    FOREIGN KEY (date_sk)    REFERENCES dw.DimDate(date_sk)
);
CREATE INDEX IX_FactSavingsTxn_date ON dw.FactSavingsTransactions(date_sk);
CREATE INDEX IX_FactSavingsTxn_type ON dw.FactSavingsTransactions(transaction_type);
GO

-- -----------------------------------------------------------------------------
-- Control table for incremental loading (watermarks) - see
-- pyspark/utils/incremental_load.py for the production Delta-table equivalent
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.etl_watermark', 'U') IS NOT NULL DROP TABLE dw.etl_watermark;
CREATE TABLE dw.etl_watermark (
    dataset_name    VARCHAR(100) PRIMARY KEY,
    watermark_value VARCHAR(50) NOT NULL,
    updated_at      DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

-- -----------------------------------------------------------------------------
-- Audit log table for governance (every pipeline run logs here)
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dw.pipeline_audit_log', 'U') IS NOT NULL DROP TABLE dw.pipeline_audit_log;
CREATE TABLE dw.pipeline_audit_log (
    audit_id        BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id        VARCHAR(20) NOT NULL,
    pipeline_name   VARCHAR(100) NOT NULL,
    step_name       VARCHAR(100) NOT NULL,
    status          VARCHAR(20) NOT NULL,   -- STARTED / SUCCESS / FAILED
    rows_processed  BIGINT NULL,
    rows_rejected   BIGINT NULL,
    error_message   NVARCHAR(MAX) NULL,
    started_at      DATETIME2 NOT NULL,
    ended_at        DATETIME2 NULL
);
GO
