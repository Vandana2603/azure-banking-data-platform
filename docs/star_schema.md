# Star Schema

```
                        ┌───────────────┐
                        │   DimDate     │
                        │  date_sk (PK) │
                        └───────┬───────┘
                                │
        ┌───────────────┐      │      ┌───────────────┐
        │  DimCustomer  │      │      │   DimBranch   │
        │ customer_sk PK│      │      │ branch_sk  PK │
        └───────┬───────┘      │      └───────┬───────┘
                │              │              │
                │      ┌───────▼───────┐      │
                └─────►│  DimAccount   │◄─────┘
                       │ account_sk PK │
                       │ customer_sk FK│
                       │ branch_sk  FK │
                       │ product_sk FK │◄────┐
                       └───────┬───────┘     │
                               │      ┌───────┴───────┐
                               │      │  DimProduct   │
                               │      │ product_sk PK │
                               │      └───────────────┘
                 ┌─────────────┼─────────────┐
                 ▼                           ▼
        ┌──────────────────┐      ┌───────────────────────────┐
        │   FactPayments    │      │  FactSavingsTransactions  │
        │ payment_sk    PK  │      │ transaction_sk        PK  │
        │ from_account_sk FK│      │ account_sk           FK  │
        │ to_account_sk   FK│      │ date_sk              FK  │
        │ date_sk         FK│      │ transaction_type          │
        │ amount             │      │ transaction_direction     │
        │ currency           │      │ amount                    │
        │ payment_type       │      │ channel_id                │
        │ status             │      └───────────────────────────┘
        └────────────────────┘
```

## Grain

- **FactPayments**: one row per payment transaction.
- **FactSavingsTransactions**: one row per savings account transaction
  (deposit, withdrawal, interest credit, fee debit).

## Keys

- Every dimension has a surrogate key (`*_sk`, `IDENTITY`) used for joins,
  and a natural/business key (`*_id`) used for upserts (`MERGE ... ON`).
- Foreign keys enforce referential integrity between facts and dimensions
  (see `sql/ddl.sql`).
- `DimDate` uses `yyyyMMdd` integer surrogate keys for fast partition pruning
  and simple joins.

## Indexing Strategy

- Non-clustered indexes on frequently filtered columns:
  `DimAccount.account_status`, `FactPayments.date_sk`,
  `FactPayments.status`, `FactSavingsTransactions.date_sk`,
  `FactSavingsTransactions.transaction_type`.
- Unique constraints on all natural keys to prevent duplicate dimension rows
  and support fast `MERGE` upserts.
