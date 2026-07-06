# Power BI Dashboards

Connect Power BI Desktop to Azure SQL Database (`dw` schema) using either
**Import** mode (recommended for this data volume) or **DirectQuery** if you
want dashboards to always reflect the latest warehouse load.

`Get Data -> Azure SQL Database -> server: <server>.database.windows.net,
database: BankAnalyticsDW -> select dw.DimCustomer, dw.DimAccount,
dw.DimBranch, dw.DimProduct, dw.DimDate, dw.FactPayments,
dw.FactSavingsTransactions (or the equivalent vw_* views in
sql/analytics_queries.sql).`

Mark `DimDate` as a **Date Table** in Power BI (Modeling -> Mark as Date
Table) to enable time-intelligence DAX functions.

## 1. Executive Dashboard

**Visuals:** KPI cards, line chart, map, bar chart.

| Metric | DAX Measure |
|---|---|
| Total Payments | `Total Payments = SUM(FactPayments[amount])` |
| Total Savings Volume | `Total Savings = SUM(FactSavingsTransactions[amount])` |
| Daily Transactions | `Daily Txns = COUNTROWS(FactSavingsTransactions)` |
| Active Customers | `Active Customers = CALCULATE(DISTINCTCOUNT(DimAccount[customer_sk]), DimAccount[account_status]="Active")` |
| MoM Payment Growth | `MoM Growth % = DIVIDE([Total Payments] - CALCULATE([Total Payments], DATEADD(DimDate[full_date], -1, MONTH)), CALCULATE([Total Payments], DATEADD(DimDate[full_date], -1, MONTH)))` |

Branch-wise performance uses `sql/analytics_queries.sql` query #3
(`vw_BranchPerformance`) as the visual's data source, sliced by `region`.

## 2. Customer Analytics Dashboard

**Visuals:** Donut chart (segment), scatter plot, table, funnel.

| Metric | DAX Measure |
|---|---|
| Avg Account Balance | `Avg Balance = AVERAGE(DimAccount[balance])` *(if balance is retained in the gold layer)* |
| Payment Frequency per Customer | `Payments per Customer = DIVIDE(COUNTROWS(FactPayments), DISTINCTCOUNT(DimCustomer[customer_sk]))` |
| Customer Growth | `New Customers = CALCULATE(DISTINCTCOUNT(DimCustomer[customer_sk]), DimAccount[opened_date] = MAX(DimDate[full_date]))` |
| Top Customers | Table visual bound to `sql/analytics_queries.sql` query #6 |

Segmentation uses `DimCustomer[segment]` and `DimCustomer[age_group]` as
slicers across all visuals on this page.

## 3. Operations Dashboard

**Visuals:** Stacked bar (deposits vs withdrawals), pie (channels), gauge
(failed transactions), table (pipeline/data-quality status).

| Metric | DAX Measure |
|---|---|
| Deposits vs Withdrawals | Uses `vw_DepositsVsWithdrawals` directly |
| Failed Transaction Rate | `Failed Rate % = DIVIDE(CALCULATE(COUNTROWS(FactPayments), FactPayments[status]="FAILED"), COUNTROWS(FactPayments))` |
| Transaction Channel Mix | Pie chart on `channel_id`, values = `COUNTROWS(FactSavingsTransactions)` |
| Pipeline Refresh Status | Table bound to `dw.pipeline_audit_log` (last run per pipeline step) |
| Data Quality Metrics | Table bound to the null/duplicate rate reports emitted by `pyspark/transformations/validation.py` and saved under `docs/data_quality_reports/` |

## Screenshots

This portfolio repo ships without live screenshots since it isn't connected
to a running Azure environment. After you build the .pbix file against your
own Azure SQL Database, export screenshots (File -> Export -> PDF/Image in
Power BI Desktop, or a screen capture) and drop them here as:

```
dashboards/screenshots/executive_dashboard.png
dashboards/screenshots/customer_analytics_dashboard.png
dashboards/screenshots/operations_dashboard.png
```

Then reference them in the main `README.md` Dashboard section with standard
Markdown image syntax so they render on GitHub.
