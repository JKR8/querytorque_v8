# Databricks TPC-DS on a downsized cluster: runbook + configs + forensics

This is a practical checklist for running TPC-DS at large scale (e.g., 10TB) on a *small* Databricks cluster/warehouse. The goal is to finish reliably by **reducing skew + shuffles**, and by using **Photon + Delta layout** correctly.

---

## 0) Non-negotiables

- **Photon ON** (cluster or SQL warehouse)
- **AQE ON** + **skew join handling ON**
- **Delta table layout prepared** (clustering + OPTIMIZE)
- **Small files mitigated** (optimizeWrite + autoCompact)
- **Measure spill + shuffle** every run (system tables / Spark UI)

---

## 1) Compute: what to enable

### 1.1 Photon
**Enable Photon Acceleration** on the compute running the benchmark.

Validation:
- Spark UI ➜ SQL tab ➜ physical plan nodes show Photon operators
- Query profile / plan indicates Photon execution

---

## 2) Delta data layout: make the files queryable on small compute

### 2.1 Use Liquid clustering (recommended)
For each large fact table, set clustering keys that match common filters and joins.

Example (adjust keys to your workload):
```sql
ALTER TABLE tpcds.store_sales
CLUSTER BY (ss_sold_date_sk, ss_item_sk);

ALTER TABLE tpcds.web_sales
CLUSTER BY (ws_sold_date_sk, ws_item_sk);

ALTER TABLE tpcds.catalog_sales
CLUSTER BY (cs_sold_date_sk, cs_item_sk);
