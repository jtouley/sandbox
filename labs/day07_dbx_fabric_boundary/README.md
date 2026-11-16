# Day 07: Databricks + Fabric boundary lab

This lab tests the claims in *Databricks + Fabric: The Governance Gap at the Boundary*
(Nov 16, 2026) and the six-panel architecture diagram. It runs locally with no cloud accounts.

## What is simulated, and how faithfully

| Real component | Stand-in used here | Fidelity |
|---|---|---|
| Databricks writes Gold Delta tables | `deltalake` (delta-rs 1.6.6) writer | Same Delta protocol and log format |
| Unity Catalog row filter and column mask | DuckDB governed view keyed on a session principal | Policy logic only. It is not UC itself. |
| Fabric mirrored catalog / OneLake shortcut | A second, independent DuckDB (2.0.0) session reading the Delta path | Same access pattern: read files in place |
| Power BI semantic model | `gold_sales.bim` (TMSL) with 11 measures and one RLS role | Real TMSL structure |
| Apache Ossie converters | `apache/ossie-converters@8b878e9`: `microsoft/` (Python) and `databricks/java` (built with Maven) | Real code, **run end to end** (E9) |
| ODCS contracts | `bitol-io/open-data-contract-standard@529d4c0`, official JSON schemas | Real schemas |
| Databricks SQL to Fabric T-SQL | `sqlglot` 30.31.0, `databricks` to `fabric` dialects | Real transpiler |

## Results

| # | Claim in the doc | Experiment | Result | Verdict |
|---|---|---|---|---|
| E1 | Unity Catalog policies don't carry into Fabric | Same Delta table read through a governed view and through a path reader | Governed view: 6 rows, email masked. Path reader: 12 rows, 2 regions, `c105@example.com` in clear. The `_delta_log` holds only `add`, `commitInfo`, `metaData` and `protocol`, with an empty table configuration. | **Confirmed (mechanism).** The Delta format has nowhere to put a row filter or mask, so any reader that holds a storage credential gets everything. |
| E2 | The same rule in two dialects drifts | One conformance rule, run under Databricks and T-SQL semantics, plus a sqlglot round trip | 4 of 8 output cells differ: NULL concat (`NULL` vs `'Ann  Lee'`), int/int division (`3.5` vs `3`, `2.25` vs `2`), and case-sensitive matching of `'GOLD'`. sqlglot handled the division (`CAST(units AS FLOAT)`), passed `MONTHS_BETWEEN` through into T-SQL unchanged, and turned `concat()` into `+`, which it left as `+` on the way back to Databricks. | **Confirmed.** "Translate then test" is the right rule. The transpiler's output is a draft. |
| E3 | Auto-sync carries breaking changes into Power BI | Producer renames `amount`, retypes `customer_key` and drops `region`, then a mirror serves the latest version | No gate: version 1 is live and `amount` and `region` no longer bind. An ODCS v3.1.0 contract, valid against the official schema, flags 3 violations before commit. | **Confirmed.** A contract gate in Databricks CI catches it. Binding by column name alone missed the type change, and the contract caught it. |
| E4 | Usage never flows back upstream | 300 consumer reads (DuckDB and delta-rs), with the `_delta_log` hashed before and after | Hash unchanged (`ff057b73c4a635b4`, 2 files). `commitInfo` records writers only. | **Confirmed.** Usage has to come from Fabric and Power BI logs, joined on a shared ID. |
| E5 | Delta feature drift causes quiet sync errors | One `ALTER ... SET TBLPROPERTIES (enableDeletionVectors)` | Protocol went from reader 1 / writer 2 to reader 3 / writer 7, with reader features `deletionVectors` and `variantType`. No review step ran, and the second feature was not requested. | **Confirmed.** Pin protocol versions in the contract. |
| E6 | Panel 3/4 rule: ratios and distinct counts belong in the semantic model | Precomputed monthly Gold vs evaluation at query time | Margin 37.31% vs **26.78%**. Customers 12 vs **8**. | **Diagram principle confirmed.** |
| E7 | Panel 6: DAX measures are least likely to round-trip | 11 measures classified against the converter's own `_dax_catalog.py` and `_dax_to_sql.py` | **3 of 11 translate today** (`Total Sales`, `Total Cost`, `Margin %`). 4 of 11 have a portable form at all. `TOTALYTD` and `SAMEPERIODLASTYEAR` are `SPEC-GAP(window-measure)`. `CALCULATE` is `SPEC-GAP(filter-context)`. `ALL` is `REFUSE`. `DISTINCTCOUNT` is refused as a semantic mismatch (BLANK counts as a value). 39 of 41 time-intelligence functions are spec gaps. RLS `roles` have no Ossie counterpart (converter README). | **Confirmed and sharpened.** Only additive measures survive. Every time-intelligence and filter-context measure stays DAX. |
| E8 | ODCS v3.2 adds a context block; measures (RFC-0034) are still proposed | Key search of the official v3.1.0 and v3.2.0 JSON schemas | v3.2.0 has `context`. Neither version has `measures`, `metrics`, a row filter or masking. | **Confirmed.** Contracts can't carry security policy either, so E1's gap needs its own control. |
| E9 | Panel 6's two-way arrow | `ossie_roundtrip.sh`: Power BI → Ossie → Databricks Metric View → Ossie → Power BI with the official converters | Power BI → Ossie → Power BI is lossless (11/11 measures, RLS kept) only because DAX and RLS ride along in a Power BI-only stash. The Databricks leg stops twice: Fabric's 2-part `gold.fact_sales` is rejected because UC needs 3 parts, and `ProductKey` in two tables collides in the Metric View's single namespace. After manual fixes, 8 of 11 measures are dropped, and every column loses its `datatype` and `is_time`. The return leg fails: the Databricks converter writes Ossie 0.2 `custom_extensions` and the Microsoft converter rejects them. After a manual bridge, 3 of 11 measures come back. `Margin %` comes back as `BLANK()`, with 0 of 2 relationships and no RLS. | **Confirmed, and worse than the doc says.** Two-way sync doesn't work today without hand-written glue, and the full loop keeps 2 of 11 measures. |

## Spark in Docker: an Azure Databricks stand-in (`spark/`)

E10 to E12 replace the earlier stand-ins with real engines. Spark 4.0.1 with Delta 4.0 (the
same Spark line as Databricks Runtime 17) writes Gold tables over `wasb://` into **Azurite**,
Microsoft's Azure Blob Storage emulator. Non-Spark engines then read those tables from the same
storage. T-SQL runs on **SQL Server 2022**, the Fabric Warehouse dialect's engine family.

| # | Test | Result |
|---|---|---|
| E10 | Spark writes 5 Gold tables with the Delta features Databricks uses: plain; deletion vectors plus `DELETE`; column mapping (`name`, `id`) plus `RENAME COLUMN` / `DROP COLUMN`; liquid clustering. delta-rs 1.6.6 and DuckDB 2.0 (delta-kernel-rs) read them from Azure storage and are compared with Spark. | DuckDB matched Spark on all 5. delta-rs refused the deletion-vector table with an error, which is the safe failure. On both column-mapped tables delta-rs returned **every value as NULL** (3,000 of 3,000 and 4,000 of 4,000) with the right column names, the right row count and **no error**. Spark's real protocol: deletion vectors → reader 3 / writer 7; column mapping → 2 / 7; liquid → 1 / 7. Spark did not add `variantType`, so E5's extra feature came from delta-rs. |
| E11 | The E2 rule, executed for real: Spark SQL with ANSI on, and a hand-written T-SQL port on SQL Server | On Fabric's default case-sensitive collation (`Latin1_General_100_BIN2_UTF8`), 5 of 10 cells differ from Spark. On a case-insensitive collation, 6 of 10 differ. Causes: `CONCAT` NULL handling (`'Ann  Lee'` vs `NULL`), integer division (3 vs 3.5), `DATEDIFF(month)` vs `months_between` (2 vs 1.032), and collation. |
| E12 | sqlglot's translation, executed on both engines | Its T-SQL **fails to compile** on SQL Server: `'MONTHS_BETWEEN' is not a recognized built-in function name`. Without that column, its output matches Spark. The round trip back to Databricks (`first_name + ' ' + ...`) **fails** in Spark ANSI mode (`DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE`). With ANSI off it returns NULL for every row, including `'Bo J Kim'`. |
| E1 (Spark) | Spark view with a row filter vs a direct path read of the same table | 450 vs 900 rows. |

Run it (Docker daemon required; Maven downloads the jars):

```bash
cd labs/day07_dbx_fabric_boundary/spark
JARS=$PWD/jars ./setup.sh
JARS=$PWD/jars ./run.sh boundary_spark.py     # writes spark_truth.json
python3 read_from_fabric_side.py              # writes fabric_side_results.json
python3 tsql_side.py                          # writes tsql_results.json
```

Limits: Azurite emulates Blob storage, not ADLS Gen2's hierarchical namespace, so the tables use
`wasb://` and not `abfss://`. Unity Catalog row filters, column masks and the real Fabric
readers are not available outside the cloud. delta-rs and DuckDB show that a second Delta
engine can silently disagree with Spark. They do not show what Fabric's own engine does.

## Limits

- **No live Databricks, Unity Catalog, Fabric or Power BI.** E1 shows the mechanism: policy
  lives in the catalog, not the files. It does not reproduce Fabric's credential behaviour. That
  part still rests on Microsoft and Databricks documentation.
- **E2's T-SQL semantics are emulated** (E11 then ran them on real SQL Server) in DuckDB from documented T-SQL behaviour: `CONCAT`
  treats NULL as `''`, int/int truncates, and `_CI` collations match case-insensitively. Fabric
  Warehouse's default collation is case-sensitive, so the case result applies only to `_CI`
  warehouses. Two outcomes were not executed. `MONTHS_BETWEEN` is not a T-SQL function, so it
  would fail. String `+` in Spark fails under ANSI mode and returns NULL otherwise.
- E7 parses the converter's classification tables statically. E9 then ran the converters
  for real, and the live import gave the same 3 of 11.
- E1 to E6 use delta-rs as the writer. The real-Spark tests (E10 to E12) confirm or correct
  them. In particular, `variantType` in E5 came from delta-rs.

## Run it

```bash
cd labs/day07_dbx_fabric_boundary
mkdir -p vendor
git clone --depth 1 https://github.com/bitol-io/open-data-contract-standard.git vendor/odcs
git clone --depth 1 https://github.com/apache/ossie-converters.git vendor/ossie-converters
pip install duckdb deltalake sqlglot pyarrow pyyaml jsonschema
python make_bim.py && python boundary_lab.py | tee lab_output.txt
(cd vendor/ossie-converters/databricks/java && mvn -q -DskipTests package)
bash ossie_roundtrip.sh | tee roundtrip_output.txt
```

`lab_output.txt` holds the full output from the run quoted above.
