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
| Apache Ossie Power BI converter | `apache/ossie-converters@8b878e9`, `microsoft/`, **read statically** | Real code, but not run (see Limits) |
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

## Limits

- **No live Databricks, Unity Catalog, Fabric or Power BI.** E1 shows the mechanism: policy
  lives in the catalog, not the files. It does not reproduce Fabric's credential behaviour. That
  part still rests on Microsoft and Databricks documentation.
- **E2's T-SQL semantics are emulated** in DuckDB from documented T-SQL behaviour: `CONCAT`
  treats NULL as `''`, int/int truncates, and `_CI` collations match case-insensitively. Fabric
  Warehouse's default collation is case-sensitive, so the case result applies only to `_CI`
  warehouses. Two outcomes were not executed. `MONTHS_BETWEEN` is not a T-SQL function, so it
  would fail. String `+` in Spark fails under ANSI mode and returns NULL otherwise.
- **The Ossie converter was read, not run.** The sandbox's permission policy blocked running
  downloaded third-party code. E7 parses the converter's own classification tables with `ast`
  and runs none of its code. Running `ossie-microsoft import --translate-dax gold_sales.bim`
  would confirm the result end to end.
- Spark (`pyspark` 4.0.1) did not build on Python 3.13 here, so delta-rs stands in for the
  Databricks writer. E5's extra `variantType` feature may be specific to delta-rs.

## Run it

```bash
cd labs/day07_dbx_fabric_boundary
mkdir -p vendor
git clone --depth 1 https://github.com/bitol-io/open-data-contract-standard.git vendor/odcs
git clone --depth 1 https://github.com/apache/ossie-converters.git vendor/ossie-converters
pip install duckdb deltalake sqlglot pyarrow pyyaml jsonschema
python make_bim.py && python boundary_lab.py | tee lab_output.txt
```

`lab_output.txt` holds the full output from the run quoted above.
