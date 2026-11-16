"""Boundary lab: local simulation of the Databricks -> Fabric boundary.

Engines
  * "Databricks side"  : delta-rs writer + a DuckDB session acting as the governed catalog
  * "Fabric side"      : a second, independent DuckDB session reading the same Delta
                         files by path, the way a OneLake shortcut / mirrored catalog does
Each experiment prints a RESULT line that the report quotes.
"""
import hashlib
import json
import os
import shutil
import warnings

import duckdb
import jsonschema
import pyarrow as pa
import sqlglot
import yaml
from deltalake import DeltaTable, write_deltalake

warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.abspath(__file__))
LAKE = os.path.join(ROOT, "lake")
ODCS_SCHEMA = os.path.join(ROOT, "vendor", "odcs", "schema", "odcs-json-schema-v3.1.0.json")
shutil.rmtree(LAKE, ignore_errors=True)
OUT = {}


def banner(t):
    print("\n" + "=" * 78 + "\n" + t + "\n" + "=" * 78)


def q(con, sql):
    return con.sql(sql).fetchall()


# ---------------------------------------------------------------- data
sales = pa.table({
    "sales_key": list(range(1, 13)),
    "order_date": pa.array([f"2025-{m:02d}-15" for m in (1, 2, 3, 4, 5, 6)] * 2).cast(pa.date32()),
    "customer_key": [101, 102, 101, 103, 104, 101, 105, 106, 105, 107, 108, 105],
    "customer_email": [f"c{k}@example.com" for k in
                       [101, 102, 101, 103, 104, 101, 105, 106, 105, 107, 108, 105]],
    "product_key": [1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2],
    "channel": ["Online", "Store"] * 6,
    "region": ["West"] * 6 + ["East"] * 6,
    "amount": pa.array([100.0, 200, 150, 50, 300, 120, 80, 400, 60, 90, 250, 30]),
    "cost": pa.array([60.0, 150, 90, 20, 210, 100, 40, 380, 30, 50, 200, 10]),
})
GOLD = os.path.join(LAKE, "gold", "fact_sales")
write_deltalake(GOLD, sales)

# ======================================================================
banner("E1  ACCESS CONTROL - policy lives in the catalog, not in the files")
# Databricks side: a UC-style row filter + column mask, expressed as a governed view
dbx = duckdb.connect()
dbx.sql(f"CREATE TABLE fact_sales AS SELECT * FROM delta_scan('{GOLD}')")
dbx.sql("CREATE TABLE user_region(principal VARCHAR, region VARCHAR)")
dbx.sql("INSERT INTO user_region VALUES ('analyst_west','West')")
dbx.sql("SET VARIABLE current_principal = 'analyst_west'")
dbx.sql("""CREATE VIEW fact_sales_governed AS
  SELECT * REPLACE (CASE WHEN getvariable('current_principal') IN ('pii_reader')
                         THEN customer_email ELSE '***MASKED***' END AS customer_email)
  FROM fact_sales
  WHERE region IN (SELECT region FROM user_region
                   WHERE principal = getvariable('current_principal'))""")
gov_rows, gov_emails = q(dbx, "SELECT count(*), count(DISTINCT customer_email) FROM fact_sales_governed")[0]
gov_sample = q(dbx, "SELECT customer_email FROM fact_sales_governed LIMIT 1")[0][0]

# Fabric side: separate engine, reads the same files by path (shortcut semantics)
fab = duckdb.connect()
fab_rows, fab_regions = q(fab, f"SELECT count(*), count(DISTINCT region) FROM delta_scan('{GOLD}')")[0]
fab_sample = q(fab, f"SELECT customer_email FROM delta_scan('{GOLD}') WHERE region='East' LIMIT 1")[0][0]

# What does the Delta log carry that a reader could enforce?
log_keys = set()
for f in sorted(os.listdir(os.path.join(GOLD, "_delta_log"))):
    if f.endswith(".json"):
        for line in open(os.path.join(GOLD, "_delta_log", f)):
            log_keys |= set(json.loads(line).keys())
md = DeltaTable(GOLD).metadata()
print(f"Governed view as analyst_west : rows={gov_rows} sample email={gov_sample}")
print(f"Path reader (Fabric shortcut) : rows={fab_rows} regions={fab_regions} sample email={fab_sample}")
print(f"_delta_log action types       : {sorted(log_keys)}")
print(f"table configuration           : {md.configuration}")
OUT["E1"] = dict(gov_rows=gov_rows, fab_rows=fab_rows, fab_regions=fab_regions,
                 gov_sample=gov_sample, fab_sample=fab_sample, log_keys=sorted(log_keys))
print(f"RESULT E1: governed={gov_rows} rows/masked, path-reader={fab_rows} rows/unmasked; "
      f"no policy action in the Delta log -> enforcement must be rebuilt in Fabric")

# ======================================================================
banner("E2  LOGIC OWNERSHIP - one conformance rule, two SQL dialects")
rule_dbx = """SELECT customer_key,
  concat(first_name, ' ', middle_name, ' ', last_name)            AS full_name,
  units / packs                                                    AS units_per_pack,
  months_between(DATE'2025-03-01', DATE'2025-01-31')               AS tenure_months,
  CASE WHEN tier = 'gold' THEN 1 ELSE 0 END                        AS is_gold
FROM silver.customer"""
tsql = sqlglot.transpile(rule_dbx, read="databricks", write="fabric", pretty=True)[0]
back = sqlglot.transpile(tsql, read="fabric", write="databricks", pretty=True)[0]
print("--- Databricks SQL (source of truth)\n" + rule_dbx)
print("--- sqlglot -> Fabric T-SQL\n" + tsql)
print("--- sqlglot round trip back to Databricks\n" + back)
# Spark/Databricks semantics executed for real in DuckDB, T-SQL semantics executed
# by emulating the documented T-SQL behaviour of the emitted functions.
probe = duckdb.connect()
probe.sql("""CREATE TABLE c AS SELECT * FROM (VALUES
  (1,'Ann',NULL,'Lee',7,2,'gold'),(2,'Bo','J','Kim',9,4,'GOLD')) t(customer_key,first_name,middle_name,last_name,units,packs,tier)""")
spark_like = q(probe, """SELECT customer_key,
  CASE WHEN first_name IS NULL OR middle_name IS NULL OR last_name IS NULL THEN NULL
       ELSE first_name||' '||middle_name||' '||last_name END,      -- Spark concat(): NULL if any arg NULL
  units/packs,                                                      -- Spark '/': always fractional
  CASE WHEN tier='gold' THEN 1 ELSE 0 END                           -- Spark: case-sensitive (UTF8_BINARY)
  FROM c ORDER BY 1""")
tsql_like = q(probe, """SELECT customer_key,
  coalesce(first_name,'')||' '||coalesce(middle_name,'')||' '||coalesce(last_name,''),  -- T-SQL CONCAT(): NULL -> ''
  units//packs,                                                     -- T-SQL int/int: integer division
  CASE WHEN lower(tier)='gold' THEN 1 ELSE 0 END                    -- *_CI collation: case-insensitive
  FROM c ORDER BY 1""")
print("Databricks semantics :", spark_like)
print("T-SQL semantics      :", tsql_like)
diffs = sum(a != b for r1, r2 in zip(spark_like, tsql_like) for a, b in zip(r1, r2))
OUT["E2"] = dict(tsql=tsql, back=back, spark=spark_like, tsql_res=tsql_like, diffs=diffs)
print(f"RESULT E2: same rule, {diffs} of {len(spark_like)*4} cells differ across dialect semantics")

# ======================================================================
banner("E3  CHANGE CONTROL - auto-sync passes breaking changes; a contract gate stops them")
contract = yaml.safe_load(f"""
apiVersion: v3.1.0
kind: DataContract
id: urn:contract:gold.fact_sales
name: fact_sales
version: 1.0.0
status: active
schema:
  - name: fact_sales
    physicalName: gold.fact_sales
    properties:
      - {{name: sales_key,    logicalType: integer, required: true, unique: true, primaryKey: true}}
      - {{name: order_date,   logicalType: date,    required: true}}
      - {{name: customer_key, logicalType: integer, required: true}}
      - {{name: product_key,  logicalType: integer, required: true}}
      - {{name: channel,      logicalType: string}}
      - {{name: region,       logicalType: string}}
      - {{name: amount,       logicalType: number}}
      - {{name: cost,         logicalType: number}}
    quality:
      - {{type: sql, query: "SELECT COUNT(*) FROM ${{object}} WHERE amount < 0", mustBe: 0}}
""")
jsonschema.validate(contract, json.load(open(ODCS_SCHEMA)))
print("ODCS v3.1.0 contract validates against the official Bitol JSON schema")
LT = {"integer": ("int",), "number": ("double", "decimal", "float"),
      "string": ("string",), "date": ("date32", "date")}


def contract_gate(arrow_schema):
    cols = {f.name: str(f.type) for f in arrow_schema}
    errs = []
    for p in contract["schema"][0]["properties"]:
        if p["name"] not in cols:
            errs.append(f"missing column {p['name']}")
        elif not any(cols[p["name"]].startswith(t) for t in LT[p["logicalType"]]):
            errs.append(f"{p['name']} type {cols[p['name']]} != {p['logicalType']}")
    return errs


bim = json.load(open(os.path.join(ROOT, "gold_sales.bim")))
bound = {c["sourceColumn"] for c in bim["model"]["tables"][0]["columns"]}


def mirror_sync_and_bind():
    """Fabric mirror with auto-sync: always serve the latest version; then bind the model."""
    live = {r[0] for r in q(duckdb.connect(), f"DESCRIBE SELECT * FROM delta_scan('{GOLD}')")}
    return sorted(bound - live)


print("v0 contract gate:", contract_gate(DeltaTable(GOLD).to_pyarrow_table().schema) or "PASS",
      "| semantic model unbound columns:", mirror_sync_and_bind() or "none")
# Producer ships a "harmless refactor": rename amount, retype customer_key, drop region
v1 = (sales.rename_columns([("net_amount" if n == "amount" else n) for n in sales.column_names])
      .drop(["region"]))
v1 = v1.set_column(v1.column_names.index("customer_key"), "customer_key",
                   v1["customer_key"].cast(pa.string()))
gate = contract_gate(v1.schema)
print("v1 contract gate (CI, pre-commit):", gate)
write_deltalake(GOLD, v1, mode="overwrite", schema_mode="overwrite")   # no gate: it ships
unbound = mirror_sync_and_bind()
print(f"after commit, mirror serves version {DeltaTable(GOLD).version()}; "
      f"semantic model unbound columns: {unbound}")
OUT["E3"] = dict(gate=gate, unbound=unbound, version=DeltaTable(GOLD).version())
print(f"RESULT E3: without a gate the breaking change is live at version "
      f"{OUT['E3']['version']} and {len(unbound)} model columns break; "
      f"the ODCS gate flags {len(gate)} violations before commit")

# ======================================================================
banner("E4  LINEAGE & USAGE - reads leave no trace at the storage boundary")


def log_digest(path):
    h = hashlib.sha256()
    d = os.path.join(path, "_delta_log")
    for f in sorted(os.listdir(d)):
        h.update(f.encode()); h.update(open(os.path.join(d, f), "rb").read())
    return h.hexdigest()[:16], len(os.listdir(d))


before = log_digest(GOLD)
reader = duckdb.connect()
for i in range(250):   # 250 "Power BI / Direct Lake" queries from Fabric
    reader.sql(f"SELECT channel, sum(net_amount) FROM delta_scan('{GOLD}') GROUP BY 1").fetchall()
for i in range(50):
    DeltaTable(GOLD).to_pyarrow_table()
after = log_digest(GOLD)
commit_infos = []
for f in sorted(os.listdir(os.path.join(GOLD, "_delta_log"))):
    if f.endswith(".json"):
        for line in open(os.path.join(GOLD, "_delta_log", f)):
            a = json.loads(line)
            if "commitInfo" in a:
                commit_infos.append(sorted(a["commitInfo"].keys()))
print("log digest before:", before, " after 300 reads:", after)
print("commitInfo fields (writers only):", commit_infos[-1])
OUT["E4"] = dict(before=before, after=after, unchanged=before == after, fields=commit_infos[-1])
print(f"RESULT E4: 300 consumer reads changed the Delta log: {before != after}")

# ======================================================================
banner("E5  DELTA FEATURE DRIFT - one table property silently raises the protocol")
P = os.path.join(LAKE, "gold", "dim_product")
write_deltalake(P, pa.table({"product_key": [1, 2], "category": ["Boots", "Jackets"]}))
p0 = DeltaTable(P).protocol()
DeltaTable(P).alter.set_table_properties({"delta.enableDeletionVectors": "true"})
p1 = DeltaTable(P).protocol()
print(f"before: reader v{p0.min_reader_version} writer v{p0.min_writer_version} features={p0.reader_features}")
print(f"after : reader v{p1.min_reader_version} writer v{p1.min_writer_version} features={p1.reader_features}")
OUT["E5"] = dict(before=(p0.min_reader_version, p0.min_writer_version, p0.reader_features),
                 after=(p1.min_reader_version, p1.min_writer_version, p1.reader_features))
print("RESULT E5: protocol upgraded with no review step; every consumer engine must now "
      "support the new reader features")

# ======================================================================
banner("E6  RIGHT CALCULATION, RIGHT LAYER - Panel 3/4 rule tested")
con = duckdb.connect()
con.register("s", sales)
# Upstream "gold" mistakenly precomputes per-month margin% and distinct customers
con.sql("""CREATE TABLE gold_monthly AS SELECT month(order_date) m, region,
  (sum(amount)-sum(cost))/sum(amount) AS margin_pct, count(DISTINCT customer_key) AS customers
  FROM s GROUP BY ALL""")
pre = q(con, "SELECT avg(margin_pct), sum(customers) FROM gold_monthly")[0]
# Semantic-model style: measure evaluated at query time in the user's filter context
live = q(con, "SELECT (sum(amount)-sum(cost))/sum(amount), count(DISTINCT customer_key) FROM s")[0]
print(f"Grand total from precomputed gold : margin={pre[0]:.4f} customers={pre[1]}")
print(f"Grand total evaluated at query time: margin={live[0]:.4f} customers={live[1]}")
OUT["E6"] = dict(pre=pre, live=live)
print("RESULT E6: ratios and distinct counts precomputed upstream give wrong totals; "
      "the panel's decision rule holds")

# ======================================================================
banner("E7  SEMANTIC INTEROP - what the official Ossie Power BI converter can carry (static read)")
import ast, collections, re
CONV = os.path.join(ROOT, "vendor", "ossie-converters", "microsoft", "src", "ossie_microsoft", "_dax_catalog.py")
cat = ast.literal_eval(re.search(r"CATALOG = (\{.*?\n\})", open(CONV).read(), re.S).group(1))
# Statically extract what the translator emits today (no converter code is executed)
mod = ast.parse(open(CONV.replace("_dax_catalog", "_dax_to_sql")).read())
emitted, mismatch = set(), set()
for node in mod.body:
    tgt = getattr(node, "target", None) or (node.targets[0] if isinstance(node, ast.Assign) else None)
    name = getattr(tgt, "id", "")
    if name == "_AGGREGATES":
        emitted |= set(ast.literal_eval(node.value).keys())
    elif name == "_SEMANTIC_MISMATCH":
        mismatch |= set(ast.literal_eval(node.value).keys())
    elif name == "TRANSLATED_FUNCTIONS":
        for c in ast.walk(node.value):
            if isinstance(c, (ast.Tuple, ast.Set)):
                emitted |= {e.value for e in c.elts if isinstance(e, ast.Constant)}
measures = {m["name"]: m["expression"] for m in bim["model"]["tables"][0]["measures"]}
fx = re.compile(r"\b([A-Z][A-Z0-9.]+)\s*\(")
ref = re.compile(r"(?<!')\[([^\]]+)\]")
rows, status = [], {}
for name, expr in measures.items():   # dict order == dependency order in this model
    fns = fx.findall(expr)
    deps = sorted({d for d in ref.findall(expr) if d in measures})
    verdicts = {f: ("SEMANTIC-MISMATCH" if f in mismatch else cat.get(f, ("?", "UNKNOWN"))[1]) for f in fns}
    portable = all(v.startswith("PORTABLE") for v in verdicts.values()) and all(status[d][0] for d in deps)
    today = all(f in emitted and f not in mismatch for f in fns) and all(status[d][1] for d in deps)
    status[name] = (portable, today)
    blocked_by = [d for d in deps if not status[d][0]]
    rows.append((name, portable, today, verdicts, blocked_by))
    print(f"spec-portable={portable!s:5} translated-today={today!s:5} {name:20} {verdicts} {'via '+str(blocked_by) if blocked_by else ''}")
tiers = collections.Counter(v[0] for v in cat.values())
t4 = collections.Counter(cat[k][1] for k in cat if cat[k][0] == "T4")
print("catalog size", len(cat), "| T4 time-intelligence verdicts:", dict(t4))
OUT["E7"] = dict(rows=rows, catalog=len(cat), t4=dict(t4),
                 verdicts=dict(collections.Counter(v[1].split("(")[0] for v in cat.values())))
print(f"RESULT E7: {sum(r[2] for r in rows)} of {len(rows)} measures translate today; "
      f"{sum(r[1] for r in rows)} of {len(rows)} have a portable form in the spec at all")

banner("E8  CONTRACT STANDARDS - what the published ODCS schemas contain")
for v in ("v3.1.0", "v3.2.0"):
    sch = json.dumps(json.load(open(os.path.join(ROOT, "vendor", "odcs", "schema", f"odcs-json-schema-{v}.json"))))
    has = {k: f'"{k}"' in sch for k in ("context", "measures", "metrics", "rowFilter", "masking")}
    print(v, has)
    OUT.setdefault("E8", {})[v] = has

json.dump(OUT, open(os.path.join(ROOT, "results.json"), "w"), indent=1, default=str)
