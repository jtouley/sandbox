"""Spark side of the boundary lab, run by run.sh inside apache/spark (Azure Databricks stand-in).

Writes Gold tables with the Delta features Databricks turns on, into Azurite (wasb://),
and records what Spark itself sees so non-Spark readers can be checked against it.
"""
import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
LAKE = "wasb://lake@devstoreaccount1.blob.core.windows.net/gold"
truth = {"spark": spark.version}


def sql(s):
    return spark.sql(s)


def rows(s):
    return [list(r) for r in sql(s).collect()]


# ------------------------------------------------------------------ S1: dialect, executed
sql("""CREATE OR REPLACE TEMP VIEW customer AS SELECT * FROM VALUES
  (1,'Ann',CAST(NULL AS STRING),'Lee',7,2,'gold'),(2,'Bo','J','Kim',9,4,'GOLD')
  AS t(customer_key,first_name,middle_name,last_name,units,packs,tier)""")
truth["S1_rule"] = rows("""SELECT customer_key,
  concat(first_name,' ',middle_name,' ',last_name) AS full_name,
  units / packs AS units_per_pack,
  months_between(DATE'2025-03-01', DATE'2025-01-31') AS tenure_months,
  CASE WHEN tier = 'gold' THEN 1 ELSE 0 END AS is_gold
  FROM customer ORDER BY 1""")
# sqlglot's round-trip output (Fabric -> Databricks) for full_name
try:
    truth["S1_roundtrip_plus"] = rows("SELECT first_name + ' ' + middle_name + ' ' + last_name FROM customer")
except Exception as e:  # ANSI mode, as on Databricks SQL warehouses
    truth["S1_roundtrip_plus"] = "ERROR: " + str(e).split("\n")[0][:160]
spark.conf.set("spark.sql.ansi.enabled", "false")
truth["S1_roundtrip_plus_nonansi"] = rows("SELECT first_name + ' ' + middle_name + ' ' + last_name FROM customer")
spark.conf.set("spark.sql.ansi.enabled", "true")

# ------------------------------------------------------------------ S2: Delta features
base = """SELECT id AS sales_key, CAST(id % 3 AS INT) AS product_key,
          CASE WHEN id % 2 = 0 THEN 'West' ELSE 'East' END AS region,
          CAST(id * 10 AS DOUBLE) AS amount FROM range(1, 1001)"""
tables = {
    "plain": "",
    "deletion_vectors": "TBLPROPERTIES ('delta.enableDeletionVectors'='true')",
    "colmap_name": "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
    "colmap_id": "TBLPROPERTIES ('delta.columnMapping.mode'='id')",
    "liquid": "CLUSTER BY (region)",
}
for name, props in tables.items():
    path = f"{LAKE}/{name}"
    sql(f"CREATE OR REPLACE TABLE delta.`{path}` USING DELTA {props} AS {base}")

# Typical Databricks maintenance on each table
sql(f"DELETE FROM delta.`{LAKE}/deletion_vectors` WHERE sales_key <= 100")          # writes a DV
sql(f"DELETE FROM delta.`{LAKE}/plain` WHERE sales_key <= 100")                      # rewrites files
for t in ("colmap_name", "colmap_id"):
    sql(f"ALTER TABLE delta.`{LAKE}/{t}` RENAME COLUMN amount TO net_amount")       # metadata-only
sql(f"ALTER TABLE delta.`{LAKE}/colmap_name` DROP COLUMN region")                    # metadata-only

for name in tables:
    path = f"{LAKE}/{name}"
    df = spark.read.format("delta").load(path)
    detail = sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0].asDict()
    truth[name] = {
        "columns": df.columns,
        "rows": df.count(),
        "sum_last_col": float(df.selectExpr(f"sum(`{df.columns[-1]}`)").collect()[0][0]),
        "minReader": detail["minReaderVersion"], "minWriter": detail["minWriterVersion"],
        "features": sorted(detail.get("tableFeatures") or []),
    }

# ------------------------------------------------------------------ S3: governance in Spark
# Unity Catalog row filters/masks are not in OSS Spark. A dynamic view is the classic pre-UC
# Databricks pattern; it shows where the policy lives: in the catalog object, not the files.
sql(f"""CREATE OR REPLACE TEMP VIEW plain_governed AS
  SELECT sales_key, product_key, region, amount FROM delta.`{LAKE}/plain`
  WHERE region = 'West'""")
truth["S3"] = {"governed_rows": sql("SELECT count(*) FROM plain_governed").collect()[0][0],
               "path_rows": spark.read.format("delta").load(f"{LAKE}/plain").count()}

json.dump(truth, open("/work/spark_truth.json", "w"), indent=1, default=str)
print(json.dumps(truth, indent=1, default=str))
