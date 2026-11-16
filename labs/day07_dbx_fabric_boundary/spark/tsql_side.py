"""Run the conformance rule as T-SQL on a real SQL Server 2022 (docker 'mssql'), and compare with
what Spark returned (spark_truth.json). Fabric Warehouse speaks T-SQL; its default collation is
Latin1_General_100_BIN2_UTF8 (case-sensitive), so both CS and CI tables are tested.
"""
import json
import os
import subprocess

import sqlglot

HERE = os.path.dirname(os.path.abspath(__file__))
truth = json.load(open(os.path.join(HERE, "spark_truth.json")))


def tsql(q):
    r = subprocess.run(["docker", "exec", "mssql", "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost",
                        "-U", "sa", "-P", "Lab_Passw0rd!", "-C", "-d", "lab", "-h", "-1", "-W", "-s", "|",
                        "-Q", "SET NOCOUNT ON; " + q], capture_output=True, text=True)
    out = (r.stdout + r.stderr).strip()
    return out


subprocess.run(["docker", "exec", "mssql", "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
                "-P", "Lab_Passw0rd!", "-C", "-Q", "IF DB_ID('lab') IS NULL CREATE DATABASE lab"],
               capture_output=True)
for coll, tbl in (("Latin1_General_100_BIN2_UTF8", "customer_cs"), ("SQL_Latin1_General_CP1_CI_AS", "customer_ci")):
    tsql(f"""DROP TABLE IF EXISTS {tbl};
      CREATE TABLE {tbl}(customer_key INT, first_name VARCHAR(20) COLLATE {coll}, middle_name VARCHAR(20) COLLATE {coll},
        last_name VARCHAR(20) COLLATE {coll}, units INT, packs INT, tier VARCHAR(10) COLLATE {coll});
      INSERT INTO {tbl} VALUES (1,'Ann',NULL,'Lee',7,2,'gold'),(2,'Bo','J','Kim',9,4,'GOLD');""")

rule_dbx = """SELECT customer_key,
  concat(first_name, ' ', middle_name, ' ', last_name) AS full_name,
  units / packs AS units_per_pack,
  months_between(DATE'2025-03-01', DATE'2025-01-31') AS tenure_months,
  CASE WHEN tier = 'gold' THEN 1 ELSE 0 END AS is_gold
FROM customer ORDER BY 1"""
hand_port = """SELECT customer_key,
  CONCAT(first_name, ' ', middle_name, ' ', last_name) AS full_name,
  units / packs AS units_per_pack,
  DATEDIFF(month, '2025-01-31', '2025-03-01') AS tenure_months,
  CASE WHEN tier = 'gold' THEN 1 ELSE 0 END AS is_gold
FROM {t} ORDER BY 1"""
transpiled = sqlglot.transpile(rule_dbx, read="databricks", write="fabric")[0]

res = {"spark": truth["S1_rule"], "sqlglot_tsql": transpiled}
print("Spark (Databricks semantics, ANSI on):", truth["S1_rule"])
for t in ("customer_cs", "customer_ci"):
    res[f"hand_port/{t}"] = tsql(hand_port.format(t=t))
    print(f"\nHand-written T-SQL port on {t}:\n{res[f'hand_port/{t}']}")
res["sqlglot_on_sqlserver"] = tsql(transpiled.replace("FROM customer", "FROM customer_cs"))
print("\nsqlglot's T-SQL, run verbatim:\n" + transpiled + "\n->\n" + res["sqlglot_on_sqlserver"])
no_mb = transpiled.replace("MONTHS_BETWEEN(CAST('2025-03-01' AS DATE), CAST('2025-01-31' AS DATE))", "NULL")
res["sqlglot_without_months_between"] = tsql(no_mb.replace("FROM customer", "FROM customer_cs"))
print("\nsqlglot's T-SQL with MONTHS_BETWEEN removed:\n" + res["sqlglot_without_months_between"])
json.dump(res, open(os.path.join(HERE, "tsql_results.json"), "w"), indent=1)
