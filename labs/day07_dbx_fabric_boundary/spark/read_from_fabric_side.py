"""Fabric-side readers of the Spark-written Gold tables in Azurite, compared with Spark's own view.

Readers: delta-rs (deltalake) and DuckDB's delta extension (delta-kernel-rs), both over Azure Blob
(Azurite). Neither is Fabric's engine; they stand in for "a second engine reading in place".
"""
import json
import os

import duckdb
from deltalake import DeltaTable

HERE = os.path.dirname(os.path.abspath(__file__))
truth = json.load(open(os.path.join(HERE, "spark_truth.json")))
AZ = {"use_emulator": "true", "account_name": "devstoreaccount1"}
CONN = ("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
con = duckdb.connect()
con.sql("INSTALL azure; INSTALL delta; LOAD azure; LOAD delta")
con.sql(f"CREATE SECRET az (TYPE azure, CONNECTION_STRING '{CONN}')")


def via_deltars(name):
    t = DeltaTable(f"az://lake/gold/{name}", storage_options=AZ).to_pyarrow_table()
    last = t.column_names[-1]
    nulls = sum(t[c].null_count for c in t.column_names)
    return t.column_names, t.num_rows, float(sum(v for v in t[last].to_pylist() if v is not None)), nulls


def via_duckdb(name):
    rel = con.sql(f"SELECT * FROM delta_scan('az://lake/gold/{name}')")
    cols = rel.columns
    nn = " + ".join(f'count(*) - count("{c}")' for c in cols)
    n, s, nulls = con.sql(f'SELECT count(*), sum("{cols[-1]}"), {nn} FROM delta_scan(\'az://lake/gold/{name}\')').fetchall()[0]
    return cols, n, float(s or 0), nulls


results = {}
print(f"{'table':18} {'reader':8} {'match':6} detail")
for name in ("plain", "deletion_vectors", "colmap_name", "colmap_id", "liquid"):
    want = truth[name]
    for reader, fn in (("delta-rs", via_deltars), ("duckdb", via_duckdb)):
        try:
            cols, n, s, nulls = fn(name)
            ok = cols == want["columns"] and n == want["rows"] and abs(s - want["sum_last_col"]) < 1e-6 and nulls == 0
            detail = "" if ok else (f"no error, but {nulls} of {n * len(cols)} values NULL; rows={n}, sum={s:.0f} "
                                    f"(spark: rows={want['rows']}, sum={want['sum_last_col']:.0f}, 0 NULL)")
        except Exception as e:
            ok, detail = False, "ERROR " + str(e).split("\n")[0][:150]
        results[f"{name}/{reader}"] = {"ok": ok, "detail": detail}
        print(f"{name:18} {reader:8} {str(ok):6} {detail}")
json.dump(results, open(os.path.join(HERE, "fabric_side_results.json"), "w"), indent=1)
