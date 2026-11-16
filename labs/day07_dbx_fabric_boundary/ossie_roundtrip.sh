#!/usr/bin/env bash
# E9: Power BI -> Ossie -> Databricks Metric View -> Ossie -> Power BI, using the official
# apache/ossie-converters (Microsoft: Python; Databricks: Java, built with `mvn -DskipTests package`).
# Each manual bridge step below is a boundary break that the converters do not handle.
set -uo pipefail
cd "$(dirname "$0")"
OUT=roundtrip_out; rm -rf "$OUT"; mkdir -p "$OUT"
MS="python3 -m ossie_microsoft.cli"; export PYTHONPATH=vendor/ossie-converters/microsoft/src
JAR=vendor/ossie-converters/databricks/java/target/ossie-databricks-converter-0.1.0-SNAPSHOT.jar
step() { echo; echo "### $*"; }

python3 make_bim.py >/dev/null
step "1  Power BI -> Ossie (--translate-dax)"
$MS import -i gold_sales.bim -o $OUT/1.ossie.yaml --translate-dax 2>&1

step "2  Ossie -> Power BI, same converter (stash intact)"
$MS export -i $OUT/1.ossie.yaml -o $OUT/2.roundtrip.bim 2>&1 | tail -1

step "3  Ossie -> Databricks Metric View, as produced"
java -jar $JAR export $OUT/1.ossie.yaml -o $OUT/3.mv.yaml 2>&1 | grep -v JAVA_TOOL

step "3b bridge: 2-part Fabric names -> 3-part UC names; de-duplicate dimension names"
python3 - "$OUT" <<'EOF'
import sys, yaml
o = sys.argv[1]; d = yaml.safe_load(open(f"{o}/1.ossie.yaml"))
for ds in d["datasets"]:
    ds["source"] = "main." + ds["source"]
    if ds["name"] != "Sales":
        for f in ds.get("fields", []):
            f["name"] = ds["name"] + f["name"]
yaml.safe_dump(d, open(f"{o}/3b.ossie.yaml", "w"), sort_keys=False)
EOF
java -jar $JAR export $OUT/3b.ossie.yaml -o $OUT/3b.mv.yaml 2>&1 | grep -v JAVA_TOOL

step "4  Metric View -> Ossie (Databricks converter)"
java -jar $JAR import $OUT/3b.mv.yaml -o $OUT/4.ossie.yaml 2>&1 | grep -v JAVA_TOOL

step "5  Ossie -> Power BI, as produced"
$MS export -i $OUT/4.ossie.yaml -o $OUT/5.bim 2>&1

step "5b bridge: drop legacy custom_extensions (Ossie 0.2 -> 0.3)"
python3 - "$OUT" <<'EOF'
import sys, yaml
o = sys.argv[1]; d = yaml.safe_load(open(f"{o}/4.ossie.yaml"))
def strip(x):
    if isinstance(x, dict):
        x.pop("custom_extensions", None); [strip(v) for v in x.values()]
    elif isinstance(x, list):
        [strip(v) for v in x]
strip(d); yaml.safe_dump(d, open(f"{o}/4b.ossie.yaml", "w"), sort_keys=False)
EOF
$MS export -i $OUT/4b.ossie.yaml -o $OUT/5b.bim 2>&1 | grep -v "home table"

step "Summary"
python3 - "$OUT" <<'EOF'
import json, sys
o = sys.argv[1]
def measures(p):
    b = json.load(open(p, encoding="utf-8-sig"))
    return {m["name"]: m["expression"] for t in b["model"]["tables"] for m in t.get("measures", [])}, b["model"]
src, srcm = measures("gold_sales.bim")
rt, rtm = measures(f"{o}/2.roundtrip.bim")
full, fullm = measures(f"{o}/5b.bim")
print(f"PBI -> Ossie -> PBI           : {sum(src[k] == rt.get(k) for k in src)}/{len(src)} measures identical, roles kept: {bool(rtm.get('roles'))}")
working = {k: v for k, v in full.items() if v.strip() != "BLANK()"}
print(f"PBI -> Ossie -> MV -> Ossie -> PBI: {len(full)}/{len(src)} measures come back, {len(working)} non-BLANK, "
      f"roles kept: {bool(fullm.get('roles'))}, relationships: {len(fullm.get('relationships', []))}/{len(srcm['relationships'])}")
for k, v in full.items():
    print(f"   {k:12} {v}")
EOF
