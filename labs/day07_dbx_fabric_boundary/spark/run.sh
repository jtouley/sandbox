#!/usr/bin/env bash
# Run a PySpark script in the apache/spark image as a stand-in for an Azure Databricks cluster:
# Spark 4.0.1 + Delta 4.0 (≈ DBR 17 line), storage = Azurite (Azure Blob emulator) via wasb://.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; JARS="${JARS:?set JARS to the downloaded jar dir}"
docker run --rm --user 0 --network host -v "$HERE":/work -v "$JARS":/extra:ro -e HOME=/tmp -w /work \
  apache/spark:4.0.1-scala2.13-java17-python3-ubuntu \
  /opt/spark/bin/spark-submit --master 'local[2]' \
  --jars "$(ls "$JARS"/*.jar | sed "s#$JARS#/extra#" | paste -sd,)" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.azure.storage.emulator.account.name=devstoreaccount1.blob.core.windows.net \
  --conf spark.sql.ansi.enabled=true \
  --conf spark.ui.enabled=false \
  "$@" 2>&1 | grep -vE "WARN|INFO|^\s*$|NativeCodeLoader|log4j|\"level\": \"ERROR\"|Prepared|Actual:"
