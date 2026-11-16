#!/usr/bin/env bash
# Start the local "Azure" stack: Azurite (Blob emulator), SQL Server 2022 (T-SQL engine),
# and download the jars Spark needs for Delta + wasb://. Requires a running Docker daemon.
set -euo pipefail
JARS="${JARS:-$(pwd)/jars}"; mkdir -p "$JARS"
docker pull -q apache/spark:4.0.1-scala2.13-java17-python3-ubuntu
docker pull -q mcr.microsoft.com/azure-storage/azurite:latest
docker pull -q mcr.microsoft.com/mssql/server:2022-latest
docker rm -f azurite mssql >/dev/null 2>&1 || true
docker run -d --name azurite --network host mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0 --loose --skipApiVersionCheck
docker run -d --name mssql --network host -e ACCEPT_EULA=Y -e 'MSSQL_SA_PASSWORD=Lab_Passw0rd!' \
  mcr.microsoft.com/mssql/server:2022-latest
for a in io.delta:delta-spark_2.13:4.0.0 io.delta:delta-storage:4.0.0 \
         org.apache.hadoop:hadoop-azure:3.4.1 com.microsoft.azure:azure-storage:7.0.1 \
         com.microsoft.azure:azure-keyvault-core:1.0.0 org.apache.hadoop.thirdparty:hadoop-shaded-guava:1.3.0 \
         org.eclipse.jetty:jetty-util-ajax:9.4.56.v20240826 org.eclipse.jetty:jetty-util:9.4.56.v20240826; do
  mvn -q dependency:copy -Dartifact="$a" -DoutputDirectory="$JARS"
done
echo "Ready. Next: JARS=$JARS ./run.sh boundary_spark.py && python3 read_from_fabric_side.py && python3 tsql_side.py"
