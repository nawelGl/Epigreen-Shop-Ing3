#!/usr/bin/env bash
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"

NN_URI="hdfs://vm-datalake:9000"
RAW_BASE="/datalake/nawres/raw/nyc_taxi/yellow"
CUR_BASE="/datalake/nawres/curated/product_ec_base"

# Détection du dernier mois disponible dans RAW via hdfs dfs -ls -R
LATEST_YM="$(
  if command -v hdfs >/dev/null 2>&1; then
    hdfs dfs -ls -R "${RAW_BASE}" 2>/dev/null \
      | awk '{print $NF}' \
      | grep -E 'year=[0-9]{4}/month=[0-9]{2}$' \
      | sed -E 's#.*/year=([0-9]{4})/month=([0-9]{2})#\1-\2#' \
      | sort -V \
      | tail -n 1
  else
    echo ""
  fi
)"

if [[ -z "${LATEST_YM}" ]]; then
  LATEST_YM="2025-11"
fi

YEAR="${LATEST_YM%-*}"
MONTH="${LATEST_YM#*-}"

RAW_PATH="${NN_URI}${RAW_BASE}/year=${YEAR}/month=${MONTH}"
CUR_PATH="${NN_URI}${CUR_BASE}/year=${YEAR}/month=${MONTH}"

echo "===== DEMO AVANT / APRES ====="
echo "RAW     = ${RAW_PATH}"
echo "CURATED = ${CUR_PATH}"
echo

TMP_SCALA="/tmp/demo_before_after.scala"
cat > "${TMP_SCALA}" <<SCALA
import org.apache.log4j.{Level, Logger}
Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

val rawPath = "${RAW_PATH}"
val curPath = "${CUR_PATH}"

println("---- RAW schema ----")
val raw = spark.read.parquet(rawPath)
raw.printSchema()

println("---- RAW sample (5) ----")
raw.show(5, false)

println("RAW count = " + raw.count())

println("\\n---- CURATED schema ----")
val curated = spark.read.parquet(curPath)
curated.printSchema()

println("---- CURATED sample (5) ----")
curated.show(5, false)

println("CURATED count = " + curated.count())

println("\\n===== FIN DEMO =====")
System.exit(0)
SCALA

# Exécution Spark (silencieux)
"${SPARK_HOME}/bin/spark-shell" \
  --master "spark://vm-spark-master:7077" \
  --conf "spark.ui.enabled=false" \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.eventLog.enabled=false" \
  -i "${TMP_SCALA}" \
  2>&1 | egrep -v "(INFO |WARN |NativeCodeLoader|TaskSchedulerImpl|TaskSetManager|SparkContext|Standalone|TransportClient|Jetty|BlockManager|ShutdownHookManager|Registering|Executor|Scheduler|Created local directory|Using Spark's default log4j profile)"