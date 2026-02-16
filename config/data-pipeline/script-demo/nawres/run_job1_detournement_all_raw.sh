#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# JOB1 - Déournement NYC Taxi -> product_ec
# Boucle sur toutes les partitions RAW (year/month)
# et écrit en CURATED (overwrite) pour chaque mois.
# ==========================================

export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop/hadoop-3.3.6
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin"

NN_URI="hdfs://vm-datalake:9000"
RAW_BASE="/datalake/nawres/raw/nyc_taxi/yellow"
CUR_BASE="/datalake/nawres/curated/product_ec_base"

N_PRODUCT_REF="${N_PRODUCT_REF:-42019}"
N_WAREHOUSE="${N_WAREHOUSE:-200}"
PRESENCE_PCT="${PRESENCE_PCT:-50}"

echo "[JOB1-ALL] Listing RAW partitions from ${NN_URI}${RAW_BASE}..."

# Liste des partitions RAW au format: "YYYY MM"
PARTS="$(
  hdfs dfs -ls "${NN_URI}${RAW_BASE}" 2>/dev/null \
    | awk '{print $NF}' \
    | grep -E 'year=[0-9]{4}$' \
    | while read -r YPATH; do
        YEAR="$(echo "$YPATH" | sed -E 's#.*/year=([0-9]{4})#\1#')"
        hdfs dfs -ls "${NN_URI}${RAW_BASE}/year=${YEAR}" 2>/dev/null \
          | awk '{print $NF}' \
          | grep -E 'month=[0-9]{2}$' \
          | while read -r MPATH; do
              MONTH="$(echo "$MPATH" | sed -E 's#.*/month=([0-9]{2})#\1#')"
              echo "${YEAR} ${MONTH}"
            done
      done \
    | sort -k1,1n -k2,2n
)"

if [[ -z "${PARTS}" ]]; then
  echo "[JOB1-ALL] ERROR: no RAW partitions found."
  exit 1
fi

echo "[JOB1-ALL] Partitions found:"
echo "${PARTS}" | awk '{print "  - "$1"-"$2}'

echo
echo "[JOB1-ALL] Running detournement for each month..."

while read -r YEAR MONTH; do
  [[ -z "${YEAR}" ]] && continue

  INPUT="${NN_URI}${RAW_BASE}/year=${YEAR}/month=${MONTH}"
  OUTPUT="${NN_URI}${CUR_BASE}/year=${YEAR}/month=${MONTH}"

  echo
  echo "============================================================"
  echo "[JOB1-ALL] YEAR=${YEAR} MONTH=${MONTH}"
  echo "[JOB1-ALL] INPUT  = ${INPUT}"
  echo "[JOB1-ALL] OUTPUT = ${OUTPUT}"
  echo "============================================================"

  spark-submit \
    --master spark://vm-spark-master:7077 \
    --conf spark.ui.showConsoleProgress=false \
    --conf spark.eventLog.enabled=false \
    --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:/home/hadoop/jobs/log4j2-silent.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j2.configurationFile=file:/home/hadoop/jobs/log4j2-silent.properties" \
    /home/hadoop/jobs/job1_detournement.py \
    --input "$INPUT" \
    --output "$OUTPUT" \
    --n-product-ref "$N_PRODUCT_REF" \
    --n-warehouse "$N_WAREHOUSE" \
    --presence-pct "$PRESENCE_PCT"

done <<< "${PARTS}"

echo
echo "[JOB1-ALL] DONE for all RAW partitions."