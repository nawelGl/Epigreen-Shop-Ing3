#!/usr/bin/env bash
set -euo pipefail
#############################################
# CONFIG
#############################################
ORCH_USER="vm-source-orchestration"
ORCH_HOST="172.31.249.194" # orchestration VM IP
REMOTE_CSV_PATH="/opt/data_sources/sales/daily_sales.csv"
REMOTE_VENV_PATH="/home/vm-source-orchestration/airflow/venv"
DAG_ID="sales_pipeline"
#############################################
# 1) fichier CSV tempo (on va remplacer celui de vm)
#############################################
TMP_CSV="$(mktemp)"
cat > "$TMP_CSV" <<EOF
date,country,amount
2025-12-5,FR,40
2025-12-5,US,80
2025-12-5,UK,100
EOF
echo "=== Ok pour le csv tempo en local==="
cat "$TMP_CSV"
echo
#############################################
# 2) scp le fichier csv vers la vm airflow
#############################################

scp "$TMP_CSV" "${ORCH_USER}@${ORCH_HOST}:${REMOTE_CSV_PATH}"
echo "${ORCH_USER}@${ORCH_HOST}:${REMOTE_CSV_PATH} finished uploading file"
echo
#############################################
# 3) venv activate + DAG trigger
#############################################
ssh "${ORCH_USER}@${ORCH_HOST}" bash -lc "
set -euo pipefail
source '${REMOTE_VENV_PATH}/bin/activate'
echo \"Airflow version: \$(airflow version)\"
echo

if pgrep -f 'airflow scheduler' > /dev/null; then
echo 'scheduler déjà actif '
else
echo 'scheduler non actif → démarrage...'
airflow scheduler -D
fi
echo
echo '→ DAG trigger: ${DAG_ID}'
airflow dags trigger '${DAG_ID}'
echo 'DAG trigger done'
"
echo
echo "=== fin de démo ==="

