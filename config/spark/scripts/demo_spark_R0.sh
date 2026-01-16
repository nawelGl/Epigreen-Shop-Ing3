#!/bin/bash

##############################################
#  Démo R0 – Spark + Carbon-DB (PostgreSQL)  #
##############################################

# --- CONFIG A ADAPTER SI BESOIN ---
MASTER_URL="spark://172.31.253.136:7077"
JOB_PATH="$HOME/spark_compute_ec_total.py"

DB_HOST="172.31.250.18"
DB_NAME="carbon_db"
DB_USER="postgres"
DB_PASSWORD="toto"  
DB_TABLE_IN="products"
DB_TABLE_OUT="products_with_ec_total"
# ---------------------------------

# Couleurs
C_RESET="\e[0m"
C_TITLE="\e[95m"
C_STEP="\e[96m"
C_OK="\e[92m"
C_WARN="\e[93m"
C_ERR="\e[91m"

line() { echo -e "${C_TITLE}------------------------------------------------------------${C_RESET}"; }

echo -e "${C_TITLE}"
echo "    Démo R0 – Calcul d’empreinte carbone avec Spark"
echo -e "${C_RESET}"
line

echo -e "${C_STEP}[1/4] Contexte de la démo${C_RESET}"
echo -e "  • Master Spark : ${C_OK}$MASTER_URL${C_RESET}"
echo -e "  • Carbon-DB    : ${C_OK}$DB_HOST / $DB_NAME${C_RESET}"
echo -e "  • Table source : ${C_OK}$DB_TABLE_IN${C_RESET}"
echo -e "  • Table sortie : ${C_OK}$DB_TABLE_OUT${C_RESET}"
line

# Petit helper pour exécuter psql si dispo
psql_cmd() {
  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "$1"
  else
    echo -e "${C_WARN}[WARN] psql n'est pas installé sur cette VM, affichage SQL ignoré.${C_RESET}"
  fi
}

echo -e "${C_STEP}[2/4] État de la table AVANT le job Spark${C_RESET}"
psql_cmd "SELECT * FROM $DB_TABLE_IN ORDER BY id;"

line
echo -e "${C_STEP}[3/4] Lancement du job Spark${C_RESET}"
echo -e "  ➤ Fichier Python : ${C_OK}$JOB_PATH${C_RESET}"
echo -e "  ➤ Ouverture de l'UI Spark possible sur : ${C_OK}http://$(hostname -I | awk '{print $1}'):8080${C_RESET}"
echo ""

if [ ! -f "$JOB_PATH" ]; then
  echo -e "${C_ERR}[ERREUR] Le fichier $JOB_PATH n'existe pas.${C_RESET}"
  exit 1
fi

# Lancement du job
spark-submit --master "$MASTER_URL" "$JOB_PATH"
RET=$?

if [ $RET -ne 0 ]; then
  echo -e "${C_ERR}[ERREUR] Le job Spark s'est terminé avec un code $RET.${C_RESET}"
  exit $RET
fi

echo -e "${C_OK}[OK] Job Spark terminé avec succès.${C_RESET}"
line

echo -e "${C_STEP}[4/4] État de la table APRÈS le job Spark${C_RESET}"
psql_cmd "SELECT * FROM $DB_TABLE_OUT ORDER BY id;"

line
echo -e "${C_OK}Démo terminée : le cluster Spark a lu PostgreSQL, calculé ec_total, et écrit le résultat dans $DB_TABLE_OUT.${C_RESET}"
line
