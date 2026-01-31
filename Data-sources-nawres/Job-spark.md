# Workflow de traitement – Détournement des données  + Scoring (Spark)



## Job Spark 1 — Nettoyage & transformation -> Detournement (RAW → CURATED)

**Entrée :**
- HDFS `/datalake/raw/nyc_taxi/yellow/`

**Traitements :**
- Lecture des fichiers Parquet
- Suppression des colonnes inutiles
- Renommage / remapping des colonnes vers le modèle cible
- Calculs intermédiaires nécessaires (distance, durée, valeurs techniques)
- Mise en forme cohérente avec la table cible `product_ec`

**Sortie :**
- HDFS `/datalake/curated/product_ec_base/`

À la fin de ce job :
- les données sont propres
- les colonnes sont prêtes
- aucune agrégation métier n’est encore faite

---

## Job Spark 2 — Agrégation & scoring (CURATED → BASE MÉTIER)

**Entrée :**
- HDFS `/datalake/curated/product_ec_base/`

**Traitements :**
- Calcul de l’empreinte totale (`ec_total`) par ligne
- Agrégation par `id_product_ref`
- Calcul de la moyenne des impacts
- Attribution d’un score environnemental (A → E)

**Sortie :**
- Insertion dans PostgreSQL :
  - table `product_ec`
  - mise à jour du score dans `ref_product_catalog`



---

## Résumé du workflow
```
HDFS RAW
↓
Job Spark 1 (clean + remap)
↓
HDFS CURATED
↓
Job Spark 2 (agrégations + scoring)
↓
PostgreSQL (tables métier)
```

--- 
## Workflow – Déclenchement du calcul & notifications

```text
[IHM Admin]
    |
    | (clic "Calculer les scores")
    v
[ms-service-spark-batch]
    |
    | submit Spark Job (Job 2)
    v
[Spark Cluster]
    |
    | code de sortie / état du job
    v
[ms-service-spark-batch]
    |
    | publication d’événement
    v
[Kafka]
   |              |
   |              |
[topic-success]  [topic-error]
   |              |
   v              v
[IHM Admin]      [IHM Admin]