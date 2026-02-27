## Scénarios de démonstration – Release finale

---

### Scénario 1 — Exécution nominale (1 worker)

**Objectif :**  
Mesurer la performance minimale du cluster avec un seul worker.

**Déroulé :**
- Déclenchement du **JOB 2 : calcul des scores**
- Cluster Spark configuré avec **1 worker**
- Spark :
  - lit les données depuis HDFS
  - calcule les scores environnementaux
  - écrit les résultats dans PostgreSQL
- Fin du job :
  - statut `SUCCESS`
  - métriques disponibles dans Grafana

**Éléments montrés en démonstration :**
- Spark UI avec 1 worker actif
- Temps total d’exécution
- Métrique `job2_duration_seconds`
- Statut SUCCESS

---

### Scénario 2 — Exécution nominale (3 workers)

**Objectif :**  
Comparer les performances avec un parallélisme intermédiaire.

**Déroulé :**
- Relance du **même JOB 2**
- Cluster configuré avec **3 workers**
- Données et logique identiques au scénario 1

**Éléments montrés :**
- Spark UI avec 3 workers actifs
- Temps d’exécution réduit
- Comparaison avec scénario 1
- Statut SUCCESS

---

### Scénario 3 — Exécution nominale (6 workers)

**Objectif :**  
Analyser le gain supplémentaire avec un cluster plus large.

**Déroulé :**
- Relance du **JOB 2**
- Cluster configuré avec **6 workers**

**Éléments montrés :**
- Spark UI avec 6 workers actifs
- Temps d’exécution
- Comparaison :
  - 1 worker
  - 3 workers
  - 6 workers

**Analyse attendue :**
- Identifier le meilleur compromis performance / ressources
- Observer un éventuel rendement décroissant

---

### Scénario 4 — Tolérance aux pannes (Fault Tolerance)

**Objectif :**  
Démontrer la résilience native de Spark.

**Déroulé :**
- Lancement du **JOB 2** avec **3 workers**
- Pendant l’exécution :
  - arrêt volontaire d’un worker (`./stop-worker.sh`)
- Observation dans Spark UI :
  - worker passe en `DEAD`
  - tâches redistribuées automatiquement
- Le job se termine avec succès

**Comportement attendu :**
- Pas d’arrêt global du job
- Réallocation automatique des tâches
- Statut final `SUCCESS`

**Éléments montrés en démonstration :**
- Worker en `DEAD` dans Spark UI
- Job qui continue
- Job terminé avec succès malgré la perte d’un worker

---

### Résumé des scénarios

| Scénario | Objectif principal |
|----------|-------------------|
| Scénario 1 | Baseline performance (1 worker) |
| Scénario 2 | Scalabilité avec 3 workers |
| Scénario 3 | Analyse du gain avec 6 workers |
| Scénario 4 | Tolérance aux pannes Spark |