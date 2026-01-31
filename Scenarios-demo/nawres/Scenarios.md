## Scénarios de démonstration – Release finale

---

### Scénario 1 — Exécution nominale (cluster Spark : 3 workers)

**Objectif :**  
Montrer le fonctionnement normal du pipeline de calcul avec notification à l’administrateur.

**Déroulé :**
- L’administrateur déclenche le **JOB 2 : calcul des scores**
- Le `ms-service-spark-batch` soumet le job au cluster Spark (3 workers)
- Spark :
  - lit les données Parquet depuis HDFS (zone curated)
  - calcule l’empreinte carbone totale par ligne
  - calcule la moyenne par `id_product_ref`
  - attribue un score environnemental (A → E)
- Fin du job :
  - publication d’un message dans Kafka (`topic_success`)
  - la notification est consommée et **remontée à l’IHM admin**

**Éléments montrés en démonstration :**
- Spark UI avec 3 workers actifs
- Temps d’exécution du job
- Message de succès dans Kafka
- Notification visible côté administrateur
- Données finales disponibles

---

### Scénario 2 — Comparaison de performances (3 workers vs 6 workers)

**Objectif :**  
Démontrer l’impact du parallélisme Spark sur le temps d’exécution.

**Déroulé :**
- Relance du **même JOB 2**
- Cluster Spark configuré avec **6 workers**
- Données et logique métier identiques au scénario 1
- Fin du job :
  - publication d’un message dans Kafka (`topic_success`)
  - la notification est **remontée à l’IHM admin**

**Éléments montrés en démonstration :**
- Spark UI avec 6 workers actifs
- Temps d’exécution réduit
- Comparaison des temps :
  - 3 workers vs 6 workers
- Notification de succès côté administrateur

---

### Scénario 3 — Gestion d’erreur et notification

**Objectif :**  
Montrer la capacité du système à gérer un échec et à notifier l’administrateur.

**Déroulé :**
- L’administrateur déclenche le **JOB 2**
- Pendant l’exécution :
  - arrêt volontaire du Spark Master **ou**
  - arrêt d’un worker Spark
- Le job échoue

**Comportement attendu :**
- Le `ms-service-spark-batch` détecte l’échec du job
- Envoi d’un message dans Kafka (`topic_failure`)
- L’administrateur est informé de l’échec
- Possibilité de relancer le calcul

**Éléments montrés en démonstration :**
- Job en échec visible dans Spark UI
- Message d’erreur dans Kafka
- Notification d’échec côté administration

---

### Résumé des scénarios

| Scénario | Objectif principal |
|--------|-------------------|
| Scénario 1 | Pipeline fonctionnel avec 3 workers |
| Scénario 2 | Comparaison de performance avec 6 workers |
| Scénario 3 | Gestion des erreurs et notifications |