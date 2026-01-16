# Déploiement d’un cluster Apache Spark en mode Standalone

## 1. Initiation au cluster spark
Ce qui a été fait : 
- creation d'1 master et de 2 workers 
- Job python de test pour montrer que le calculs a été separé entre les 2 workers

Ce qu'il reste a faire: 
- Fichiers de conf à retravailler.
- Creer le rester des vm avec le reste des workers
- ...

 

---

## 2. Architecture du cluster

Le cluster Spark est déployé en mode **Standalone** et est composé de trois machines virtuelles :

- **Spark Master**
  - Nom : `vm-spark-master`
  - Adresse IP : `172.31.253.136`
  - Interface Web : `http://172.31.253.136:8080`

- **Spark Worker 1**
  - Nom : `vm-spark-worker1`
  - Adresse IP : `172.31.253.248`

- **Spark Worker 2**
  - Nom : `vm-spark-worker2`
  - Adresse IP : `172.31.252.235`

Les Spark Workers sont connectés au Spark Master et visibles dans l’interface Web du Master.

---

## 3. Principe de fonctionnement

En mode Standalone, les Spark Workers ne sont pas configurés via des fichiers dédiés.  
Ils héritent des paramètres communs définis dans les fichiers `spark-env.sh` et `spark-defaults.conf`, puis s’enregistrent dynamiquement auprès du Spark Master lors de leur démarrage.

Cette approche permet une architecture flexible et facilite l’ajout ou la suppression de workers sans modification de la configuration du Master.

---

## 4. Démarrage du cluster

Le Spark Master est démarré sur la machine `vm-spark-master` à l’aide de la commande :

```bash
/opt/spark/sbin/start-master.sh
```

---
## Accès au Spark Master

Le Spark Master est accessible via son interface Web, permettant de superviser l’état du cluster, les Spark Workers connectés et les applications Spark exécutées.

- **Adresse IP du Spark Master** : `172.31.253.136`
- **URL de l’interface Web** :`http://172.31.253.136:8080`
