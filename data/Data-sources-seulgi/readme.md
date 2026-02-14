# README — Scénario de démo J1

## Objectif

Mettre en place une chaîne complète de traitement de données d'évènement utilisateur (click/search/cart) simulant des interactions utilisateurs sur un site e-commerce.

La démo J1 montre :

* la génération d’événements (click / cart / search),
* l’ingestion via Kafka (3 topics),
* le traitement batch avec pyspark,
* la pipeline RAW → CURATED → STAGING,
* l’exposition finale dans un Data Mart PostgreSQL.

---

## Architecture (vue globale)

```
NCC / Frontend
   ↓
Simulation d’événements (Python / option Selenium? pour automatiser des évènements)
   ↓
Kafka (topics : click / cart / search)
   ↓
PySpark
   ↓
RAW (partitionné par jour)
   ↓
PySpark
   ↓
CURATED (agrégé par jour)
   ↓
STAGING
   ↓
DATA MART (PostgreSQL – vues)
```

---


### Simulation d’événements

* Priorité J1 : script Python.
* Option J2 : automatisation Selenium?? pour reproduire des parcours réels.

---

## ource de données

### Dataset
https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset/data?select=events.csv
* Source : Kaggle – RetailRocket E-commerce Dataset
* Format : CSV (`events.csv`)

Colonnes principales :

* `visitorid`
* `event` (`view`, `addtocart`, `transaction`)
* `itemid`
* `timestamp`

===> la démo J1, le `timestamp` du CSV est ignoré.
Le timestamp d’événement correspond à l’heure d’exécution du script.

--
##  Ingestion Kafka

### Topics

| Topic               | Description                     |
| ------------------- | ------------------------------- |
| `user-event-click`  | navigation produit (`view`)     |
| `user-event-cart`   | ajout au panier (`addtocart`)   |
| `user-event-search` | recherche utilisateur (simulée) |

### Script Producer
`product_data_kafka.py`

Rôles du script :

* lecture du CSV source,
* mapping des événements vers les topics Kafka,
* génération d’un payload événementiel standard,
* envoi des messages vers Kafka.

Règles de routage :

* `view` → `user-event-click`
* `addtocart` → `user-event-cart`
* `transaction` → ignoré (géré par un autre service)
* `search` → généré artificiellement (optionnel)

---

## Traitement PySpark — RAW

### Objectif

Stocker les données telles quelles, sans logique métier.

### Caractéristiques

* Consommation des topics Kafka.
* Écriture en RAW (JSON / Parquet).
* Partitionnement par jour (`dt=YYYY-MM-DD`).

Exemple d’arborescence :

```
raw/
 └── user-event-click/
     └── dt=2026-01-30/
         └── events.parquet
```

---

## Traitement PySpark — CURATED

### Objectif

* Nettoyer et structurer les données.
* Appliquer des agrégations journalières. ()

### Exemples d’indicateurs

* nombre de clicks par jour,
* nombre d’add-to-cart par jour,
* volume utilisateurs uniques quotidiens,

---

## STAGING

* Zone intermédiaire avant exposition.
* Schéma stabilisé.
* Données prêtes pour la consommation SQL / BI.

---

## Data Mart (PostgreSQL)

### Objectif

* Exposer des données métier facilement exploitables.


Exemples de vues :

* KPIs journaliers,
* indicateurs de conversion.

---

