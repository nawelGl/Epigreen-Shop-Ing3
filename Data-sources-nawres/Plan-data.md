# Détournement de données – NYC Taxi → product_ec

## Objectif
- Réutiliser un jeu de données réel (NYC Taxi)
- Le détourner pour alimenter une table métier `product_ec`
- Conserver une cohérence logique (produits, entrepôts, empreintes)
- Générer un volume de données suffisant pour un traitement distribué (Spark)

---

## Table source
Jeu de données : NYC Yellow Taxi Trip Records  
Format : Parquet  

Colonnes principales utilisées :
- `PULocationID`
- `DOLocationID`
- `trip_distance`
- `fare_amount`
- `total_amount`

---

## Table cible : `product_ec`

### Colonnes cibles
- `id_product_ec`
- `id_product_ref`
- `id_warehouse`
- `stock_qty`
- `ec_process`
- `ec_transport`
- `ec_total`

---

## Règles métier retenues
- Une ligne représente :
  - un produit
  - dans un entrepôt
- Plusieurs lignes peuvent partager le même `id_product_ref`
- Un même produit peut exister dans plusieurs entrepôts
- Les empreintes peuvent varier selon l’entrepôt
- La taille n’est pas gérée à ce stade (simplification volontaire)

---

## Paramètres de transformation
- `NB_PRODUCT_REF` : nombre de références produit (paramétrable)
- `NB_WAREHOUSE` : nombre d’entrepôts (paramétrable)
- `FACTEUR_TRANSPORT` : coefficient d’ajustement transport (ex: 0.1)

Ces paramètres permettent :
- d’adapter la volumétrie
- de rejouer les traitements sans dépendre des tables finales
- de rester cohérent avec un futur catalogue produit réel

---

## Détournement des colonnes

### Mapping des colonnes + règles de calcul

### `PULocationID` → `id_warehouse`
- Entrepôt d’origine
- Valeur numérique déjà discrète
- Règle de calcul :
  - `id_warehouse = (PULocationID % NB_WAREHOUSE) + 1`
- Permet de projeter les trajets vers un nombre contrôlé d’entrepôts

---

### `DOLocationID` → `id_product_ref`
- Base d’identification produit
- Permet la répétition naturelle des produits
- Règle de calcul :
  - `id_product_ref = (DOLocationID % NB_PRODUCT_REF) + 1`
- Garantit :
  - plusieurs lignes pour un même produit
  - une distribution réaliste des références

---

### `fare_amount` → `ec_process`
- Coût lié à la fabrication / transformation
- Valeur continue et réaliste
- Règle de calcul :
  - `ec_process = max(fare_amount, 0)`
  - cast en `double`
- Les valeurs négatives sont filtrées

---

### `trip_distance` → `ec_transport`
- Distance de transport
- Règle de calcul :
  - `ec_transport = max(trip_distance, 0) * FACTEUR_TRANSPORT`
- Le facteur permet d’ajuster l’ordre de grandeur sans inventer de données

---

### `ec_process + ec_transport` → `ec_total`
- Empreinte carbone totale
- Règle de calcul :
  - `ec_total = ec_process + ec_transport`
- Calcul simple, déterministe et traçable

---

### Colonnes générées

#### `id_product_ec`
- Identifiant technique
- Règle :
  - généré par Spark (`monotonically_increasing_id()` ou équivalent)

#### `stock_qty`
- Quantité en stock par produit et entrepôt
- Règle de calcul simple et reproductible :
  - `stock_qty = (abs(hash(id_product_ref, id_warehouse)) % 120) + 1`
- Permet une dispersion cohérente sans s’éloigner du réel

---

## Colonnes supprimées
- `VendorID`
- `payment_type`
- `RatecodeID`
- `tip_amount`
- `extra`
- `mta_tax`
- `tolls_amount`
- `congestion_surcharge`
- Champs temporels non utilisés après ingestion

---

## Traitements prévus
- Nettoyage des valeurs nulles
- Filtrage des valeurs non valides
- Cast des types
- Calculs simples (addition, multiplication)
- Projection des identifiants par modulo
- Génération des identifiants techniques
- Écriture dans la table cible

---

## Outils utilisés
- Python (POC ingestion)
- HDFS (stockage raw)
- Spark (détournement et calculs)
- Hadoop cluster (exécution distribuée)

---
