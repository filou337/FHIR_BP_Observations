Ci-dessous, tu as **un README.md complet** (copie/colle tel quel) **inspiré du style** de ton exemple “arbres plantés…”, mais adapté à ton projet **FHIR Blood Pressure – Kafka → Elasticsearch → Kibana**.

> ✅ Je mets le README dans **un seul bloc Markdown**.
> 🔎 À la fin (hors README), je te laisse les **sources** que j’ai utilisées pour les bonnes pratiques + docs officielles.

````markdown
# 🩺 Système de surveillance de la pression artérielle (FHIR) – Kafka, Elasticsearch & Kibana

Ce dépôt présente un **système de streaming temps réel** qui génère des mesures de **pression artérielle** au format **FHIR Observation (JSON)**, les publie dans **Kafka**, détecte et catégorise les anomalies via un **consumer Python**, puis indexe les **cas anormaux** dans **Elasticsearch** pour une visualisation dans **Kibana**.

✅ Les mesures **NORMAL** sont **archivées localement** (JSONL) pour constituer un dataset exploitable (analyses + option ML).

---

## 🎯 Objectifs du projet

- Générer des Observations **FHIR Blood Pressure** (SYS/DIA) réalistes côté backend Python.
- Mettre en place un pipeline **streaming** :
  - **Producer** → Kafka (topic `fhir-observations-raw`)
  - **Consumer** → validation + enrichissement + catégorisation + routage
  - **Elasticsearch** → stockage des **anomalies uniquement**
  - **Kibana** → visualisation & suivi
- Appliquer des **règles cliniques** (AHA + hypotension) pour classer les mesures :
  - `NORMAL`, `ELEVATED`, `HYPERTENSION_STAGE_1`, `HYPERTENSION_STAGE_2`, `HYPERTENSIVE_CRISIS`, `HYPOTENSION`
- (Optionnel) Ajouter une **brique Machine Learning** (proba de risque) pour compléter les seuils.

---

## 🧱 Architecture (vue d’ensemble)

**Flux :**
1. `fhir_producer.py` génère des Observations FHIR BP → publie dans Kafka (`fhir-observations-raw`)
2. `fhir_consumer.py` consomme → valide → enrichit → catégorise → route :
   - `fhir-observations-validated` (toutes les observations enrichies)
   - `blood-pressure-alerts` (uniquement alertes MEDIUM/HIGH/CRITICAL)
   - `error-messages` (messages invalides / erreurs de parsing)
   - `monitoring-metrics` (métriques périodiques)
3. Elasticsearch indexe **uniquement les anomalies** (≠ `NORMAL`) dans un index **journalier**
4. Kibana permet la **visualisation** + tableaux de bord

> 📌 Ajoute ici ton schéma d’architecture :
- `docs/architecture.png`

---

## 🗂 Contenu du dépôt

### Scripts Python
- `fhir_data_generator.py`  
  Génère des données réalistes et construit une **FHIR Observation Blood Pressure**.

- `fhir_producer.py`  
  Producer Kafka :  
  - crée les topics si absents (idempotent)
  - publie les Observations sur `fhir-observations-raw`
  - publie aussi `monitoring-metrics` + `audit-log`

- `fhir_consumer.py`  
  Consumer Kafka + pipeline :
  - parsing/validation
  - enrichissement (patient/practitioner, stress, trend, risk score…)
  - catégorisation BP (AHA + hypotension)
  - indexation ES **si anomalie**
  - archivage local JSONL des NORMAL (`archives/`)

### Infra Docker
- `Docker-compose.yml`  
  Lance :
  - Zookeeper
  - Kafka (Confluent)
  - Elasticsearch 8.11.1
  - Kibana 8.11.1
  - Kafka UI

> Tu peux ajouter plus tard :
- `docs/` (captures Kibana / Kafka UI, schémas)
- `dashboards/` (exports Kibana)
- `requirements.txt` (dépendances exactes)
- `.env.example` (variables d’environnement)

---

## 📦 Stack & Services

### Docker
- Zookeeper
- Kafka (Confluent CP)
- Elasticsearch `8.11.1`
- Kibana `8.11.1`
- Kafka UI

### Python (librairies)
- `confluent-kafka`
- `elasticsearch`
- `faker`
- `numpy`
- `fhir.resources`

---

## 🚀 Démarrage rapide

### 1) Lancer l’infrastructure Docker

> Le fichier s’appelle **`Docker-compose.yml`** (D majuscule).

```bash
docker compose -f Docker-compose.yml up -d
````

### 2) Accéder aux interfaces

* **Kafka UI** : [http://localhost:8080](http://localhost:8080)
* **Kibana** : [http://localhost:5601](http://localhost:5601)
* **Elasticsearch** : [http://localhost:9200](http://localhost:9200)
* **Kafka (host)** : `localhost:9092`

---

## 🐍 Setup Python (venv recommandé)

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate
```

Installer les dépendances :

```bash
pip install -U pip
pip install confluent-kafka elasticsearch faker numpy fhir.resources
```

---

## ▶️ Exécution (ordre conseillé)

### 1) Lancer le consumer (d’abord)

```bash
python fhir_consumer.py
```

✅ Le consumer :

* crée un **index template** Elasticsearch `fhir-observations-template`
* consomme `fhir-observations-raw`
* publie `validated`, `alerts`, `metrics`, `errors`
* indexe **uniquement** les anomalies dans `fhir-observations-YYYY.MM.DD`
* archive les NORMAL dans `archives/normal_observations_YYYY-MM-DD.jsonl`

### 2) Lancer le producer

```bash
python fhir_producer.py
```

✅ Le producer :

* initialise patients / practitioners
* envoie des Observations toutes les **1 à 5 secondes**
* garde l’ordre par patient via `key = patient_id`

---

## ⚙️ Configuration

### Kafka bootstrap (IMPORTANT)

Dans tes scripts :

* `KAFKA_BOOTSTRAP = "localhost:9092"`

👉 Si tu exécutes les scripts **dans Docker**, utilise :

* `kafka:29092`

### Elasticsearch hosts

* Par défaut : `ES_HOSTS = ["http://localhost:9200"]`
* Dans Docker : `http://elasticsearch:9200`

---

## 📨 Topics Kafka

| Topic                         | Description                               | Produit par         |
| ----------------------------- | ----------------------------------------- | ------------------- |
| `fhir-observations-raw`       | Observations FHIR brutes (Blood Pressure) | Producer            |
| `fhir-observations-validated` | Observations enrichies + validées         | Consumer            |
| `blood-pressure-alerts`       | Alertes uniquement (MEDIUM/HIGH/CRITICAL) | Consumer            |
| `error-messages`              | Erreurs parsing/validation/processing     | Producer + Consumer |
| `monitoring-metrics`          | Métriques périodiques (throughput, stats) | Producer + Consumer |
| `audit-log`                   | Traçabilité minimale (start, etc.)        | Producer            |
| `ml-features` (opt)           | Features prêtes ML                        | (à brancher)        |
| `ml-predictions` (opt)        | Prédictions ML temps réel                 | (à brancher)        |

---

## 🧾 Structure des données (FHIR Observation Blood Pressure)

Chaque message est une ressource **FHIR `Observation`** (JSON), de type “Blood Pressure”, contenant :

* un code panel “blood pressure”
* deux composants :

  * **Systolic** (`8480-6`)
  * **Diastolic** (`8462-4`)
* unité : `mm[Hg]`

### Champs extraits / enrichis pour Kibana (exemples)

| Champ                     | Type    | Description                   |
| ------------------------- | ------- | ----------------------------- |
| `patient_id`              | keyword | Identifiant patient           |
| `patient_age`             | integer | Âge                           |
| `patient_gender`          | keyword | Sexe                          |
| `systolic_pressure`       | integer | SYS                           |
| `diastolic_pressure`      | integer | DIA                           |
| `stress_level`            | integer | Stress simulé (1–10)          |
| `blood_pressure_category` | keyword | Catégorie clinique            |
| `alert_level`             | keyword | NONE/LOW/MEDIUM/HIGH/CRITICAL |
| `risk_score`              | float   | Score de risque               |
| `trend_indicator`         | keyword | Indicateur de tendance        |
| `ingestion_timestamp`     | date    | Timestamp ingestion           |

> Le consumer stocke aussi `observation_full` (FHIR brut) mais avec un mapping ES “safe” (objet désactivé) pour éviter une explosion de mapping.

---

## 🧠 Règles de catégorisation (AHA + hypotension)

Le consumer applique :

* **Hypotension** : `< 90/60`
* **Normal** : `<120` et `<80`
* **Elevated** : `120–129` et `<80`
* **HTA Stage 1** : `130–139` ou `80–89`
* **HTA Stage 2** : `≥140` ou `≥90`
* **Crise hypertensive** : `>180` et/ou `>120`

---

## 🔎 Elasticsearch & Kibana

### Indexation Elasticsearch

* Index **journalier** : `fhir-observations-YYYY.MM.DD`
* **Indexation uniquement si anomalie** (≠ `NORMAL`)
* Template : `fhir-observations-template`

### Kibana : mise en place rapide

1. Ouvre Kibana : [http://localhost:5601](http://localhost:5601)
2. Crée une **Data View** :

   * Pattern : `fhir-observations-*`
   * Time field : `ingestion_timestamp`

### Dashboards recommandés (à construire)

* **Vue globale**

  * # anomalies / jour
  * répartition par catégorie
  * alertes CRITICAL/HIGH en temps réel
* **Analyses**

  * SYS/DIA dans le temps (line chart)
  * distribution SYS/DIA (histogram)
  * stress vs SYS/DIA (scatter)
  * top patients par alertes (bar)
* **Filtres**

  * période
  * catégorie
  * alert_level
  * patient_id / age_group / gender

> 📌 Ajoute des captures :

* `docs/kibana-dashboard.png`
* `docs/kafka-ui-topics.png`

---

## 🤖 Section optionnelle : Intégration d’un modèle de Machine Learning (bonus)

Pour aller plus loin, tu peux entraîner un modèle supervisé (ex : **régression logistique**) sur :

* les données NORMAL archivées en `archives/*.jsonl`
* les anomalies indexées dans Elasticsearch

Objectif :

* produire une **probabilité de risque** (au lieu d’un simple seuil)
* publier les sorties sur :

  * `ml-features`
  * `ml-predictions`

✅ Résultat : un système plus **prédictif, probabiliste et adaptable**.

---

## 🧪 Vérifications & Debug

### Vérifier Elasticsearch

```bash
curl http://localhost:9200
curl http://localhost:9200/_cat/indices?v
```

### Vérifier Kibana

```bash
curl http://localhost:5601/api/status
```

### Vérifier Kafka UI

* [http://localhost:8080](http://localhost:8080)
  Contrôle :
* topics présents
* messages qui arrivent
* consumer group actif

### Problèmes fréquents

* Scripts sur host → `localhost:9092`
* Scripts dans Docker → `kafka:29092`
* Kibana ne démarre pas → Elasticsearch pas encore “healthy” (attends 30–60s)

---

## 📌 Roadmap (idées d’amélioration)

* [ ] Ajouter un `requirements.txt` (freeze)
* [ ] Dockeriser producer/consumer dans le compose
* [ ] Ajouter exports Kibana dans `dashboards/`
* [ ] Ajouter un script `train_ml.py` (baseline)
* [ ] Ajouter CI (lint + tests)
* [ ] Ajouter un Makefile (start/stop/reset/logs)

---

## 🤝 Contribuer

1. Fork le repo
2. Crée une branche :

```bash
git checkout -b feature/my-feature
```

3. Commit :

```bash
git commit -m "feat: add my feature"
```

4. Push + Pull Request

---

## 🛡️ Licence

MIT (recommandée pour un projet démo).
Ajoute un fichier `LICENSE` si besoin.

---

## 👤 Auteur

Philippe ROUMBO

* GitHub : (à compléter)
* LinkedIn : (à compléter)

```

