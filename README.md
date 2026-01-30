
# 🩺 Système de surveillance FHIR — Pression artérielle (Kafka • Elasticsearch • Kibana)

Projet de streaming **temps réel** autour d’observations de **pression artérielle** au format **FHIR (JSON)**.
Le système simule des patients (avec `Faker`), publie les mesures dans **Kafka**, applique des **règles de détection** (normal / anormal + niveaux d’alerte), puis :
- **archive en local** les mesures normales,
- **indexe dans Elasticsearch** les mesures anormales,
- **visualise dans Kibana** via un tableau de bord.

> Référence d’observation FHIR “Blood Pressure” (exemple) :
```txt
https://build.fhir.org/observation-example-bloodpressure.json.html
````

---

## 🎯 Objectifs pédagogiques

* Générer des observations FHIR réalistes (patients + praticiens)
* Produire des mesures à une fréquence **1 à 5 secondes**
* Mettre en place un **pipeline streaming** (Producer → Kafka → Consumer)
* Appliquer des règles de classification tension artérielle et déclencher des alertes
* Stocker et visualiser les anomalies dans **Elasticsearch/Kibana**
* (Optionnel) Ajouter une brique **Machine Learning** pour compléter les règles (score/proba de risque)

---

## 🧱 Architecture (vue d’ensemble)

```txt
[Python - Générateur FHIR] 
        ↓
[Kafka topic: fhir-observations-raw]
        ↓
[Python - Consumer (validation + enrichissement + règles)]
        ├─ NORMAL  → archives/ (JSONL local)
        └─ ANORMAL → Elasticsearch (index journalier) + alertes Kafka
                         ↓
                      Kibana
```

---

## 🧰 Stack technique

* Python 3.11+
* Kafka (via Docker Compose)
* Elasticsearch + Kibana (via Docker Compose)
* `fhir.resources`, `Faker`, `confluent-kafka`, `elasticsearch`, `pandas`, `numpy`
* (Bonus ML) `scikit-learn` + `xgboost`

---

## 📚 Données FHIR utilisées (minimal)

On conserve uniquement les champs nécessaires à l’analyse :

* **Patient**
* **Practitioner**
* **Systolic**
* **Diastolic**

---

## 🧵 Topics Kafka

### Entrée

* `fhir-observations-raw` : observations FHIR brutes (Blood Pressure)

### Sorties (pipeline)

* `fhir-observations-validated` : observation validée + enrichie
* `blood-pressure-alerts` : alertes (quand la TA est anormale, selon le niveau)
* `error-messages` : erreurs de parsing / validation
* `monitoring-metrics` : métriques de fonctionnement (débit, volumes, etc.)
* `audit-log` : événements de traçabilité (démarrage, etc.)

### (Optionnel ML)

* `ml-features` : features envoyées vers un pipeline ML (si activé)
* `ml-predictions` : prédictions ML (si activé)

---

## ✅ Règles de routage (logique métier)

À chaque observation consommée :

1. **Validation minimale** du message
2. **Enrichissement** (patient/praticien + champs utiles + scoring)
3. Classification tension :

* **NORMAL** → archivage local en `archives/*.jsonl`
* **NOT NORMAL** → indexation dans Elasticsearch + exposition Kibana + alertes Kafka

---

## 🚀 Installation & démarrage

### 1) Prérequis

* Docker Desktop (ou Docker + Compose)
* Python 3.11+
* Git

### 2) Lancer l’infrastructure (Kafka + ES + Kibana + Kafka-UI)

```bash
docker compose -f Docker-compose.yml up -d
```

### 3) Installer les dépendances Python

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate

pip install -r Requierement.txt
```

---

## ▶️ Exécution du pipeline temps réel

> Ouvre 2 terminaux (venv activé dans les deux).

### Terminal A — Consumer (analyse + routage)

```bash
python fhir_consumer.py
```

### Terminal B — Producer (génération + publication)

```bash
python fhir_producer.py
```

---

## 🌐 Interfaces utiles

* **Kibana** : [http://localhost:5601](http://localhost:5601)
* **Elasticsearch** : [http://localhost:9200](http://localhost:9200)
* **Kafka UI** : [http://localhost:8080](http://localhost:8080)

---

## 📊 Visualisation Kibana (idée de dashboard)

Dans Kibana, tu peux créer :

* Un tableau des dernières alertes (par patient / catégorie / niveau)
* Une courbe “systolic/diastolic” dans le temps
* Un filtre par :

  * catégorie (`NORMAL`, `ELEVATED`, `STAGE_1`, `STAGE_2`, `CRISIS`)
  * niveau d’alerte
  * patient / praticien

---

## 🤖 Section optionnelle — Intégration d’un modèle Machine Learning

Objectif : compléter les règles par seuils avec une couche “intelligente” :

* **prédiction** du niveau de risque / catégorie en temps réel,
* **estimation probabiliste** (ex : proba d’être en `STAGE_2`),
* meilleur réalisme et adaptabilité (bruit, tendances, profils patients, etc.).

### 1) Préparer un dataset de features (CSV)

Le pipeline ML attend un fichier du type :

* `ml_data/blood_pressure_features.csv`

Features typiques :

* `systolic`, `diastolic`, `age`, `gender`, `trend`, `risk_score`, `hour_of_day`
  Target :
* `blood_pressure_category` (5 classes)

### 2) Entraîner le modèle

```bash
python ml_training.py ml_data/blood_pressure_features.csv
```

Résultat attendu :

* un modèle sauvegardé dans `ml_models/` (modèle + scaler + metadata)

### 3) Brancher le modèle dans le streaming (concept)

Deux options propres :

* **Option A (simple)** : le consumer charge le modèle au démarrage et ajoute `ml_prediction`/`ml_proba` au document indexé ES.
* **Option B (streaming ML)** : le consumer publie des `ml-features` → un service ML renvoie `ml-predictions`.

---

## 🗂️ Structure recommandée du projet

```txt
.
├── Docker-compose.yml
├── Requierement.txt
├── fhir_data_generator.py
├── fhir_producer.py
├── fhir_consumer.py
├── ml_training.py
├── ml_feature_extraction.py
├── ml_data/
│   └── blood_pressure_features.csv
├── ml_models/
└── archives/
```

---

## 🧯 Dépannage rapide

* Kafka/ES/Kibana ne répondent pas :

```bash
docker compose -f Docker-compose.yml ps
docker compose -f Docker-compose.yml logs -f
```

* Kibana démarre lentement : attendre 1–2 minutes (ES doit être “healthy”)
* Conflits de ports : vérifier que 9092 / 9200 / 5601 / 8080 sont libres

---

## 👤 Auteurs

**Philippe ROUMBO**
**Salma ELABSODI**

```

Le README ci-dessus est aligné sur vos scripts (topics, routage NORMAL→archives et anomalies→Elasticsearch, modèle ML optionnel) et votre infra Docker (Kafka/Zookeeper, Elasticsearch, Kibana, Kafka-UI, ports). :contentReference[oaicite:0]{index=0} :contentReference[oaicite:1]{index=1} :contentReference[oaicite:2]{index=2} :contentReference[oaicite:3]{index=3}
```
