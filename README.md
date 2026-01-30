````markdown
# 🩺 FHIR Blood Pressure Monitoring — Kafka • Elasticsearch • Kibana (+ ML optionnel)

Pipeline **temps réel** de surveillance de **pression artérielle** au format **FHIR (JSON)** : génération de données (patients/praticiens), ingestion Kafka, analyse & routage, stockage ciblé dans Elasticsearch, visualisation Kibana — avec une **brique Machine Learning optionnelle** pour compléter les règles à seuils.

---

## ✨ Fonctionnalités

- ✅ Génération d’observations **FHIR Blood Pressure** en JSON (via `Faker`)
- ✅ Publication streaming dans **Kafka**
- ✅ Consumer Python : **validation**, **extraction**, **règles cliniques**
- ✅ Routage :
  - **NORMAL** → archivage **local** (JSONL)
  - **ANORMAL** → indexation **Elasticsearch** + visualisation **Kibana**
- ✅ 4 cas d’alerte “not normal” exposés dans Kibana (dashboard/table)
- 🧠 **Option ML** : modèle supervisé entraîné sur les règles → prédiction temps réel + score/proba de risque

---

## 🧱 Architecture

```text
[ fhir_data_generator.py ]  -> génère Observation FHIR (JSON)
          |
          v
[ fhir_producer.py ]  -> envoie vers Kafka (topic raw)
          |
          v
[ fhir_consumer.py ]  -> lit Kafka, valide, applique règles
      |                         |
      | NORMAL                  | NOT NORMAL
      v                         v
archives/*.jsonl        Elasticsearch (index)
                               |
                               v
                            Kibana
````

---

## 📦 Stack

* **Python 3.11+**
* **Kafka** (Docker)
* **Elasticsearch + Kibana** (Docker)
* Libs principales : `faker`, `confluent-kafka`, `elasticsearch`, `numpy`, `pandas`
* (Option ML) `scikit-learn`

---

## 📁 Structure du repo

```text
.
├── Docker-compose.yml
├── Requierement.txt
├── README.md
├── fhir_data_generator.py
├── fhir_producer.py
├── fhir_consumer.py
├── ml_feature_extraction.py
├── ml_training.py
└── archives/
```

---

## 📄 Format FHIR (minimal conservé)

Référence d’exemple (Observation Blood Pressure) :

* [https://build.fhir.org/observation-example-bloodpressure.json.html](https://build.fhir.org/observation-example-bloodpressure.json.html)

Champs conservés / exploités :

* **Patient**
* **Practitioner**
* **Systolic**
* **Diastolic**

Génération :

* Fréquence : toutes les **1s** ou **5s**
* Population : **5 patients** et **1–2 praticiens**

---

## 🧵 Topics Kafka (recommandation)

* **Entrée**

  * `fhir-observations-raw` : Observations FHIR brutes (Blood Pressure)

* **Sorties**

  * `fhir-observations-validated` : Observation validée / enrichie
  * `blood-pressure-alerts` : Alertes (uniquement anomalies)
  * `error-messages` : Parsing/validation errors
  * `monitoring-metrics` : Petites métriques (débit, volumes)

---

## ✅ Règles d’analyse & routage

À chaque observation consommée :

1. Validation minimale (message bien formé, champs attendus présents)
2. Extraction des valeurs **systolic** et **diastolic**
3. Classification :

* **Si NORMAL**

  * Stockage **local** dans `archives/` au format **JSONL**

* **Si NOT NORMAL**

  * Création d’une alerte (catégorie / niveau)
  * Indexation dans **Elasticsearch**
  * Exposition dans **Kibana** via un tableau de bord

> Les **4 cas d’alerte** “not normal blood pressure” sont stockés dans Elasticsearch et visualisés dans Kibana.

---

## 🚀 Quickstart

### 1) Lancer l’infrastructure (Kafka + Elasticsearch + Kibana)

```bash
docker compose -f Docker-compose.yml up -d
```

Vérifier l’état :

```bash
docker compose -f Docker-compose.yml ps
```

### 2) Installer les dépendances Python

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -r Requierement.txt
```

### 3) Démarrer le pipeline temps réel

**Terminal A — Consumer (analyse + routage)**

```bash
python fhir_consumer.py
```

**Terminal B — Producer (génération + push Kafka)**

```bash
python fhir_producer.py
```

---

## 🌐 Interfaces

* **Kibana** : [http://localhost:5601](http://localhost:5601)
* **Elasticsearch** : [http://localhost:9200](http://localhost:9200)
* **Kafka UI** (si présent) : [http://localhost:8080](http://localhost:8080)

---

## 🧠 Section optionnelle — Machine Learning

Pour ceux qui souhaitent aller plus loin : intégrer une dimension IA via un modèle supervisé (ex. **régression logistique** ou classification).

Principe :

* Le modèle est entraîné à partir des données de pression artérielle en utilisant les **seuils cliniques** comme labels (normal / anormal ou multi-classes).
* Une fois entraîné, il est **chargé dans le consumer Kafka** pour produire une **prédiction temps réel** sur chaque nouvelle mesure.
* Le modèle complète les règles basées sur les seuils en fournissant une **estimation probabiliste du risque**, rendant le système plus **réaliste**, **prédictif** et **adaptable**.

### Workflow conseillé

1. Extraction de features (ex : systolic, diastolic, dérivés, tendances)
2. Entraînement du modèle
3. Sauvegarde dans `ml_models/`
4. Chargement dans `fhir_consumer.py` pour annoter les observations :

   * `ml_prediction`
   * `ml_proba` (ou `risk_score`)

### Lancer l’entraînement (exemple)

```bash
python ml_training.py
```

---

## 📊 Kibana — idées de dashboard

* Table : dernières alertes (patient, praticien, systolic, diastolic, catégorie)
* Série temporelle : évolution systolic/diastolic
* Filtres : catégorie, niveau, patient, praticien
* Compteurs : volumes NORMAL vs ANORMAL

---

## 🧯 Troubleshooting

### Voir les logs Docker

```bash
docker compose -f Docker-compose.yml logs -f
```

### Vérifier ports libres

* `9092` (Kafka)
* `9200` (Elasticsearch)
* `5601` (Kibana)
* `8080` (Kafka UI si activé)

### Kibana “not ready”

Attendre que Elasticsearch soit “healthy” (souvent 1–2 minutes après `up -d`).

---

## 👤 Auteurs

* **Philippe ROUMBO**
* **Salma ELABSODI**

```
