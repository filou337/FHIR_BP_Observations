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
- 🧠 **Option ML** : modèle supervisé (classification/régression) entraîné sur les règles → prédiction temps réel + score/proba de risque

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
