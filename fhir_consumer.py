"""
FHIR Kafka Consumer (Option B - topics fonctionnels) -> Elasticsearch + routing Kafka

Entrée:
- fhir-observations-raw (Observation FHIR Blood Pressure)

Sorties:
- fhir-observations-validated (observation enrichie + validée)
- blood-pressure-alerts (uniquement les alertes MEDIUM/HIGH/CRITICAL)
- error-messages (messages invalides / erreurs de parsing)
- monitoring-metrics (petites métriques périodiques)

+ Elasticsearch (index daily: fhir-observations-YYYY.MM.DD) -> uniquement si anomalie (≠ NORMAL)
+ Archivage local JSONL des NORMAL (dossier archives/)
"""

import json
import time
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
from confluent_kafka import Consumer, Producer, KafkaError
from elasticsearch import Elasticsearch


# =============================================================================
# 0) CONFIG (simple + lisible)
# =============================================================================

@dataclass(frozen=True)
class KafkaTopics:
    RAW: str = "fhir-observations-raw"
    VALIDATED: str = "fhir-observations-validated"
    ALERTS: str = "blood-pressure-alerts"
    ERRORS: str = "error-messages"
    MONITORING: str = "monitoring-metrics"


TOPICS = KafkaTopics()

KAFKA_BOOTSTRAP = "localhost:9092"       # si consumer dans Docker: "kafka:29092" ou "kafka:9092" selon le réseau
GROUP_ID = "fhir-bp-consumer-v3"

ES_HOSTS = ["http://localhost:9200"]
ES_INDEX_PREFIX = "fhir-observations"
ES_TEMPLATE_NAME = "fhir-observations-template"

# Historique patient 
HISTORY_SIZE = 10

# Metrics périodiques
METRICS_EVERY_SEC = 30

# Archivage local des NORMAL
ARCHIVE_DIR = Path("archives")
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

# Garde-fous "réalisme" (évite valeurs absurdes dans Kibana)
SYS_MIN, SYS_MAX = 50, 260
DIA_MIN, DIA_MAX = 30, 160
AGE_MIN, AGE_MAX = 0, 120


# =============================================================================
# 1) LOGGING minimal
# =============================================================================

def log(level: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{ts} [{level}] {msg}")


# =============================================================================
# 2) ELASTICSEARCH (connexion + template + indexation)
# =============================================================================

class ElasticsearchManager:
    def __init__(self, hosts: list[str]) -> None:
        self.client = Elasticsearch(
            hosts,
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True,
        )

    def connect_or_fail(self) -> None:
        if not self.client.ping():
            raise RuntimeError("Elasticsearch indisponible.")
        info = self.client.info()
        log("INFO", f"(๑•̀ㅂ•́)و✧ Elasticsearch connecté (v{info['version']['number']}).")

    def ensure_index_template(self) -> None:
        """
        Template ES orienté Kibana:
        - dates en type date
        - catégories en keyword (filtrage/agrégations)
        - valeurs numériques en integer/float
        - observation_full désactivé (pas de mapping profond)
        """
        template_body = {
            "index_patterns": [f"{ES_INDEX_PREFIX}-*"],
            "template": {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval": "5s",
                    "index": {"codec": "best_compression"},
                },
                "mappings": {
                    "properties": {
                        # FHIR base
                        "resourceType": {"type": "keyword"},
                        "id": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "effectiveDateTime": {"type": "date"},
                        "issued": {"type": "date"},
                        "ingestion_timestamp": {"type": "date"},

                        # Patient
                        "patient_id": {"type": "keyword"},
                        "patient_name": {"type": "text"},
                        "patient_first_name": {"type": "text"},
                        "patient_last_name": {"type": "text"},
                        "patient_gender": {"type": "keyword"},
                        "patient_age": {"type": "integer"},
                        "patient_age_group": {"type": "keyword"},

                        # Practitioner 
                        "performer": {
                            "properties": {
                                "reference": {"type": "keyword"},
                                "display": {"type": "text"},
                            }
                        },

                        # mersures de BP 
                        "systolic_pressure": {"type": "integer"},
                        "diastolic_pressure": {"type": "integer"},
                        "systolic_pressure_adjusted": {"type": "float"},
                        "diastolic_pressure_adjusted": {"type": "float"},
                        "bp_reading": {"type": "keyword"},  # ex "120/80"

                        # Contexte
                        "stress_level": {"type": "integer"},
                        "stress_impact_systolic": {"type": "float"},

                        # Détection/labels
                        "blood_pressure_category": {"type": "keyword"},
                        "anomaly_type": {"type": "keyword"},  # HYPERTENSION / HYPOTENSION / ELEVATED
                        "anomaly_detected": {"type": "boolean"},
                        "risk_score": {"type": "float"},
                        "alert_level": {"type": "keyword"},
                        "trend_indicator": {"type": "keyword"},

                        # FHIR brut
                        "observation_full": {"type": "object", "enabled": False},
                    }
                },
            },
        }

        try:
            exists = self.client.indices.exists_index_template(name=ES_TEMPLATE_NAME)
        except Exception:
            exists = False

        if not exists:
            self.client.indices.put_index_template(name=ES_TEMPLATE_NAME, body=template_body)
            log("INFO", f"(๑•̀ㅂ•́)و✧ Template ES créé: {ES_TEMPLATE_NAME}")
        else:
            log("INFO", f" Template ES déjà présent: {ES_TEMPLATE_NAME}")

    def index_name_today(self) -> str:
        return f"{ES_INDEX_PREFIX}-{datetime.now().strftime('%Y.%m.%d')}"

    def index_doc(self, doc_id: str, doc: Dict[str, Any]) -> bool:
        resp = self.client.index(index=self.index_name_today(), id=doc_id, document=doc)
        return resp.get("result") in ("created", "updated")


# =============================================================================
# 3) ENRICHISSEMENT + VALIDATION + CATEGORISATION (AHA + hypotension)
# =============================================================================

class Enricher:
    """
    - Extrait SYS/DIA via LOINC 8480-6 / 8462-4
    - Valide + garde-fous "réalisme"
    - Applique un impact stress léger (stocke RAW + ADJUSTED)
    - Catégorise:
        * Hypotension: < 90/60
        * Normal: <120 and <80
        * Elevated: 120-129 and <80
        * Stage 1: 130-139 or 80-89
        * Stage 2: >=140 or >=90
        * Crisis: >180 and/or >120
    - Produit un doc optimal pour Kibana
    """

    LOINC_SYS = "8480-6"
    LOINC_DIA = "8462-4"

    @staticmethod
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    @staticmethod
    def extract_bp(observation: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        systolic = None
        diastolic = None

        for comp in observation.get("component", []):
            code = comp.get("code", {}).get("coding", [{}])[0].get("code")
            val = comp.get("valueQuantity", {}).get("value")
            if code == Enricher.LOINC_SYS:
                systolic = val
            elif code == Enricher.LOINC_DIA:
                diastolic = val

        return systolic, diastolic

    @staticmethod
    def extract_patient(observation: Dict[str, Any]) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "gender": "M",
            "age": 0,
            "age_group": "adulte",
            "first_name": "Unknown",
            "last_name": "Patient",
            "stress_level": 5,
        }

        for ext in observation.get("extension", []):
            url = ext.get("url", "")
            if "gender" in url:
                data["gender"] = ext.get("valueString", data["gender"])
            elif "age_group" in url:
                data["age_group"] = ext.get("valueString", data["age_group"])
            elif "age" in url:
                data["age"] = ext.get("valueInteger", data["age"])
            elif "first_name" in url:
                data["first_name"] = ext.get("valueString", data["first_name"])
            elif "last_name" in url:
                data["last_name"] = ext.get("valueString", data["last_name"])
            elif "stress" in url:
                data["stress_level"] = ext.get("valueInteger", data["stress_level"])

        # garde-fous (évite valeurs absurdes)
        data["stress_level"] = int(max(1, min(10, int(data["stress_level"]))))
        data["age"] = int(max(AGE_MIN, min(AGE_MAX, int(data["age"]))))

        return data

    @staticmethod
    def apply_stress(systolic: float, diastolic: float, stress_level: int) -> Tuple[float, float, float]:
        level = max(1, min(10, int(stress_level)))

        if level <= 2:
            base = 0.0
        elif level <= 5:
            base = (level - 2) * 2.0
        else:
            base = 6.0 + (level - 5) * 3.0

        sys_delta = base * float(np.random.normal(1.0, 0.1))
        dia_delta = (base * 0.5) * float(np.random.normal(1.0, 0.1))

        sys_adj = systolic + sys_delta
        dia_adj = diastolic + dia_delta

        # garde-fous physiologiques basiques
        sys_adj = Enricher._clamp(sys_adj, SYS_MIN, SYS_MAX)
        dia_adj = Enricher._clamp(dia_adj, DIA_MIN, DIA_MAX)
        if dia_adj >= sys_adj:  # rare, mais on corrige
            dia_adj = max(DIA_MIN, sys_adj - 5)

        return sys_adj, dia_adj, sys_delta

    @staticmethod
    def categorize(sys_adj: float, dia_adj: float) -> Dict[str, Any]:
        s = float(sys_adj)
        d = float(dia_adj)

        # Hypotension (anomalie basse)
        if s < 90 or d < 60:
            return {
                "category": "HYPOTENSION",
                "anomaly_type": "HYPOTENSION",
                "anomaly": True,
                "risk": 0.55,
                "alert": "MEDIUM",
            }

        # Hypertensive crisis
        if s > 180 or d > 120:
            return {
                "category": "HYPERTENSIVE_CRISIS",
                "anomaly_type": "HYPERTENSION",
                "anomaly": True,
                "risk": 0.95,
                "alert": "CRITICAL",
            }

        # Stage 2
        if s >= 140 or d >= 90:
            return {
                "category": "HYPERTENSION_STAGE_2",
                "anomaly_type": "HYPERTENSION",
                "anomaly": True,
                "risk": 0.80,
                "alert": "HIGH",
            }

        # Stage 1
        if (130 <= s <= 139) or (80 <= d <= 89):
            return {
                "category": "HYPERTENSION_STAGE_1",
                "anomaly_type": "HYPERTENSION",
                "anomaly": True,
                "risk": 0.55,
                "alert": "MEDIUM",
            }

        # Elevated (condition stricte: 120–129 ET <80)
        if (120 <= s <= 129) and (d < 80):
            return {
                "category": "ELEVATED",
                "anomaly_type": "ELEVATED",
                "anomaly": True,
                "risk": 0.25,
                "alert": "LOW",
            }

        # Normal
        return {
            "category": "NORMAL",
            "anomaly_type": "NONE",
            "anomaly": False,
            "risk": 0.05,
            "alert": "NONE",
        }

    @staticmethod
    def trend(history: deque[int], current: int) -> str:
        if len(history) < 3:
            return "STABLE"
        avg = sum(list(history)[-3:]) / 3.0
        if current > avg + 5:
            return "INCREASING"
        if current < avg - 5:
            return "DECREASING"
        return "STABLE"

    @staticmethod
    def validate_minimal(observation: Dict[str, Any]) -> Tuple[bool, str]:
        if observation.get("resourceType") != "Observation":
            return False, "resourceType != Observation"
        if not observation.get("id"):
            return False, "missing id"

        s, d = Enricher.extract_bp(observation)
        if s is None or d is None:
            return False, "missing blood pressure components (LOINC 8480-6 / 8462-4)"

        # valeurs brutes plausibles
        try:
            s_f, d_f = float(s), float(d)
        except Exception:
            return False, "systolic/diastolic not numeric"

        if not (SYS_MIN <= s_f <= SYS_MAX and DIA_MIN <= d_f <= DIA_MAX):
            return False, f"bp out of plausible range: {s_f}/{d_f}"

        if d_f >= s_f:
            return False, f"invalid bp (diastolic >= systolic): {s_f}/{d_f}"

        return True, "ok"

    def enrich(self, observation: Dict[str, Any], patient_hist: Dict[str, deque[int]]) -> Dict[str, Any]:
        systolic, diastolic = self.extract_bp(observation)
        assert systolic is not None and diastolic is not None

        # patient infos
        patient = self.extract_patient(observation)

        # stress -> adjusted
        sys_adj, dia_adj, sys_delta = self.apply_stress(float(systolic), float(diastolic), int(patient["stress_level"]))

        # catégorisation sur les valeurs ajustées
        cat = self.categorize(sys_adj, dia_adj)

        # patient_id
        patient_ref = observation.get("subject", {}).get("reference", "")
        patient_id = patient_ref.split("/")[-1] if "/" in patient_ref else patient_ref

        # tendance sur la mesure brute
        hist_sys = patient_hist.setdefault(patient_id, deque(maxlen=HISTORY_SIZE))
        trend = self.trend(hist_sys, int(float(systolic)))
        hist_sys.append(int(float(systolic)))

        now_iso = datetime.now(timezone.utc).isoformat()

        patient_name = f"{patient['first_name']} {patient['last_name']}".strip()
        bp_reading = f"{int(float(systolic))}/{int(float(diastolic))}"

        return {
            "id": observation.get("id"),
            "resourceType": observation.get("resourceType"),
            "status": observation.get("status"),
            "effectiveDateTime": observation.get("effectiveDateTime"),
            "issued": observation.get("issued"),
            "ingestion_timestamp": now_iso,
            "patient_id": patient_id,
            "patient_name": patient_name,
            "patient_first_name": patient["first_name"],
            "patient_last_name": patient["last_name"],
            "patient_gender": patient["gender"],
            "patient_age": int(patient["age"]),
            "patient_age_group": patient["age_group"],

            "performer": observation.get("performer", [{}])[0] if observation.get("performer") else {},

            # Variables BP
            "systolic_pressure": int(float(systolic)),
            "diastolic_pressure": int(float(diastolic)),
            "systolic_pressure_adjusted": float(sys_adj),
            "diastolic_pressure_adjusted": float(dia_adj),
            "bp_reading": bp_reading,

            "stress_level": int(patient["stress_level"]),
            "stress_impact_systolic": float(sys_delta),

            "blood_pressure_category": cat["category"],
            "anomaly_type": cat["anomaly_type"],
            "anomaly_detected": bool(cat["anomaly"]),
            "risk_score": float(cat["risk"]),
            "alert_level": cat["alert"],

            "trend_indicator": trend,

            "observation_full": observation,
        }


# =============================================================================
# 4) PIPELINE (consume -> validate/enrich -> route -> ES/archive -> commit)
# =============================================================================

class Pipeline:
    def __init__(self) -> None:
        self.running = True

        # ES
        self.es = ElasticsearchManager(ES_HOSTS)

        # Consumer Kafka
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,   
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        })

        # Producer Kafka 
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 20,
            "batch.num.messages": 10000,
        })

        self.enricher = Enricher()
        self.patient_hist: Dict[str, deque[int]] = {}

        self.stats = {
            "consumed": 0,
            "validated": 0,
            "indexed": 0,
            "archived_normal": 0,
            "alerts": 0,
            "errors": 0,
            "start_ts": time.time(),
            "last_metrics_ts": 0.0,
        }

    # -------- Kafka helpers

    def _delivery_cb(self, err, msg) -> None:
        if err is not None:
            log("ERROR", f"Produce failed: {err}")

    def produce_json(self, topic: str, key: str, payload: Dict[str, Any]) -> None:
        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            callback=self._delivery_cb,
        )
        # Sert les callbacks sans bloquer
        self.producer.poll(0)

    def commit_msg(self, msg) -> None:
        self.consumer.commit(message=msg, asynchronous=False)

    # -------- Archivage local

    def archive_normal(self, enriched: Dict[str, Any]) -> None:
        """
        JSONL: 1 ligne = 1 observation normalisée (sans le gros observation_full)
        => parfait pour réaliser notre ML pour plus tard.
        """
        day = datetime.now().strftime("%Y-%m-%d")
        path = ARCHIVE_DIR / f"normal_observations_{day}.jsonl"

        light = {k: v for k, v in enriched.items() if k != "observation_full"}
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(light, ensure_ascii=False) + "\n")

    # -------- lifecycle

    def start(self) -> None:
        log("INFO", " Connexion services...")
        self.es.connect_or_fail()
        self.es.ensure_index_template()

        log("INFO", f"(๑•̀ㅂ•́)و✧ Subscribe: {TOPICS.RAW}")
        self.consumer.subscribe([TOPICS.RAW])

        log("INFO", "( •̀ ω •́ )✧ Consumer démarré (CTRL+C pour stop).")

        try:
            while self.running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    self.maybe_emit_metrics()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log("ERROR", f"Kafka error: {msg.error()}")
                    continue

                self.handle_message(msg)
                self.maybe_emit_metrics()

        finally:
            self.stop()

    def stop(self) -> None:
        if not self.running:
            return
        self.running = False

        log("INFO", "Arrêt... flush producer + close consumer")
        self.producer.flush(10)
        self.consumer.close()
        self.print_final_stats()

    # -------- processing

    def handle_message(self, msg) -> None:
        self.stats["consumed"] += 1

        # 1) Parse JSON
        try:
            observation = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            self.stats["errors"] += 1
            self.produce_json(TOPICS.ERRORS, key="parse_error", payload={
                "error": f"json_decode_failed: {str(e)}",
                "topic": msg.topic(),
                "ts": datetime.now(timezone.utc).isoformat(),
            })
            self.commit_msg(msg)
            return

        # 2) Validation minimale
        ok, reason = self.enricher.validate_minimal(observation)
        if not ok:
            self.stats["errors"] += 1
            self.produce_json(TOPICS.ERRORS, key=observation.get("id", "missing_id"), payload={
                "error": f"validation_failed: {reason}",
                "ts": datetime.now(timezone.utc).isoformat(),
                "observation": observation,
            })
            self.commit_msg(msg)
            return

        # 3) Enrichissement
        enriched = self.enricher.enrich(observation, self.patient_hist)
        self.stats["validated"] += 1

        # 4) Toujours publier VALIDATED (utile pour debug / replay)
        route_key = enriched["patient_id"] or enriched["id"]
        self.produce_json(TOPICS.VALIDATED, key=route_key, payload=enriched)

        # 5) NORMAL -> archive local ; sinon -> ES + (éventuel) alert
        try:
            if enriched["blood_pressure_category"] == "NORMAL":
                self.archive_normal(enriched)
                self.stats["archived_normal"] += 1
            else:
                # Index ES seulement si anomalie (≠ NORMAL)
                if self.es.index_doc(doc_id=enriched["id"], doc=enriched):
                    self.stats["indexed"] += 1

                # alert uniquement MEDIUM/HIGH/CRITICAL
                if enriched["alert_level"] in {"MEDIUM", "HIGH", "CRITICAL"}:
                    self.stats["alerts"] += 1
                    self.produce_json(TOPICS.ALERTS, key=route_key, payload={
                        "ts": enriched["ingestion_timestamp"],
                        "patient_id": enriched["patient_id"],
                        "patient_name": enriched["patient_name"],
                        "bp": enriched["bp_reading"],
                        "category": enriched["blood_pressure_category"],
                        "anomaly_type": enriched["anomaly_type"],
                        "alert_level": enriched["alert_level"],
                        "risk_score": enriched["risk_score"],
                        "trend": enriched["trend_indicator"],
                        "stress_level": enriched["stress_level"],
                    })

                    if enriched["alert_level"] == "CRITICAL":
                        log("ERROR", f"(▀̿Ĺ̯▀̿ ̿) CRITICAL {enriched['patient_id']} {enriched['bp_reading']} stress={enriched['stress_level']}")

        except Exception as e:
            # Si ES/IO tombe, on push error + commit pour éviter boucle infinie
            self.stats["errors"] += 1
            self.produce_json(TOPICS.ERRORS, key=enriched.get("id", "unknown"), payload={
                "error": f"processing_failed: {str(e)}",
                "ts": datetime.now(timezone.utc).isoformat(),
                "enriched_light": {k: v for k, v in enriched.items() if k != "observation_full"},
            })
            self.commit_msg(msg)
            return

        # 6) Commit (traitement OK)
        self.commit_msg(msg)

    # -------- metrics

    def maybe_emit_metrics(self) -> None:
        now = time.time()
        if now - self.stats["last_metrics_ts"] < METRICS_EVERY_SEC:
            return

        self.stats["last_metrics_ts"] = now
        elapsed = max(1.0, now - self.stats["start_ts"])
        rate = self.stats["consumed"] / elapsed

        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "group_id": GROUP_ID,
            "consumed": self.stats["consumed"],
            "validated": self.stats["validated"],
            "indexed": self.stats["indexed"],
            "archived_normal": self.stats["archived_normal"],
            "alerts": self.stats["alerts"],
            "errors": self.stats["errors"],
            "rate_msg_per_sec": round(rate, 3),
        }

        self.produce_json(TOPICS.MONITORING, key=GROUP_ID, payload=payload)

        log("INFO", f" consumed={payload['consumed']} validated={payload['validated']} "
                    f"indexed={payload['indexed']} archived_normal={payload['archived_normal']} "
                    f"alerts={payload['alerts']} errors={payload['errors']} rate={payload['rate_msg_per_sec']}/s")

    def print_final_stats(self) -> None:
        elapsed = max(1.0, time.time() - self.stats["start_ts"])
        log("INFO", "################## STATS FINALES ################")
        log("INFO", f"Durée: {int(elapsed)}s")
        log("INFO", f"consumed={self.stats['consumed']}")
        log("INFO", f"validated={self.stats['validated']}")
        log("INFO", f"indexed={self.stats['indexed']}")
        log("INFO", f"archived_normal={self.stats['archived_normal']}")
        log("INFO", f"alerts={self.stats['alerts']}")
        log("INFO", f"errors={self.stats['errors']}")
        log("INFO", f"rate={self.stats['consumed']/elapsed:.2f} msg/s")
        log("INFO", "#################################################")


# =============================================================================
# 5) MAIN + signal handler
# =============================================================================

def main() -> None:
    pipeline = Pipeline()

    def _sigint_handler(signum, frame) -> None:
        pipeline.stop()

    signal.signal(signal.SIGINT, _sigint_handler)
    pipeline.start()


if __name__ == "__main__":
    main()
