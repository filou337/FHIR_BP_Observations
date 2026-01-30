"""
FHIR Kafka Producer (Option B - topics fonctionnels)

But:
- Générer des Observations FHIR "Blood Pressure" réalistes via FHIRDataGenerator
- Publier sur Kafka avec garantie d'ordre par patient (key = patient_id)
- Créer les topics si absents
- Publier des métriques de monitoring et un audit minimal

Topics (alignés consumer Option B):
- fhir-observations-raw : Observations brutes
- fhir-observations-validated : (produit par le consumer)
- blood-pressure-alerts : (produit par le consumer)
- error-messages : erreurs de parsing/validation (consumer/producer)
- monitoring-metrics : métriques système (producer + consumer)
- audit-log : traçabilité (producer)
- ml-features / ml-predictions : optionnels (consumer/ML pipeline)

Dépendances:
- confluent-kafka
- fhir.resources + faker (déjà utilisés par ton fhir_data_generator.py)
"""

import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fhir_data_generator import FHIRDataGenerator, PatientProfile

# =========================================================
# 1) Topics & Config
# =========================================================

@dataclass(frozen=True)
class Topics:
    RAW: str = "fhir-observations-raw"
    VALIDATED: str = "fhir-observations-validated"
    ALERTS: str = "blood-pressure-alerts"
    ERRORS: str = "error-messages"
    METRICS: str = "monitoring-metrics"
    AUDIT: str = "audit-log"
    ML_FEATURES: str = "ml-features"
    ML_PREDICTIONS: str = "ml-predictions"

TOPICS = Topics()
KAFKA_BOOTSTRAP = "localhost:9092"
CLIENT_ID = "fhir-bp-producer-v3"

MIN_SEC_BETWEEN_MEASURES = 1.0
MAX_SEC_BETWEEN_MEASURES = 5.0
METRICS_EVERY_SEC = 30

# =========================================================
# 2) JSON Encoder (FHIR dict() peut contenir Decimal/datetime)
# =========================================================

class SafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def log(level: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{ts} [{level}] {msg}")

# =========================================================
# 3) Kafka Admin: création topics (idempotent)
# =========================================================

def ensure_topics(bootstrap: str) -> None:
    """Crée les topics si absents."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    desired = [
        (TOPICS.RAW, 3, 24 * 60 * 60 * 1000),
        (TOPICS.VALIDATED, 3, 24 * 60 * 60 * 1000),
        (TOPICS.ALERTS, 2, 24 * 60 * 60 * 1000),
        (TOPICS.ERRORS, 1, 24 * 60 * 60 * 1000),
        (TOPICS.METRICS, 1, 24 * 60 * 60 * 1000),
        (TOPICS.AUDIT, 1, 7 * 24 * 60 * 60 * 1000),
        (TOPICS.ML_FEATURES, 2, 24 * 60 * 60 * 1000),
        (TOPICS.ML_PREDICTIONS, 2, 24 * 60 * 60 * 1000),
    ]
    topics = [
        NewTopic(
            topic=name,
            num_partitions=partitions,
            replication_factor=1,
            config={
                "cleanup.policy": "delete",
                "retention.ms": str(retention_ms),
            },
        )
        for (name, partitions, retention_ms) in desired
    ]
    fs = admin.create_topics(topics, request_timeout=15.0)
    log("INFO", "Vérification / création des topics Kafka…")
    
    for (name, partitions, _) in desired:
        try:
            fs[name].result()
            log("INFO", f"ヾ(๑•̀ㅂ•́)و✧♪topic created: {name} ({partitions} partitions)")
        except Exception as e:
            if "already exists" in str(e).lower():
                log("INFO", f"ℹ️ topic exists: {name}")
            else:
                log("WARN", f"(▀̿Ĺ̯▀̿ ̿) topic error: {name} -> {e}")

# =========================================================
# 4) Producer Kafka (fiable + ordre par clé)
# =========================================================

class ReliableProducer:
    """Notes:
    - enable.idempotence=True réduit les doublons en cas de retries et requiert acks=all.
    - poll() sert les callbacks de delivery report.
    - librdkafka est thread-safe, mais ici on reste simple avec une boucle unique.
    """

    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self.sent_ok = 0
        self.sent_err = 0
        self.p = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": CLIENT_ID,
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 1,
            "linger.ms": 20,
            "compression.type": "gzip",
            "queue.buffering.max.messages": 200000,
        })

    def _delivery_cb(self, err, msg) -> None:
        if err is not None:
            self.sent_err += 1
            log("ERROR", f"delivery failed topic={msg.topic()} err={err}")
        else:
            self.sent_ok += 1

    def send_json(self, topic: str, key: str, payload: Dict[str, Any]) -> None:
        self.p.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False, cls=SafeJSONEncoder).encode("utf-8"),
            callback=self._delivery_cb,
        )
        self.p.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self.p.flush(timeout)

# =========================================================
# 5) Scheduler simple (1 boucle) = synchro propre
# =========================================================

class ObservationScheduler:
    """Objectif: éviter N threads inutiles."""

    def __init__(self, producer: ReliableProducer, generator: FHIRDataGenerator) -> None:
        self.prod = producer
        self.gen = generator
        self.running = True
        self.assignments: Dict[str, List[Dict[str, str]]] = {}
        self.next_emit_ts: Dict[str, float] = {}
        self.stats = {
            "start_ts": time.time(),
            "total_obs": 0,
            "last_metrics_ts": 0.0,
        }

    def init_data(self) -> None:
        log("INFO", "(๑•̀ㅂ•́)و✧ Initialisation FHIR (patients/practitioners)…")
        self.gen.generate_practitioners()
        self.gen.generate_patients()
        self.assignments = self.gen.assign_practitioners_to_patients()

        now = time.time()
        for p in self.gen.patients:
            self.next_emit_ts[p.patient_id] = now + random.uniform(0.2, 1.0)

        self.prod.send_json(TOPICS.AUDIT, key=CLIENT_ID, payload={
            "ts": now_iso(),
            "event": "producer_started",
            "client_id": CLIENT_ID,
            "patients": len(self.gen.patients),
            "practitioners": len(self.gen.practitioners),
        })

        log("INFO", f"ヾ(๑•̀ㅂ•́)و✧ Patients prêts: {len(self.gen.patients)} | Practitioners: {len(self.gen.practitioners)}")
        log("INFO", f"ヾ(๑•̀ㅂ•́)و✧ Publication RAW sur topic: {TOPICS.RAW}")

    def _emit_one(self, patient: PatientProfile) -> None:
        pid = patient.patient_id
        pract = random.choice(self.assignments[pid])

        ts = datetime.now(timezone.utc)
        obs = self.gen.generate_blood_pressure_observation(
            patient_profile=patient,
            practitioner_id=pract["id"],
            practitioner_name=pract["name"],
            timestamp=ts,
        )

        payload = obs.dict()
        self.prod.send_json(TOPICS.RAW, key=pid, payload=payload)
        self.stats["total_obs"] += 1

        #  BUGFIX: Utiliser full_name au lieu de name
        sys, dia, stress, crisis, hypo = patient.simulate_bp(ts)
        log("INFO", f"RAW {patient.full_name}: {sys}/{dia} mm[Hg] (key={pid})")

    def _emit_metrics(self) -> None:
        """Métriques (throughput)."""
        now = time.time()
        elapsed = now - self.stats["start_ts"]
        rate = self.stats["total_obs"] / elapsed if elapsed > 0 else 0.0

        self.prod.send_json(TOPICS.METRICS, key=CLIENT_ID, payload={
            "ts": now_iso(),
            "metric": "producer_throughput",
            "obs_total": self.stats["total_obs"],
            "rate_obs_per_sec": rate,
            "kafka_sent_ok": self.prod.sent_ok,
            "kafka_sent_err": self.prod.sent_err,
        })

        self.stats["last_metrics_ts"] = now

    def run(self) -> None:
        """Boucle principale ."""
        log("INFO", "( •̀ ω •́ )✧ Producer démarré (CTRL+C pour stop).")

        try:
            while self.running:
                now = time.time()

                for patient in self.gen.patients:
                    if now >= self.next_emit_ts.get(patient.patient_id, 0):
                        try:
                            self._emit_one(patient)
                        except Exception as e:
                            log("ERROR", f"_emit_one failed: {e}")
                            self.prod.send_json(TOPICS.ERRORS, key=patient.patient_id, payload={
                                "ts": now_iso(),
                                "error": str(e),
                                "patient_id": patient.patient_id,
                            })

                        next_interval = random.uniform(MIN_SEC_BETWEEN_MEASURES, MAX_SEC_BETWEEN_MEASURES)
                        self.next_emit_ts[patient.patient_id] = now + next_interval

                if now - self.stats["last_metrics_ts"] >= METRICS_EVERY_SEC:
                    self._emit_metrics()

                time.sleep(0.01)

        except KeyboardInterrupt:
            log("INFO", "φ(*￣0￣) Arrêt… flush Kafka")
            self.prod.flush()
            log("INFO", "========== STATS FINALES ==========")
            elapsed = time.time() - self.stats["start_ts"]
            log("INFO", f"durée={int(elapsed)}s obs={self.stats['total_obs']} ok={self.prod.sent_ok} err={self.prod.sent_err}")
            rate = self.stats["total_obs"] / elapsed if elapsed > 0 else 0.0
            log("INFO", f"rate={rate:.2f} obs/s")
            log("INFO", "===================================")

        except Exception as e:
            log("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Boucle principale échouée: {e}")
            self.prod.flush()
            raise

# =========================================================
# 6) main()
# =========================================================

def main() -> None:
    log("INFO", f" Kafka bootstrap: {KAFKA_BOOTSTRAP}")
    ensure_topics(KAFKA_BOOTSTRAP)

    prod = ReliableProducer(KAFKA_BOOTSTRAP)
    gen = FHIRDataGenerator()
    sched = ObservationScheduler(prod, gen)

    sched.init_data()
    sched.run()

if __name__ == "__main__":
    main()
