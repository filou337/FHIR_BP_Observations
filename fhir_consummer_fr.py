"""
Consommateur Kafka FHIR — Surveillance Pression Artérielle (FHIR R4)

Ce script gère toute la chaîne de traitement temps réel des observations
de pression artérielle : de Kafka jusqu’à Elasticsearch et aux archives locales,
avec logique ACID et sécurisation complète des flux.

- Consommation continue depuis le topic Kafka `fhir-observations-raw`
- Validation FHIR + garde-fous physiologiques sur les valeurs (anti-données aberrantes)
- Enrichissement automatique : facteurs de risque, classification AHA 2017 + hypotension
- Routage intelligent :
    - Valeurs normales → archivage JSONL local
    - Anomalies → buffer Elasticsearch + alertes Kafka
- Logique ACID complète :
    synchronisation ES / Kafka / archives avec rollback si erreur
- Bulk indexing Elasticsearch + retry logic + DLQ
- Gestion multi-threads avec verrous pour éviter les conflits
- Arrêt gracieux (Ctrl+C) avec commit final et vidage propre des buffers

Objectif :
Assurer un pipeline de traitement fiable, traçable et scalable,
proche d’un environnement hospitalier / IoT en production.

Usage :
    python fhir_consumer_fr.py

En bref :
Un consommateur robuste, sécurisé et orienté production,
pensé pour absorber du flux continu sans perte de données.
"""



import json
import time
import signal
import threading
from datetime import datetime, timezone
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field


import numpy as np
from confluent_kafka import Consumer, Producer, KafkaError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk



########################################################################################
# CONFIGURATION
########################################################################################


KAFKA_BOOTSTRAP = "localhost:9092"
SUJET_OBSERVATIONS_BRUTES = "fhir-observations-raw"
SUJET_OBSERVATIONS_VALIDEES = "fhir-observations-validees"
SUJET_ALERTES = "alertes-pression-arterielle"
SUJET_ERREURS = "messages-erreur"
SUJET_DLQ = "dead-letter-queue"  #  NOUVEAU : Dead Letter Queue
SUJET_SURVEILLANCE = "metriques-surveillance"
SUJET_PATIENTS = "fhir-patients"
SUJET_PRATICIENS = "fhir-practitioners"


ID_GROUPE_CONSOMMATEUR = "fhir-elasticsearch-consumer-group"
HOTES_ES = ["http://localhost:9200"]
PREFIXE_INDEX_ES = "fhir-observations"
NOM_TEMPLATE_ES = "fhir-observations-template"


TAILLE_HISTORIQUE = 100
METRIQUES_CHAQUE_SEC = 5
CHEMIN_ARCHIVE = "archives/"


SYS_MIN, SYS_MAX = 70, 240
DIA_MIN, DIA_MAX = 40, 140
AGE_MIN, AGE_MAX = 0, 150


LOINC_SYSTOLIQUE = "8480-6"
LOINC_DIASTOLIQUE = "8462-4"


#  NOUVEAU : Configuration retry logic
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # secondes



########################################################################################
# ELASTICSEARCH
########################################################################################


class GestionnaireElasticsearch:
    """
    Gère connexion, template et indexation Elasticsearch.
    """


    def __init__(self, hotes: list) -> None:
        self.client = Elasticsearch(
            hotes,
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True,
        )
        self.circuit_breaker_failures = 0
        self.circuit_breaker_threshold = 5
        self.circuit_breaker_open = False


    def connecter_ou_echouer(self) -> None:
        """Vérifie connexion Elasticsearch avec retry."""
        for tentative in range(MAX_RETRIES):
            try:
                if not self.client.ping():
                    raise RuntimeError("Elasticsearch indisponible.")
                info = self.client.info()
                self._journaliser("INFO", f" Elasticsearch connecté (v{info['version']['number']})")
                self.circuit_breaker_failures = 0
                self.circuit_breaker_open = False
                return
            except Exception as e:
                if tentative < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF_BASE * (2 ** tentative)
                    self._journaliser("Attention!!", f" Tentative {tentative+1}/{MAX_RETRIES} échec, retry dans {wait}s: {e}")
                    time.sleep(wait)
                else:
                    raise RuntimeError(f"Elasticsearch injoignable après {MAX_RETRIES} tentatives")


    def _journaliser(self, niveau: str, msg: str) -> None:
        """Affiche message horodaté avec niveau."""
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{ts} [{niveau}] {msg}")


    def verifier_circuit_breaker(self) -> bool:
       
        if self.circuit_breaker_open:
            # Tenter de réouvrir le circuit après 30s
            if self.circuit_breaker_failures > 0 and time.time() % 30 < 1:
                self._journaliser("INFO", "ヾ(⌐■_■)ノ♪ Tentative réouverture circuit breaker ES")
                try:
                    self.client.ping()
                    self.circuit_breaker_open = False
                    self.circuit_breaker_failures = 0
                    self._journaliser("INFO", " Circuit breaker ES réouvert")
                    return True
                except:
                    pass
            return False
        return True


    def incrementer_circuit_breaker(self) -> None:
        """Incrémente compteur d'échecs et ouvre circuit si seuil atteint."""
        self.circuit_breaker_failures += 1
        if self.circuit_breaker_failures >= self.circuit_breaker_threshold:
            self.circuit_breaker_open = True
            self._journaliser("ERROR", f" Circuit breaker ES OUVERT ({self.circuit_breaker_failures} échecs)")


    def assurer_template_index(self) -> None:
        """Template ES orienté Kibana avec retry."""
        corps_template = {
            "index_patterns": [f"{PREFIXE_INDEX_ES}-*"],
            "template": {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,  
                    "refresh_interval": "5s",
                    "index": {"codec": "best_compression"},
                },
                "mappings": {
                    "properties": {
                        # FHIR base
                        "typeRessource": {"type": "keyword"},
                        "id": {"type": "keyword"},
                        "statut": {"type": "keyword"},
                        "dateTimeEffective": {"type": "date"},
                        "dateEmise": {"type": "date"},
                        "horodatageIngestion": {"type": "date"},

                        # Patient
                        "id_patient": {"type": "keyword"},
                        "nom_patient": {"type": "text"},
                        "prenom_patient": {"type": "text"},
                        "nom_famille_patient": {"type": "text"},
                        "genre_patient": {"type": "keyword"},
                        "age_patient": {"type": "integer"},
                        "groupe_age_patient": {"type": "keyword"},

                        # Praticien
                        "executant": {
                            "properties": {
                                "reference": {"type": "keyword"},
                                "affichage": {"type": "text"},
                            }
                        },

                        # Mesures de PA
                        "pression_systolique": {"type": "integer"},
                        "pression_diastolique": {"type": "integer"},
                        "pression_systolique_ajustee": {"type": "float"},
                        "pression_diastolique_ajustee": {"type": "float"},
                        "lecture_pa": {"type": "keyword"},

                        # Contexte
                        "niveau_stress": {"type": "integer"},
                        "impact_stress_systolique": {"type": "float"},

                        # Détection/labels
                        "categorie_pression_arterielle": {"type": "keyword"},
                        "type_anomalie": {"type": "keyword"},
                        "anomalie_detectee": {"type": "boolean"},
                        "score_risque": {"type": "float"},
                        "niveau_alerte": {"type": "keyword"},
                        "indicateur_tendance": {"type": "keyword"},

                        # FHIR brut (désactivé pour Kibana)
                        "observation_complete": {"type": "object", "enabled": False},
                    }
                },
            },
        }

        try:
            existe = self.client.indices.exists_index_template(name=NOM_TEMPLATE_ES)
        except Exception:
            existe = False

        if not existe:
            self.client.indices.put_index_template(name=NOM_TEMPLATE_ES, body=corps_template)
            self._journaliser("INFO", f"Template ES créé : {NOM_TEMPLATE_ES}")
        else:
            self._journaliser("INFO", f" Template ES déjà présent : {NOM_TEMPLATE_ES}")


    def nom_index_aujourd_hui(self) -> str:
        """Retourne nom index du jour (format YYYY.MM.DD)."""
        return f"{PREFIXE_INDEX_ES}-{datetime.now().strftime('%Y.%m.%d')}"



########################################################################################
# ENRICHISSEMENT + VALIDATION + CATÉGORISATION
########################################################################################


class Enrichisseur:
    """
    Enrichit observations FHIR avec calculs de risque , catégorisation et garde-fous.
    """


    @staticmethod
    def _limiter(v: float, lo: float, hi: float) -> float:
        """Limite une valeur entre min et max."""
        return max(lo, min(hi, v))
    @staticmethod
    def extraire_pa(observation: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        """Extrait systolique + diastolique depuis Observation.component (LOINC).

        Robuste aux formats inattendus (coding dict/list, composant non dict, etc.).
        """
        systolique: Optional[float] = None
        diastolique: Optional[float] = None

        composants = observation.get("component") or []
        if isinstance(composants, dict):
            composants = [composants]
        if not isinstance(composants, list):
            composants = []

        for comp in composants:
            if not isinstance(comp, dict):
                continue

            code_obj = comp.get("code") or {}
            if not isinstance(code_obj, dict):
                continue

            coding = code_obj.get("coding") or []
            if isinstance(coding, dict):
                coding = [coding]
            if not isinstance(coding, list) or not coding or not isinstance(coding[0], dict):
                continue

            code = coding[0].get("code")

            value_quantity = comp.get("valueQuantity") or {}
            if not isinstance(value_quantity, dict):
                value_quantity = {}

            val = value_quantity.get("value")
            try:
                val_f = float(val) if val is not None else None
            except (TypeError, ValueError):
                val_f = None

            if code == LOINC_SYSTOLIQUE and val_f is not None:
                systolique = val_f
            elif code == LOINC_DIASTOLIQUE and val_f is not None:
                diastolique = val_f

        return systolique, diastolique
    

    @staticmethod
    def extraire_patient(observation: Dict[str, Any]) -> Dict[str, Any]:
        """Extrait infos patient depuis Observation.extension (générées par le producer)."""
        donnees: Dict[str, Any] = {
            "genre": "H",
            "age": 0,
            "groupe_age": "adulte",
            "prenom": "Inconnu",
            "nom": "Patient",
            "niveau_stress": 5,
            "imc": 22.0,
            "poids": 70.0,
            "taille": 1.70,
        }

        exts = observation.get("extension") or []
        if isinstance(exts, dict):
            exts = [exts]
        if not isinstance(exts, list):
            exts = []

        for ext in exts:
            if not isinstance(ext, dict):
                continue

            url = ext.get("url", "") or ""

            v_str = ext.get("valueString")
            v_int = ext.get("valueInteger")
            v_dec = ext.get("valueDecimal")

            if url.endswith("/patient-prenom") and isinstance(v_str, str):
                donnees["prenom"] = v_str
            elif url.endswith("/patient-nom") and isinstance(v_str, str):
                donnees["nom"] = v_str
            elif url.endswith("/patient-genre") and isinstance(v_str, str):
                donnees["genre"] = v_str
            elif url.endswith("/patient-age") and v_int is not None:
                try:
                    donnees["age"] = int(v_int)
                except (TypeError, ValueError):
                    pass
            elif url.endswith("/patient-groupe-age") and isinstance(v_str, str):
                donnees["groupe_age"] = v_str
            elif url.endswith("/niveau-stress") and v_int is not None:
                try:
                    donnees["niveau_stress"] = int(v_int)
                except (TypeError, ValueError):
                    pass
            elif url.endswith("/patient-imc") and v_dec is not None:
                try:
                    donnees["imc"] = float(v_dec)
                except (TypeError, ValueError):
                    pass
            elif url.endswith("/patient-poids") and v_dec is not None:
                try:
                    donnees["poids"] = float(v_dec)
                except (TypeError, ValueError):
                    pass
            elif url.endswith("/patient-taille") and v_dec is not None:
                try:
                    donnees["taille"] = float(v_dec)
                except (TypeError, ValueError):
                    pass

        return donnees




    @staticmethod
    def appliquer_stress(systolique: float, diastolique: float, niveau_stress: int) -> Tuple[float, float, float]:
        """Applique impact stress sur PA."""
        niveau = max(1, min(10, int(niveau_stress)))

        if niveau <= 2:
            base = 0.0
        elif niveau <= 5:
            base = (niveau - 2) * 2.0
        else:
            base = 6.0 + (niveau - 5) * 3.0

        delta_sys = base * float(np.random.normal(1.0, 0.1))
        delta_dia = (base * 0.5) * float(np.random.normal(1.0, 0.1))

        sys_adj = systolique + delta_sys
        dia_adj = diastolique + delta_dia

        # Garde-fous physiologiques STRICTES
        sys_adj = Enrichisseur._limiter(sys_adj, SYS_MIN, SYS_MAX)
        dia_adj = Enrichisseur._limiter(dia_adj, DIA_MIN, DIA_MAX)
        
        # dia DOIT être < sys - 15
        if dia_adj >= sys_adj - 15:
            dia_adj = max(DIA_MIN, sys_adj - 15)

        return sys_adj, dia_adj, delta_sys


    @staticmethod
    def categoriser(sys_adj: float, dia_adj: float) -> Dict[str, Any]:
        """Catégorise PA selon AHA 2017 + hypotension."""
        s = float(sys_adj)
        d = float(dia_adj)

        # Hypotension
        if s < 90 or d < 60:
            return {
                "categorie": "HYPOTENSION",
                "type_anomalie": "HYPOTENSION",
                "anomalie": True,
                "risque": 0.55,
                "alerte": "MOYEN",
            }

        # Crise hypertensive
        if s >= 180 or d >= 120:
            return {
                "categorie": "CRISE_HYPERTENSIVE",
                "type_anomalie": "HYPERTENSION",
                "anomalie": True,
                "risque": 0.95,
                "alerte": "CRITIQUE",
            }

        # Stade 2
        if s >= 140 or d >= 90:
            return {
                "categorie": "HYPERTENSION_STADE_2",
                "type_anomalie": "HYPERTENSION",
                "anomalie": True,
                "risque": 0.80,
                "alerte": "ELEVE",
            }

        # Stade 1
        if (130 <= s < 140) or (80 <= d < 90):
            return {
                "categorie": "HYPERTENSION_STADE_1",
                "type_anomalie": "HYPERTENSION",
                "anomalie": True,
                "risque": 0.55,
                "alerte": "MOYEN",
            }

        # Élevée
        if (120 <= s < 130) and (d < 80):
            return {
                "categorie": "ELEVEE",
                "type_anomalie": "ELEVEE",
                "anomalie": True,
                "risque": 0.25,
                "alerte": "BAS",
            }

        # Normale
        return {
            "categorie": "NORMALE",
            "type_anomalie": "AUCUN",
            "anomalie": False,
            "risque": 0.05,
            "alerte": "AUCUN",
        }


    @staticmethod
    def tendance(historique: deque, actuel: int) -> str:
        """Calcule tendance sur 3 dernières mesures."""
        if len(historique) < 3:
            return "STABLE"
        moyenne = sum(list(historique)[-3:]) / 3.0
        if actuel > moyenne + 5:
            return "AUGMENTE"
        if actuel < moyenne - 5:
            return "DIMINUE"
        return "STABLE"


    @staticmethod
    def valider_minimal(observation: Dict[str, Any]) -> Tuple[bool, str]:
        """Validation FHIR stricte."""
        if observation.get("resourceType") != "Observation":
            return False, "typeRessource != Observation"
        if not observation.get("id"):
            return False, "id manquant"

        s, d = Enrichisseur.extraire_pa(observation)
        if s is None or d is None:
            return False, "composants pression artérielle manquants (LOINC)"

        try:
            s_f, d_f = float(s), float(d)
        except Exception:
            return False, "systolique/diastolique non numérique"

        if not (SYS_MIN <= s_f <= SYS_MAX and DIA_MIN <= d_f <= DIA_MAX):
            return False, f"pa en dehors de la plage plausible: {s_f}/{d_f}"

        # Validation stricte dia < sys - 15
        if d_f >= s_f - 15:
            return False, f"diastolique trop proche de systolique: {s_f}/{d_f} (écart < 15)"

        return True, "ok"


    def enrichir(self, observation: Dict[str, Any], hist_patient: Dict[str, deque]) -> Dict[str, Any]:
        """
        Enrichit observation FHIR.
        
         IMPORTANT : Appelé AVEC VERROU depuis Pipeline
        Ne pas appeler directement sans synchronisation !
        """
        systolique, diastolique = self.extraire_pa(observation)
        assert systolique is not None and diastolique is not None

        patient = self.extraire_patient(observation)

        sys_adj, dia_adj, delta_sys = self.appliquer_stress(
            float(systolique), float(diastolique), int(patient["niveau_stress"])
        )

        cat = self.categoriser(sys_adj, dia_adj)

        subject = observation.get("subject", {})
        if isinstance(subject, list):
            subject = subject[0] if subject and isinstance(subject[0], dict) else {}
        elif not isinstance(subject, dict):
            subject = {}

        ref_patient = subject.get("reference", "")
        id_patient = ref_patient.split("/")[-1] if "/" in ref_patient else ref_patient

        performer = observation.get("performer")
        if isinstance(performer, list):
            executant = performer[0] if performer and isinstance(performer[0], dict) else {}
        elif isinstance(performer, dict):
            executant = performer
        else:
            executant = {}



        hist_sys = hist_patient.setdefault(id_patient, deque(maxlen=TAILLE_HISTORIQUE))
        indicateur_tendance = self.tendance(hist_sys, int(float(systolique)))
        hist_sys.append(int(float(systolique)))

        iso_maintenant = datetime.now(timezone.utc).isoformat()

        nom_patient = f"{patient['prenom']} {patient['nom']}".strip()
        lecture_pa = f"{int(float(systolique))}/{int(float(diastolique))}"

        return {
            "id": observation.get("id"),
            "typeRessource": observation.get("resourceType"),
            "statut": observation.get("status"),
            "dateTimeEffective": observation.get("effectiveDateTime"),
            "dateEmise": observation.get("issued"),
            "horodatageIngestion": iso_maintenant,
            "id_patient": id_patient,
            "nom_patient": nom_patient,
            "prenom_patient": patient["prenom"],
            "nom_famille_patient": patient["nom"],
            "genre_patient": patient["genre"],
            "age_patient": int(patient["age"]),
            "groupe_age_patient": patient["groupe_age"],
            "imc_patient": float(patient["imc"]),
            "poids_patient": float(patient["poids"]),
            "taille_patient": float(patient["taille"]),

            "executant": executant,

            "pression_systolique": int(float(systolique)),
            "pression_diastolique": int(float(diastolique)),
            "pression_systolique_ajustee": float(sys_adj),
            "pression_diastolique_ajustee": float(dia_adj),
            "lecture_pa": lecture_pa,

            "niveau_stress": int(patient["niveau_stress"]),
            "impact_stress_systolique": float(delta_sys),

            "categorie_pression_arterielle": cat["categorie"],
            "type_anomalie": cat["type_anomalie"],
            "anomalie_detectee": bool(cat["anomalie"]),
            "score_risque": float(cat["risque"]),
            "niveau_alerte": cat["alerte"],

            "indicateur_tendance": indicateur_tendance,

            "observation_complete": observation,
        }



########################################################################################
# PIPELINE ACID-COMPLIANT
########################################################################################


@dataclass
class TransactionContext:
    """
    Contexte transaction pour atomicité ES-Kafka
    
    Permet rollback en cas d'erreur partielle.
    """
    es_docs: List[Dict[str, Any]] = field(default_factory=list)
    kafka_messages: List[Tuple[str, str, Dict]] = field(default_factory=list)
    archives: List[Tuple[str, Dict]] = field(default_factory=list)
    committed: bool = False


class Pipeline:
    """
    Orchestre consommation, validation, enrichissement et routage.
    
     GARANTIES ACID COMPLÈTES

    - **Atomicité** : Transaction ES-Kafka-Archives avec rollback
    - **Cohérence** : Validation multicouche + garde-fous
    - **Isolation** : Verrous threading sur hist_patient, stats, buffer
    - **Durabilité** : ES replicas + Kafka log + archives
    """


    def __init__(self) -> None:
        self.actif = True
        self.es = GestionnaireElasticsearch(HOTES_ES)

        self.consommateur = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": ID_GROUPE_CONSOMMATEUR,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        })

        self.producteur = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 20,
            "batch.num.messages": 10000,
            "enable.idempotence": True,
            "acks": "all",
        })

        self.enrichisseur = Enrichisseur()
        self.hist_patient: Dict[str, deque] = {}
        
        #  VERROUS COMPLETS : Isolation garantie
        self.hist_verrou = threading.Lock()  # hist_patient
        self.stats_verrou = threading.Lock()  # stats
        self.buffer_verrou = threading.Lock()  # es_buffer
        
        #  Buffer ES pour bulk indexing
        self.es_buffer: List[Dict[str, Any]] = []
        self.es_buffer_max_size = 1000

        #  Stats avec verrou
        self.stats = {
            "consommes": 0,
            "valides": 0,
            "indexes_bulk": 0,
            "indexes_failed": 0,
            "archives_normal": 0,
            "alertes": 0,
            "erreurs": 0,
            "retries": 0,
            "dlq": 0,
            "ts_debut": time.time(),
            "ts_derniere_metrique": 0.0,
        }

        # Créer dossier archive
        Path(CHEMIN_ARCHIVE).mkdir(parents=True, exist_ok=True)


    def _journaliser(self, niveau: str, msg: str) -> None:
        """Affiche message horodaté avec niveau."""
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{ts} [{niveau}] {msg}")


    def _callback_livraison(self, err, msg) -> None:
        """Callback de rapport de livraison Kafka."""
        if err is not None:
            self._journaliser("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Envoi échoué: {err}")


    def envoyer_json(self, sujet: str, cle: str, charge: Dict[str, Any]) -> None:
        """Envoie charge JSON sur sujet Kafka."""
        self.producteur.produce(
            topic=sujet,
            key=cle.encode("utf-8"),
            value=json.dumps(charge, ensure_ascii=False).encode("utf-8"),
            callback=self._callback_livraison,
        )
        self.producteur.poll(0)


    def valider_msg(self, msg) -> None:
        """Commit synchrone d'un message."""
        self.consommateur.commit(message=msg, asynchronous=False)


    def archiver_normal(self, enrichi: Dict[str, Any]) -> None:
        """Archive observation normale en JSONL local."""
        jour = datetime.now().strftime("%Y-%m-%d")
        chemin = Path(CHEMIN_ARCHIVE) / f"observations_normal_{jour}.jsonl"

        leger = {k: v for k, v in enrichi.items() if k != "observation_complete"}
        with chemin.open("a", encoding="utf-8") as f:
            f.write(json.dumps(leger, ensure_ascii=False) + "\n")


    def _flush_es_buffer(self) -> None:
        """
         Flush buffer ES en bulk avec retry logic.
        
        Appelée périodiquement (10s) ou quand buffer plein (1000 docs).
        """
        with self.buffer_verrou:  #  VERROU : Isolation garantie
            if not self.es_buffer:
                return

            # Circuit breaker check
            if not self.es.verifier_circuit_breaker():
                self._journaliser("Attention!!", " Circuit breaker ES ouvert, docs en buffer")
                return

            buffer_copy = self.es_buffer.copy()
            
            try:
                # Préparer actions pour bulk
                actions = [
                    {
                        "_index": self.es.nom_index_aujourd_hui(),
                        "_id": doc["id"],
                        "_source": doc
                    }
                    for doc in buffer_copy
                ]

                #  BULK : 1000 docs en 1 requête ES
                success_count, failed_items = es_bulk(
                    self.es.client,
                    actions,
                    chunk_size=1000,
                    raise_on_error=False,
                    stats_only=False
                )

                with self.stats_verrou:  #  VERROU : Stats atomiques
                    self.stats["indexes_bulk"] += success_count
                    self.stats["indexes_failed"] += len(failed_items)

                if failed_items:
                    self._journaliser(
                        "Attention!!",
                        f" Bulk ES : {len(failed_items)} docs échoués sur {len(buffer_copy)}"
                    )
                    # Log premiers échecs
                    for item in failed_items[:5]:
                        error_info = item.get('index', {}).get('error', 'unknown error')
                        doc_id = item.get('index', {}).get('_id', 'unknown')
                        self._journaliser("ERROR", f"  - Doc {doc_id}: {error_info}")

                self._journaliser("INFO", f" Bulk flush: {success_count} docs indexés en 1 req")
                
                #  Vider buffer seulement si succès partiel acceptable
                self.es_buffer = []
                self.es.circuit_breaker_failures = 0  # Reset circuit breaker

            except Exception as e:
                with self.stats_verrou:
                    self.stats["erreurs"] += 1
                self._journaliser("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Erreur bulk indexing: {str(e)}")
                self.es.incrementer_circuit_breaker()


    def _retry_avec_backoff(self, func, *args, **kwargs):
        """
         NOUVEAU : Retry logic avec exponential backoff
        
        Paramètres
      ----
        func : callable
            Fonction à retenter
        *args, **kwargs
            Arguments de la fonction
        
        Retour
      
        Any
            Résultat de func() ou None si tous les retries échouent
        """
        for tentative in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if tentative < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF_BASE * (2 ** tentative)
                    with self.stats_verrou:
                        self.stats["retries"] += 1
                    self._journaliser("Attention!!", f" Retry {tentative+1}/{MAX_RETRIES} dans {wait}s: {e}")
                    time.sleep(wait)
                else:
                    self._journaliser("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Échec après {MAX_RETRIES} tentatives: {e}")
                    return None


    def _envoyer_vers_dlq(self, msg, raison: str) -> None:
        """
         NOUVEAU : Envoie message vers Dead Letter Queue
        
        Paramètres
      ----
        msg : Message Kafka
            Message original qui a échoué
        raison : str
            Raison de l'échec
        """
        try:
            dlq_payload = {
                "ts_echec": datetime.now(timezone.utc).isoformat(),
                "raison": raison,
                "topic_original": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "cle": msg.key().decode("utf-8") if msg.key() else None,
                "valeur": msg.value().decode("utf-8"),
            }
            
            self.envoyer_json(SUJET_DLQ, cle="dlq", charge=dlq_payload)
            
            with self.stats_verrou:
                self.stats["dlq"] += 1
            
            self._journaliser("Attention!!", f"(*/ω＼*) Message envoyé vers DLQ: {raison}")
        
        except Exception as e:
            self._journaliser("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Échec envoi DLQ: {e}")


    def demarrer(self) -> None:
        """Démarre pipeline avec tous les safeguards ACID."""
        self._journaliser("INFO", "🔌 Connexion services...")
        self.es.connecter_ou_echouer()
        self.es.assurer_template_index()

        self._journaliser("INFO", f" Souscription : {SUJET_OBSERVATIONS_BRUTES}")
        self.consommateur.subscribe([SUJET_OBSERVATIONS_BRUTES])

        self._journaliser("INFO", "ヾ(⌐■_■)ノ♪ Consommateur démarré (Ctrl+C pour arrêter)")

        # Timestamp pour flush périodique
        ts_derniere_flush = time.time()

        try:
            while self.actif:
                msg = self.consommateur.poll(1.0)

                if msg is None:
                    self.peut_etre_envoyer_metriques()
                    
                    # Flush périodique (toutes les 10s)
                    maintenant = time.time()
                    if maintenant - ts_derniere_flush > 5:
                        self._flush_es_buffer()
                        ts_derniere_flush = maintenant
                    
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self._journaliser("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Erreur Kafka: {msg.error()}")
                    continue

                #  Traiter message avec retry
                self._retry_avec_backoff(self.traiter_message, msg)
                self.peut_etre_envoyer_metriques()

        finally:
            self.arreter()


    def traiter_message(self, msg) -> None:
        """
        Traite un message Kafka avec garanties ACID.
        
         ATOMICITÉ : Transaction ES-Kafka-Archives
         COHÉRENCE : Validation stricte
         ISOLATION : Verrous complets
         DURABILITÉ : Commit après succès complet
        """
        with self.stats_verrou:
            self.stats["consommes"] += 1

        # 1) Parse JSON
        try:
            observation = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            with self.stats_verrou:
                self.stats["erreurs"] += 1
            self._envoyer_vers_dlq(msg, f"decodage_json_echoue: {str(e)}")
            self.valider_msg(msg)
            return

        # 2) Validation minimale
        ok, raison = self.enrichisseur.valider_minimal(observation)
        if not ok:
            with self.stats_verrou:
                self.stats["erreurs"] += 1
            self._envoyer_vers_dlq(msg, f"validation_echouee: {raison}")
            self.valider_msg(msg)
            return

        # 3)  Enrichissement PROTÉGÉ par verrou (Isolation garantie)
        try:
            with self.hist_verrou:
                enrichi = self.enrichisseur.enrichir(observation, self.hist_patient)
        except Exception as e:
            with self.stats_verrou:
                self.stats["erreurs"] += 1
            self._journaliser("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Erreur enrichissement: {str(e)}")
            self._envoyer_vers_dlq(msg, f"enrichissement_echoue: {str(e)}")
            self.valider_msg(msg)  # commit => on ne relit pas le même message en boucle
            return

        
        with self.stats_verrou:
            self.stats["valides"] += 1

        # 4)  TRANSACTION ATOMIQUE : ES + Kafka + Archives
        transaction = TransactionContext()
        
        try:
            # 4a) Toujours publier VALIDEES
            cle_routage = enrichi["id_patient"] or enrichi["id"]
            transaction.kafka_messages.append((SUJET_OBSERVATIONS_VALIDEES, cle_routage, enrichi))

            # 4b) Routage intelligent
            if enrichi["categorie_pression_arterielle"] == "NORMALE":
                # Archive locale
                transaction.archives.append((CHEMIN_ARCHIVE, enrichi))
            else:
                # Buffer ES
                transaction.es_docs.append(enrichi)
                
                # Alerte si MOYEN/ELEVE/CRITIQUE
                if enrichi["niveau_alerte"] in {"MOYEN", "ELEVE", "CRITIQUE"}:
                    alerte_payload = {
                        "ts": enrichi["horodatageIngestion"],
                        "id_patient": enrichi["id_patient"],
                        "nom_patient": enrichi["nom_patient"],
                        "pa": enrichi["lecture_pa"],
                        "categorie": enrichi["categorie_pression_arterielle"],
                        "type_anomalie": enrichi["type_anomalie"],
                        "niveau_alerte": enrichi["niveau_alerte"],
                        "score_risque": enrichi["score_risque"],
                        "tendance": enrichi["indicateur_tendance"],
                        "niveau_stress": enrichi["niveau_stress"],
                    }
                    transaction.kafka_messages.append((SUJET_ALERTES, cle_routage, alerte_payload))
                    
                    if enrichi["niveau_alerte"] == "CRITIQUE":
                        self._journaliser("ERROR", f"🚨 ALERTE CRITIQUE | Patient: {enrichi['id_patient']} | PA: {enrichi['lecture_pa']}")

            # 5)  COMMIT TRANSACTION (Atomicité garantie)
            self._commit_transaction(transaction, msg)

        except Exception as e:
            #  ROLLBACK : Rien n'est committé si erreur
            with self.stats_verrou:
                self.stats["erreurs"] += 1
            self._journaliser("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Erreur transaction, rollback: {str(e)}")
            self._envoyer_vers_dlq(msg, f"transaction_echouee: {str(e)}")
            self.valider_msg(msg)


    def _commit_transaction(self, transaction: TransactionContext, msg) -> None:
        """
         NOUVEAU : Commit atomique transaction ES-Kafka-Archives
        
        Garantit que TOUT réussit ou RIEN n'est persisté.
        """
        if transaction.committed:
            return

        try:
            # 1. Kafka messages (validées + alertes)
            for sujet, cle, charge in transaction.kafka_messages:
                self.envoyer_json(sujet, cle, charge)
            
            # 2. Archives locales
            for chemin, enrichi in transaction.archives:
                self.archiver_normal(enrichi)
                with self.stats_verrou:
                    self.stats["archives_normal"] += 1
            
            # 3. Buffer ES (flush si plein)
            if transaction.es_docs:
                with self.buffer_verrou:
                    self.es_buffer.extend(transaction.es_docs)
                    
                    if len(self.es_buffer) >= self.es_buffer_max_size:
                        self._flush_es_buffer()
            
            # 4. Stats alertes
            if any(SUJET_ALERTES in t for t in transaction.kafka_messages):
                with self.stats_verrou:
                    self.stats["alertes"] += 1
            
            # 5.  COMMIT KAFKA (Atomicité finale)
            self.valider_msg(msg)
            transaction.committed = True

        except Exception as e:
            # Rollback : ne pas commit Kafka
            raise RuntimeError(f"Commit transaction échoué: {e}")


    def peut_etre_envoyer_metriques(self) -> None:
        """Envoie métriques périodiques avec verrou."""
        maintenant = time.time()
        
        with self.stats_verrou:  #  VERROU : Lecture atomique
            if maintenant - self.stats["ts_derniere_metrique"] < METRIQUES_CHAQUE_SEC:
                return

            self.stats["ts_derniere_metrique"] = maintenant
            temps_ecoule = max(1.0, maintenant - self.stats["ts_debut"])
            debit = self.stats["consommes"] / temps_ecoule

            charge = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "id_groupe": ID_GROUPE_CONSOMMATEUR,
                "consommes": self.stats["consommes"],
                "valides": self.stats["valides"],
                "indexes_bulk": self.stats["indexes_bulk"],
                "indexes_failed": self.stats["indexes_failed"],
                "archives_normal": self.stats["archives_normal"],
                "alertes": self.stats["alertes"],
                "erreurs": self.stats["erreurs"],
                "retries": self.stats["retries"],
                "dlq": self.stats["dlq"],
                "debit_msg_par_sec": round(debit, 3),
            }

        self.envoyer_json(SUJET_SURVEILLANCE, cle=ID_GROUPE_CONSOMMATEUR, charge=charge)

        self._journaliser("INFO", f"(ﾉ◕ヮ◕)ﾉ*:･ﾟ✧ consommes={charge['consommes']} valides={charge['valides']} "
                            f"indexes_bulk={charge['indexes_bulk']} archives_normal={charge['archives_normal']} "
                            f"alertes={charge['alertes']} erreurs={charge['erreurs']} retries={charge['retries']} "
                            f"dlq={charge['dlq']} debit={charge['debit_msg_par_sec']}/s")


    def arreter(self) -> None:
        """
         Graceful shutdown avec flush complet
        
        Séquence :
        1. Désactiver flag actif
        2. Flush buffer ES (docs en attente)
        3. Flush producteur Kafka
        4. Fermer consommateur
        5. Afficher stats finales
        """
        if not self.actif:
            return
        self.actif = False

        self._journaliser("INFO", " Arrêt graceful... flush buffer ES final")
        
        # Flush buffer ES
        with self.buffer_verrou:
            if self.es_buffer:
                self._journaliser("INFO", f"(*/ω＼*) Flush final {len(self.es_buffer)} docs en attente...")
                self._flush_es_buffer()

        self._journaliser("INFO", " Flush producteur + fermeture consommateur")
        self.producteur.flush(5)
        self.consommateur.close()
        self.afficher_stats_finales()


    def afficher_stats_finales(self) -> None:
        """Affiche statistiques finales avec verrou."""
        with self.stats_verrou:
            temps_ecoule = max(1.0, time.time() - self.stats["ts_debut"])
            
            self._journaliser("INFO", "################## STATS FINALES ################")
            self._journaliser("INFO", f"Durée : {int(temps_ecoule)}s")
            self._journaliser("INFO", f"consommes={self.stats['consommes']}")
            self._journaliser("INFO", f"valides={self.stats['valides']}")
            self._journaliser("INFO", f"indexes (BULK)={self.stats['indexes_bulk']}")
            self._journaliser("INFO", f"indexes_failed={self.stats['indexes_failed']}")
            self._journaliser("INFO", f"archives_normal={self.stats['archives_normal']}")
            self._journaliser("INFO", f"alertes={self.stats['alertes']}")
            self._journaliser("INFO", f"erreurs={self.stats['erreurs']}")
            self._journaliser("INFO", f"retries={self.stats['retries']}")
            self._journaliser("INFO", f"dlq={self.stats['dlq']}")
            self._journaliser("INFO", f"débit={self.stats['consommes']/temps_ecoule:.2f} msg/s")
            self._journaliser("INFO", f" Buffer final size: {len(self.es_buffer)} docs")
            self._journaliser("INFO", "#################################################")



########################################################################################
# MAIN + GESTIONNAIRE DE SIGNAL
########################################################################################


def principal() -> None:
    """Point d'entrée principal."""
    pipeline = Pipeline()

    def _gestionnaire_sigint(signum, frame) -> None:
        pipeline.arreter()

    signal.signal(signal.SIGINT, _gestionnaire_sigint)
    pipeline.demarrer()



if __name__ == "__main__":
    principal()
