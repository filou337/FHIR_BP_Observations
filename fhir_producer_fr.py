"""
Producteur Kafka FHIR — Surveillance Pression Artérielle (FHIR R4)

- Génération réaliste de mesures de PA toutes les 2 à 5 secondes
- Détection continue des crises hypertensives (≥180 / 120 sur 3 mesures)
- Sauvegarde persistante des états de crise dans Redis (TTL 24h)
- ThreadPoolExecutor → 1 thread par patient + verrous pour éviter les conflits
- Arrêt gracieux (Ctrl+C) sans perte de messages Kafka
- Monitoring périodique + métriques de débit

Objectif :
Simuler un environnement proche d’un système hospitalier IoT / streaming
afin de tester une pipeline Kafka → Elasticsearch → Kibana.

Usage :
    python fhir_producer_fr_PRODUCTION.py

Arrêt :
    Ctrl + C → flush Kafka automatique + sauvegarde des états Redis

En bref :
Un producteur robuste, temps réel, pensé pour la démo comme pour la montée en charge.
"""


import json
import random 
import signal
import threading # threading pour isolation états crises (verrou) et gestion signal d'arrêt
import time
from concurrent.futures import ThreadPoolExecutor # ThreadPoolExecutor pour gestion threads (le thread est la gestion de chaque message patient)
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dataclasses import dataclass


import redis # Redis pour persistence états crises
from confluent_kafka import Producer 
from fhir.resources.observation import Observation
from fhir.resources.patient import Patient as FHIRPatient
from fhir.resources.practitioner import Practitioner
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
from fhir.resources.humanname import HumanName
from fhir.resources.identifier import Identifier
from fhir.resources.quantity import Quantity
from fhir.resources.reference import Reference
from fhir.resources.extension import Extension
from fhir.resources.observation import ObservationComponent


# Import générateur (supposé présent)
try:
    from fhir_data_generator_fr import GenerateurDonneesFHIR
except ImportError:
    print(" ERREUR : fhir_data_generator_fr.py non trouvé")
    print("   Assurez-vous que le fichier est dans le même répertoire.")
    exit(1)



########################################################################################
# CONFIGURATION
########################################################################################


KAFKA_BOOTSTRAP = "localhost:9092"
SUJET_OBSERVATIONS_BRUTES = "fhir-observations-raw"
SUJET_PATIENTS = "fhir-patients"
SUJET_PRATICIENS = "fhir-practitioners"
SUJET_METRIQUES = "metriques-producteur"


#  NOUVEAU : Redis pour persistence états crises
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_KEY_PREFIX = "fhir:crisis:state:"


# Délais adaptatifs réalistes 
DELAI_MIN = 2.0
DELAI_MAX = 5.0


# Seuils crises (AHA 2017)
SEUIL_CRISE_SYSTOLIQUE = 180
SEUIL_CRISE_DIASTOLIQUE = 120
MESURES_CONSECUTIVES_CRISE = 3
DUREE_CRISE_MIN = 30  # secondes
DUREE_CRISE_MAX = 60  # secondes



########################################################################################
# ÉTAT CRISE (avec durabilité Redis)
########################################################################################


@dataclass
class EtatCrise:
    """
    État crise patient avec persistence Redis.
    
     DURABILITÉ GARANTIE
  
    - Sauvegarde Redis à chaque changement
    - Restauration au démarrage
    - TTL 24h (nettoyage auto)
    """
    en_crise: bool = False
    compteur_mesures_anormales: int = 0
    ts_debut_crise: Optional[float] = None
    duree_crise_secondes: float = 45.0

    def to_dict(self) -> Dict[str, Any]:
        """Sérialise pour Redis."""
        return {
            "en_crise": self.en_crise,
            "compteur_mesures_anormales": self.compteur_mesures_anormales,
            "ts_debut_crise": self.ts_debut_crise,
            "duree_crise_secondes": self.duree_crise_secondes,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EtatCrise":
        """Désérialise depuis Redis."""
        return cls(
            en_crise=data.get("en_crise", False),
            compteur_mesures_anormales=data.get("compteur_mesures_anormales", 0),
            ts_debut_crise=data.get("ts_debut_crise"),
            duree_crise_secondes=data.get("duree_crise_secondes", 45.0),
        )



########################################################################################
# GESTIONNAIRE REDIS PERSISTENCE
########################################################################################


class GestionnaireRedis:
    """
    Gère persistence Redis pour états crises.

    """

    def __init__(self, host: str = REDIS_HOST, port: int = REDIS_PORT, db: int = REDIS_DB) -> None:
        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
            )
            self.client.ping()
            self._log("INFO", " Redis connecté")
        except Exception as e:
            self._log("WARNING", f" Redis indisponible (mode dégradé) : {e}")
            self.client = None

    def _log(self, niveau: str, msg: str) -> None:
        """Log horodaté."""
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{ts} [REDIS-{niveau}] {msg}")

    def sauvegarder_etat(self, patient_id: str, etat: EtatCrise) -> None:
        """Sauvegarde état crise avec TTL 24h."""
        if not self.client:
            return

        cle = f"{REDIS_KEY_PREFIX}{patient_id}"
        try:
            self.client.setex(
                name=cle,
                time=86400,  # 24h TTL
                value=json.dumps(etat.to_dict()),
            )
        except Exception as e:
            self._log("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Échec sauvegarde état {patient_id}: {e}")

    def charger_etat(self, patient_id: str) -> EtatCrise:
        """Charge état crise depuis Redis ou retourne état vide."""
        if not self.client:
            return EtatCrise()

        cle = f"{REDIS_KEY_PREFIX}{patient_id}"
        try:
            data_json = self.client.get(cle)
            if data_json:
                data = json.loads(data_json)
                self._log("INFO", f" État restauré : {patient_id}")
                return EtatCrise.from_dict(data)
        except Exception as e:
            self._log("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Échec chargement état {patient_id}: {e}")

        return EtatCrise()

    def supprimer_etat(self, patient_id: str) -> None:
        """Supprime état crise."""
        if not self.client:
            return

        cle = f"{REDIS_KEY_PREFIX}{patient_id}"
        try:
            self.client.delete(cle)
        except Exception as e:
            self._log("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Échec suppression état {patient_id}: {e}")

    def lister_etats(self) -> List[str]:
        """Liste tous les patient_id avec états en Redis."""
        if not self.client:
            return []

        try:
            cles = self.client.keys(f"{REDIS_KEY_PREFIX}*")
            return [cle.replace(REDIS_KEY_PREFIX, "") for cle in cles]
        except Exception:
            return []



########################################################################################
# PRODUCTEUR PRINCIPAL
########################################################################################


class ProducteurFHIR:
    """
    Producteur Kafka FHIR avec ThreadPoolExecutor et Redis persistence.
    """

    def __init__(self) -> None:
        self.actif = True
        self.generateur = GenerateurDonneesFHIR()
        
        #  Redis persistence
        self.redis = GestionnaireRedis()
        
        # Kafka producteur
        self.producteur = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 20,
            "batch.num.messages": 1000,
            "enable.idempotence": True,
            "acks": "all",
        })

        # Génération données
        self.generateur.generer_praticiens()
        self.patients = self.generateur.generer_patients()
        self.praticiens = self.generateur.praticiens

        #  États crises avec restauration Redis
        self.etats_crises: Dict[str, EtatCrise] = {}
        self.etats_verrou = threading.Lock()  #  Isolation
        self._restaurer_etats_depuis_redis()

        # Stats
        self.stats_verrou = threading.Lock()
        self.stats = {
            "observations_envoyees": 0,
            "crises_declenchees": 0,
            "crises_terminees": 0,
            "ts_debut": time.time(),
        }

        #  ThreadPoolExecutor
        self.executor = ThreadPoolExecutor(max_workers=len(self.patients), thread_name_prefix="patient-")

    def _log(self, niveau: str, msg: str) -> None:
        """Log horodaté."""
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{ts} [{niveau}] {msg}")

    def _restaurer_etats_depuis_redis(self) -> None:
        """
          Restaure états crises depuis Redis au démarrage

        """
        for patient in self.patients:
            etat = self.redis.charger_etat(patient.id_patient)
            self.etats_crises[patient.id_patient] = etat

        nb_etats_restaures = sum(1 for e in self.etats_crises.values() if e.en_crise)
        if nb_etats_restaures > 0:
            self._log("INFO", f"ヾ(⌐■_■)ノ♪ {nb_etats_restaures} états crises restaurés depuis Redis")

    def _sauvegarder_etat_redis(self, patient_id: str) -> None:
        """
  Sauvegarde état crise dans Redis
        
        Appelé à chaque changement d'état pour durabilité.
        """
        with self.etats_verrou:
            etat = self.etats_crises.get(patient_id)
            if etat:
                self.redis.sauvegarder_etat(patient_id, etat)

    def _callback_livraison(self, err, msg) -> None:
        """Callback Kafka livraison."""
        if err:
            self._log("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Échec envoi: {err}")

    def envoyer_json(self, sujet: str, cle: str, charge: Dict[str, Any]) -> None:
        """Envoie message JSON sur sujet Kafka."""
        self.producteur.produce(
            topic=sujet,
            key=cle.encode("utf-8"),
            value=json.dumps(charge, ensure_ascii=False).encode("utf-8"),
            callback=self._callback_livraison,
        )
        self.producteur.poll(0)

    def publier_patients(self) -> None:
        """Publie patients sur topic dédié."""
        for patient in self.patients:
            patient_fhir = self.generateur.creer_patient_fhir(patient)
            self.envoyer_json(SUJET_PATIENTS, cle=patient.id_patient, charge=patient_fhir)
        
        self._log("INFO", f"(*/ω＼*) {len(self.patients)} patients publiés")

    def publier_praticiens(self) -> None:
        """Publie praticiens sur topic dédié."""
        for prat_id, prat_nom in self.praticiens.items():
            prat_fhir = self.generateur.creer_praticien_fhir(prat_id, prat_nom)
            self.envoyer_json(SUJET_PRATICIENS, cle=prat_id, charge=prat_fhir)
        
        self._log("INFO", f"(*/ω＼*) {len(self.praticiens)} praticiens publiés")

    def detecter_et_gerer_crise(
        self,
        patient,
        systolique: int,
        diastolique: int,
        observation_fhir: Dict[str, Any],
    ) -> bool:
        """
        Détecte et gère crises hypertensives avec persistence Redis.
        
         DURABILITÉ GARANTIE
      
        - Sauvegarde Redis à chaque changement d'état
        - Restauration au restart
        - Continuité médicale préservée
        
        Paramètres
      ----
        patient : ProfilPatient
            Profil patient
        systolique : int
            PA systolique
        diastolique : int
            PA diastolique
        observation_fhir : dict
            Observation FHIR (pour ajout extension)
        
        Retour
      
        bool
            True si en crise, False sinon
        """
        with self.etats_verrou:  #  Isolation
            etat = self.etats_crises.get(patient.id_patient)
            if not etat:
                etat = EtatCrise()
                self.etats_crises[patient.id_patient] = etat

            maintenant = time.time()
            mesure_anormale = systolique >= SEUIL_CRISE_SYSTOLIQUE or diastolique >= SEUIL_CRISE_DIASTOLIQUE

            # Déjà en crise ?
            if etat.en_crise:
                duree_ecoule = maintenant - etat.ts_debut_crise
                if duree_ecoule >= etat.duree_crise_secondes:
                    # Fin de crise
                    etat.en_crise = False
                    etat.compteur_mesures_anormales = 0
                    etat.ts_debut_crise = None
                    
                    with self.stats_verrou:
                        self.stats["crises_terminees"] += 1
                    
                    self._log("INFO", f" FIN CRISE | Patient: {patient.id_patient} | Durée: {duree_ecoule:.1f}s")
                    
                    #  Sauvegarder dans Redis
                    self.redis.sauvegarder_etat(patient.id_patient, etat)
                    return False
                else:
                    # Toujours en crise
                    observation_fhir.setdefault("extension", []).append({
                        "url": "http://example.org/fhir/StructureDefinition/crise-hypertensive",
                        "valueBoolean": True,
                    })
                    return True

            # Pas en crise, vérifier déclenchement
            if mesure_anormale:
                etat.compteur_mesures_anormales += 1
                if etat.compteur_mesures_anormales >= MESURES_CONSECUTIVES_CRISE:
                    # Déclenchement crise
                    etat.en_crise = True
                    etat.ts_debut_crise = maintenant
                    etat.duree_crise_secondes = random.uniform(DUREE_CRISE_MIN, DUREE_CRISE_MAX)
                    
                    with self.stats_verrou:
                        self.stats["crises_declenchees"] += 1
                    
                    self._log("ERROR", f"(っ °Д °;)っ DÉBUT CRISE | Patient: {patient.id_patient} | "
                                       f"PA: {systolique}/{diastolique} | "
                                       f"Durée prévue: {etat.duree_crise_secondes:.1f}s")
                    
                    observation_fhir.setdefault("extension", []).append({
                        "url": "http://example.org/fhir/StructureDefinition/crise-hypertensive",
                        "valueBoolean": True,
                    })
                    
                    #  Sauvegarder dans Redis
                    self.redis.sauvegarder_etat(patient.id_patient, etat)
                    return True
                else:
                    #  Sauvegarder compteur incrémenté
                    self.redis.sauvegarder_etat(patient.id_patient, etat)
            else:
                # Mesure normale, reset compteur
                if etat.compteur_mesures_anormales > 0:
                    etat.compteur_mesures_anormales = 0
                    #  Sauvegarder reset
                    self.redis.sauvegarder_etat(patient.id_patient, etat)

            return False

    def _thread_patient(self, patient) -> None:
        """
        Génère et envoie observations périodiquement.
        """
        praticien_id = random.choice(list(self.praticiens.keys()))
        praticien_nom = self.praticiens[praticien_id]

        while self.actif:
            try:
                horodatage = datetime.now(timezone.utc)
                
                # Générer observation
                observation_fhir = self.generateur.generer_observation_pression_arterielle(
                    patient=patient,
                    praticien_id=praticien_id,
                    praticien_nom=praticien_nom,
                    horodatage=horodatage,
                )

                # Extraire PA
                systolique = None
                diastolique = None
                for comp in observation_fhir.get("component", []):
                    code = comp.get("code", {}).get("coding", [{}])[0].get("code")
                    if code == "8480-6":
                        systolique = comp.get("valueQuantity", {}).get("value")
                    elif code == "8462-4":
                        diastolique = comp.get("valueQuantity", {}).get("value")

                if systolique and diastolique:
                    # Détection crise
                    self.detecter_et_gerer_crise(patient, int(systolique), int(diastolique), observation_fhir)

                    # Envoyer
                    self.envoyer_json(SUJET_OBSERVATIONS_BRUTES, cle=patient.id_patient, charge=observation_fhir)
                    
                    with self.stats_verrou:
                        self.stats["observations_envoyees"] += 1

                #  Délai adaptatif réaliste
                delai = random.uniform(DELAI_MIN, DELAI_MAX)
                time.sleep(delai)

            except Exception as e:
                if self.actif:
                    self._log("ERROR", f"(▀̿Ĺ̯▀̿ ̿) Erreur thread {patient.id_patient}: {e}")
                    time.sleep(5)

    def demarrer(self) -> None:
        """
        Démarre production avec ThreadPoolExecutor.
        
         AMÉLIORATION : Graceful shutdown proper
        """
        self._log("INFO", "( •̀ ω •́ )✧Démarrage producteur FHIR")
        
        # Publier patients + praticiens
        self.publier_patients()
        self.publier_praticiens()
        
        self._log("INFO", f"ヾ(⌐■_■)ノ♪ Lancement {len(self.patients)} threads patients...")

        #  ThreadPoolExecutor
        futures = []
        for patient in self.patients:
            future = self.executor.submit(self._thread_patient, patient)
            futures.append(future)

        self._log("INFO", " Producteur actif (Ctrl+C pour arrêter)")

        # Thread monitoring
        try:
            while self.actif:
                time.sleep(10)
                self._envoyer_metriques()
        except KeyboardInterrupt:
            self._log("INFO", " Signal arrêt reçu")
            self.arreter()

    def _envoyer_metriques(self) -> None:
        """Envoie métriques périodiques."""
        with self.stats_verrou:
            temps_ecoule = max(1.0, time.time() - self.stats["ts_debut"])
            charge = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "observations_envoyees": self.stats["observations_envoyees"],
                "crises_declenchees": self.stats["crises_declenchees"],
                "crises_terminees": self.stats["crises_terminees"],
                "debit_msg_par_sec": round(self.stats["observations_envoyees"] / temps_ecoule, 3),
            }

        self.envoyer_json(SUJET_METRIQUES, cle="producteur", charge=charge)
        self._log("INFO", f" obs={charge['observations_envoyees']} "
                          f"crises_declenchees={charge['crises_declenchees']} "
                          f"crises_terminees={charge['crises_terminees']} "
                          f"debit={charge['debit_msg_par_sec']}/s")

    def arreter(self) -> None:
        """
        
        
    
        """
        if not self.actif:
            return
        self.actif = False

        self._log("INFO", " Arrêt des threads...")
        self.executor.shutdown(wait=True, cancel_futures=False)

        self._log("INFO", " Flush producteur Kafka...") # vidage de la file d'attente
        self.producteur.flush(5)

        self._log("INFO", "Sauvegarde états crises Redis...")
        with self.etats_verrou:
            for patient_id, etat in self.etats_crises.items():
                self.redis.sauvegarder_etat(patient_id, etat)

        self._afficher_stats_finales()

    def _afficher_stats_finales(self) -> None:
        """Stats finales."""
        with self.stats_verrou:
            temps_ecoule = max(1.0, time.time() - self.stats["ts_debut"])
            
            self._log("INFO", "############### STATS FINALES ###############")
            self._log("INFO", f"Durée : {int(temps_ecoule)}s")
            self._log("INFO", f"observations_envoyees={self.stats['observations_envoyees']}")
            self._log("INFO", f"crises_declenchees={self.stats['crises_declenchees']}")
            self._log("INFO", f"crises_terminees={self.stats['crises_terminees']}")
            self._log("INFO", f"débit={self.stats['observations_envoyees']/temps_ecoule:.2f} msg/s")
            self._log("INFO", "#############################################")



########################################################################################
# MAIN + GESTIONNAIRE SIGNAL
########################################################################################


def principal() -> None:
    """Point d'entrée principal."""
    producteur = ProducteurFHIR()

    def _gestionnaire_sigint(signum, frame) -> None:
        producteur.arreter()

    signal.signal(signal.SIGINT, _gestionnaire_sigint)
    producteur.demarrer()



if __name__ == "__main__":
    principal()
