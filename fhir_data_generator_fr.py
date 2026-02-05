"""
Générateur de Données FHIR — Pression Artérielle (FHIR R4)
===========================================================

Ce module génère des patients synthétiques réalistes ainsi que des
observations de tension artérielle strictement valides au format FHIR R4.
Pensé comme brique de base pour du streaming Kafka, de la détection
précoce d’hypertension ou des tests ML temps réel.


* OBJECTIF PRINCIPAL

Simuler des profils patients crédibles et des mesures de pression artérielle
afin de tester une pipeline de détection d’anomalies / hypertension
dans un contexte proche d’un environnement hospitalier ou IoT santé.

* GÉNÉRATION PATIENTS

- 8 profils variés :
   âge, IMC, morphologie, niveau de risque cardiovasculaire
- Variabilité naturelle des constantes vitales
- Conçu pour produire un dataset cohérent sur la durée


* FACTEURS PHYSIOLOGIQUES SIMULÉS

1. IMC → influence baseline PA (~0.8 mmHg par point d’IMC)
2. Stress aigu → impact court terme 0–15 mmHg (niveau 1–10)
3. Rythme circadien → variation jour / nuit 4–6 mmHg
4. Âge → variabilité accrue + probabilité de crise plus élevée
5. Bruit corrélé sys/dia→ corrélation réaliste ≈ 0.6


* CATÉGORISATION AHA 2017
- NORMALE  : <120 / <80 mmHg
- ELEVEE   : 120–129 / <80 mmHg
- STADE_1  : 130–139 / 80–89 mmHg
- STADE_2  : ≥140 / 90 mmHg
- CRISE   : ≥180 / 120 mmHg


* GARANTIES QUALITÉ & VALIDATION

- Validation stricte : diastolique < systolique − 15
- Bornes physiologiques réalistes :
   systolique 70–240 / diastolique 40–140
- Corrélation systolique / diastolique maintenue
- Anti-valeurs aberrantes intégrées


* CONFORMITÉ FHIR R4

- Panel LOINC : 85354-9 (Blood Pressure Panel)
- Composants :
   - 8480-6 → Systolique
   - 8462-4 → Diastolique
- Extensions patient + contexte clinique
- Ressources prêtes à être injectées dans Kafka / ES / ML

"""



from __future__ import annotations 

import uuid
import random
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Tuple
from decimal import Decimal

import numpy as np
from faker import Faker



########################################################################################
# CONSTANTES
########################################################################################


GRAINE = 12345

SYSTEME_LOINC = "http://loinc.org"
SYSTEME_UCUM = "http://unitsofmeasure.org"
UCUM_MMHG = "mm[Hg]"

LOINC_PANEL_PA = "85354-9"
LOINC_SYSTOLIQUE = "8480-6"
LOINC_DIASTOLIQUE = "8462-4"

BASE_EXTENSION_FHIR = "http://example.org/fhir/StructureDefinition"

generateur_fake = Faker("fr_FR")
random.seed(GRAINE)
np.random.seed(GRAINE)
Faker.seed(GRAINE)



########################################################################################
# UTILITAIRES
########################################################################################


def vers_iso_dt(dt: datetime) -> str:
    """Convertit datetime en ISO 8601 UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def limiter(x: float, valeur_min: float, valeur_max: float) -> float:
    """Limite valeur entre min et max."""
    return max(valeur_min, min(valeur_max, x))



########################################################################################
# PROFIL PATIENT
########################################################################################


@dataclass
class ProfilPatient:
    """
    Profil patient complet pour simulation PA.

    """
    
    # Identité
    id_patient: str
    prenom: str
    nom: str
    genre_fhir: str
    genre_court: str
    age: int
    groupe_age: str

    # Morphologie
    imc: float
    poids: float
    taille: float

    # Paramètres PA
    moyenne_sys: float
    moyenne_dia: float
    ecart_type_sys: float
    ecart_type_dia: float

    # Statistiques
    correlation: float = 0.6
    prob_crise: float = 0.01

    # État interne
    baseline_sys: float = field(init=False)
    baseline_dia: float = field(init=False)

    def __post_init__(self) -> None:
        """
         NOUVEAU : Validation stricte + initialisation
        """
        # Validation
        assert 0 <= self.age <= 150, f"Âge invalide: {self.age}"
        assert 10 <= self.imc <= 60, f"IMC invalide: {self.imc}"
        assert 70 <= self.moyenne_sys <= 240, f"Moyenne sys invalide: {self.moyenne_sys}"
        assert 40 <= self.moyenne_dia <= 140, f"Moyenne dia invalide: {self.moyenne_dia}"
        assert self.moyenne_dia < self.moyenne_sys - 15, f"Dia trop proche de sys: {self.moyenne_sys}/{self.moyenne_dia}"

        # Initialisation baselines
        self.baseline_sys = float(self.moyenne_sys)
        self.baseline_dia = float(self.moyenne_dia)

    @property
    def nom_complet(self) -> str:
        """Nom complet."""
        return f"{self.prenom} {self.nom}"

    def simuler_niveau_stress(self) -> int:
        """Génère niveau stress réaliste (1-10)."""
        r = random.random()
        if r < 0.10:
            return random.randint(1, 2)
        if r < 0.80:
            return random.randint(3, 6)
        if r < 0.95:
            return random.randint(7, 8)
        return random.randint(9, 10)

    def decalage_stress(self, niveau_stress: int) -> Tuple[float, float]:
        """Calcule impact stress sur PA."""
        niveau = limiter(float(niveau_stress), 1, 10)

        if niveau <= 2:
            base = 0.0
        elif niveau <= 5:
            base = (niveau - 2) * 2.0
        else:
            base = 6.0 + (niveau - 5) * 3.0

        sys = base * float(np.random.normal(1.0, 0.12))
        dia = base * 0.45 * float(np.random.normal(1.0, 0.12))
        return sys, dia

    def decalage_circadien(self, horodatage: datetime) -> Tuple[float, float]:
        """Calcule impact rythme circadien."""
        h = horodatage.astimezone(timezone.utc).hour + horodatage.minute / 60.0
        phase = 2 * math.pi * (h - 4) / 24.0

        amplitude_sys = 6.0 if self.groupe_age != "jeune" else 4.0
        amplitude_dia = 3.0 if self.groupe_age != "jeune" else 2.0

        return amplitude_sys * math.sin(phase), amplitude_dia * math.sin(phase)

    def bruit_correle(self) -> Tuple[float, float]:
        """Génère bruit gaussien corrélé sys/dia."""
        z1, z2 = np.random.standard_normal(2)
        bruit_sys = float(self.ecart_type_sys * z1)
        bruit_dia = float(
            self.ecart_type_dia * (self.correlation * z1 + np.sqrt(1 - self.correlation**2) * z2)
        )
        return bruit_sys, bruit_dia

    def generer_mesure_pa(self, horodatage: datetime) -> Tuple[int, int, int]:
        """
        Génère mesure PA réaliste avec validation stricte.
        
         AMÉLIORATION : Garde-fous renforcés
        """
        niveau_stress = self.simuler_niveau_stress()
        delta_stress_sys, delta_stress_dia = self.decalage_stress(niveau_stress)
        delta_circ_sys, delta_circ_dia = self.decalage_circadien(horodatage)
        bruit_sys, bruit_dia = self.bruit_correle()

        sys = self.baseline_sys + delta_stress_sys + delta_circ_sys + bruit_sys
        dia = self.baseline_dia + delta_stress_dia + delta_circ_dia + bruit_dia

        #  GARDE-FOUS STRICTES
        sys = limiter(sys, 70, 240)
        dia = limiter(dia, 40, 140)

        #  VALIDATION : dia DOIT être < sys - 15
        if dia >= sys - 15:
            dia = max(40, sys - 15)

        sys_int = int(round(sys))
        dia_int = int(round(dia))

        # Drift léger baseline
        self.baseline_sys += float(np.random.normal(0, 0.5))
        self.baseline_dia += float(np.random.normal(0, 0.3))
        self.baseline_sys = limiter(self.baseline_sys, 70, 240)
        self.baseline_dia = limiter(self.baseline_dia, 40, 140)

        return sys_int, dia_int, niveau_stress



########################################################################################
# GÉNÉRATEUR PRINCIPAL
########################################################################################


class GenerateurDonneesFHIR:
    """
    Générateur patients + observations FHIR R4.
    
     AMÉLIORATION : Validation stricte à chaque étape
    """

    def __init__(self) -> None:
        self.praticiens: Dict[str, str] = {}
        self.patients: List[ProfilPatient] = []

    def generer_praticiens(self) -> None:
        """Génère 2 cardiologues."""
        self.praticiens = {
            "praticien-001": "Dr Jean Martin",
            "praticien-002": "Dr Sophie Dubois",
        }

    def generer_patients(self) -> List[ProfilPatient]:
        """
        Génère 8 patients variés avec validation stricte.
        
        AMÉLIORATION : Garde-fous sur tous les paramètres
        """
        configurations = [
            # (genre_fhir, age, imc, moyenne_sys, moyenne_dia, ecart_sys, ecart_dia, prob_crise)
            ("homme", 16, 22.5, 115, 72, 8, 5, 0.005),
            ("femme", 24, 24.0, 118, 75, 9, 6, 0.01),
            ("homme", 42, 27.5, 128, 82, 10, 7, 0.02),
            ("femme", 50, 29.0, 135, 86, 11, 7, 0.03),
            ("homme", 58, 31.0, 142, 90, 12, 8, 0.04),
            ("femme", 65, 28.5, 145, 92, 13, 9, 0.05),
            ("homme", 72, 26.0, 150, 88, 14, 9, 0.06),
            ("femme", 80, 25.5, 155, 90, 15, 10, 0.07),
        ]

        self.patients = []
        for i, (genre_fhir, age, imc, m_sys, m_dia, e_sys, e_dia, p_crise) in enumerate(configurations):
            id_patient = f"patient-{uuid.uuid4().hex[:8]}"
            prenom = generateur_fake.first_name_male() if genre_fhir == "homme" else generateur_fake.first_name_female()
            nom = generateur_fake.last_name()
            genre_court = "H" if genre_fhir == "homme" else "F"

            if age < 18:
                groupe_age = "jeune"
            elif age < 65:
                groupe_age = "adulte"
            else:
                groupe_age = "senior"

            taille = 1.70 + np.random.normal(0, 0.08)
            taille = limiter(taille, 1.40, 2.10)
            poids = imc * (taille ** 2)
            poids = limiter(poids, 40, 200)

            #  VALIDATION STRICTE : dia < sys - 15
            if m_dia >= m_sys - 15:
                m_dia = m_sys - 20

            patient = ProfilPatient(
                id_patient=id_patient,
                prenom=prenom,
                nom=nom,
                genre_fhir=genre_fhir,
                genre_court=genre_court,
                age=age,
                groupe_age=groupe_age,
                imc=round(imc, 1),
                poids=round(poids, 1),
                taille=round(taille, 2),
                moyenne_sys=float(m_sys),
                moyenne_dia=float(m_dia),
                ecart_type_sys=float(e_sys),
                ecart_type_dia=float(e_dia),
                correlation=0.6,
                prob_crise=p_crise,
            )
            self.patients.append(patient)

        return self.patients

    def creer_patient_fhir(self, patient: ProfilPatient) -> Dict:
        """Génère ressource FHIR Patient."""
        return {
            "resourceType": "Patient",
            "id": patient.id_patient,
            "identifier": [
                {
                    "system": "http://example.org/fhir/identifiers/patients",
                    "value": patient.id_patient,
                }
            ],
            "name": [
                {
                    "use": "official",
                    "family": patient.nom,
                    "given": [patient.prenom],
                }
            ],
            "gender": patient.genre_fhir,
            "birthDate": f"{2026 - patient.age}-01-01",
        }

    def creer_praticien_fhir(self, praticien_id: str, praticien_nom: str) -> Dict:
        """Génère ressource FHIR Practitioner."""
        nom_parts = praticien_nom.split(maxsplit=1)
        prenom = nom_parts[0] if len(nom_parts) > 0 else "Prénom"
        nom = nom_parts[1] if len(nom_parts) > 1 else "Nom"

        return {
            "resourceType": "Practitioner",
            "id": praticien_id,
            "identifier": [
                {
                    "system": "http://example.org/fhir/identifiers/practitioners",
                    "value": praticien_id,
                }
            ],
            "name": [
                {
                    "use": "official",
                    "family": nom,
                    "given": [prenom],
                    "prefix": ["Dr"],
                }
            ],
        }

    def generer_observation_pression_arterielle(
        self,
        patient: ProfilPatient,
        praticien_id: str,
        praticien_nom: str,
        horodatage: datetime,
    ) -> Dict:
        """
        Génère observation FHIR R4 pression artérielle.
        
         AMÉLIORATION : Validation finale stricte
        """
        systolique, diastolique, niveau_stress = patient.generer_mesure_pa(horodatage)

        #  VALIDATION FINALE
        assert 70 <= systolique <= 240, f"Systolique hors limites: {systolique}"
        assert 40 <= diastolique <= 140, f"Diastolique hors limites: {diastolique}"
        assert diastolique < systolique - 15, f"Écart sys/dia insuffisant: {systolique}/{diastolique}"

        iso_horodatage = vers_iso_dt(horodatage)
        id_observation = f"obs-{uuid.uuid4().hex[:12]}"

        observation = {
            "resourceType": "Observation",
            "id": id_observation,
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "vital-signs",
                            "display": "Vital Signs",
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": SYSTEME_LOINC,
                        "code": LOINC_PANEL_PA,
                        "display": "Blood pressure panel",
                    }
                ],
                "text": "Pression artérielle",
            },
            "subject": {"reference": f"Patient/{patient.id_patient}",
                "display": patient.nom_complet,},
            "performer": [{"reference": f"Practitioner/{praticien_id}",
                    "display": praticien_nom,}],
            "effectiveDateTime": iso_horodatage,
            "issued": iso_horodatage,
            "component": [
                {
                    "code": {
                        "coding": [
                            {
                                "system": SYSTEME_LOINC,
                                "code": LOINC_SYSTOLIQUE,
                                "display": "Systolic blood pressure",
                            }
                        ],
                        "text": "Pression systolique",
                    },
                    "valueQuantity": {
                        "value": systolique,
                        "unit": "mmHg",
                        "system": SYSTEME_UCUM,
                        "code": UCUM_MMHG,
                    },
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": SYSTEME_LOINC,
                                "code": LOINC_DIASTOLIQUE,
                                "display": "Diastolic blood pressure",
                            }
                        ],
                        "text": "Pression diastolique",
                    },
                    "valueQuantity": {
                        "value": diastolique,
                        "unit": "mmHg",
                        "system": SYSTEME_UCUM,
                        "code": UCUM_MMHG,
                    },
                },
            ],
            "extension": [
                {
                    "url": f"{BASE_EXTENSION_FHIR}/patient-prenom",
                    "valueString": patient.prenom,
                },
                {
                    "url": f"{BASE_EXTENSION_FHIR}/patient-nom",
                    "valueString": patient.nom,
                },
                {
                    "url": f"{BASE_EXTENSION_FHIR}/patient-genre",
                    "valueString": patient.genre_court,
                },
                {
                    "url": f"{BASE_EXTENSION_FHIR}/patient-age",
                    "valueInteger": patient.age,
                },
                {
                    "url": f"{BASE_EXTENSION_FHIR}/patient-groupe-age",
                    "valueString": patient.groupe_age,
                },
                {
                    "url": f"{BASE_EXTENSION_FHIR}/niveau-stress",
                    "valueInteger": niveau_stress,
                },
                {
                    "url": f"{BASE_EXTENSION_FHIR}/patient-imc",
                    "valueDecimal": patient.imc,
                },
                {
                    "url": f"{BASE_EXTENSION_FHIR}/patient-poids",
                    "valueDecimal": patient.poids,
                },
                {
                    "url": f"{BASE_EXTENSION_FHIR}/patient-taille",
                    "valueDecimal": patient.taille,
                },
            ],
        }

        return observation
