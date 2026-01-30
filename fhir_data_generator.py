"""
FHIR Data Generator — Blood Pressure (FHIR R4)
But :
- Générer des patients et practitioners
- Simuler des Observations FHIR BP réalistes (SYS/DIA corrélés, circadien, stress, événements rares)
- Ajouter des extensions minimales utiles au consumer (Option B)

FHIR BP (structure standard) :
- Observation.code : LOINC 85354-9 (Blood pressure panel)
- Observation.component :
  - 8480-6 (Systolic blood pressure)
  - 8462-4 (Diastolic blood pressure)
- valueQuantity.unit : mm[Hg] ; code UCUM : mm[Hg]
"""

from __future__ import annotations

import uuid
import random
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Tuple, Optional

import numpy as np
from faker import Faker

from fhir.resources.observation import Observation, ObservationComponent
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
from fhir.resources.quantity import Quantity
from fhir.resources.reference import Reference
from fhir.resources.extension import Extension


# =========================
# 0) RNG / Faker (reproductible)
# =========================

fake = Faker("fr_FR")
SEED = 12345
random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)


# =========================
# 1) Constantes FHIR utiles
# =========================

LOINC_SYSTEM = "http://loinc.org"
UCUM_SYSTEM = "http://unitsofmeasure.org"
UCUM_MMHG = "mm[Hg]"

LOINC_BP_PANEL = "85354-9"
LOINC_SYS = "8480-6"
LOINC_DIA = "8462-4"

FHIR_EXT_BASE = "http://example.org/fhir/StructureDefinition"


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# =========================
# 2) Modèle patient réaliste
# =========================

@dataclass
class PatientProfile:
    # Identité
    patient_id: str
    first_name: str
    last_name: str
    gender_fhir: str              # "male"/"female" (FHIR)
    gender_short: str             # "M"/"F" (pratique pour consumer)
    age: int
    age_group: str               # "jeune" / "adulte" / "senior"

    # Paramètres physiologiques (repos)
    mean_sys: float
    mean_dia: float
    sd_sys: float
    sd_dia: float
    corr: float = 0.6            # corrélation SYS/DIA

    # Dynamics
    drift_per_hour_sys: float = 0.0
    drift_per_hour_dia: float = 0.0

    # Événements rares
    crisis_prob: float = 0.01        # crise hypertensive
    hypo_prob: float = 0.005         # hypotension

    # État interne (évolue)
    baseline_sys: float = field(init=False)
    baseline_dia: float = field(init=False)

    def __post_init__(self) -> None:
        self.baseline_sys = float(self.mean_sys)
        self.baseline_dia = float(self.mean_dia)

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"

    def simulate_stress_level(self) -> int:
        """
        Stress 1–10 (distribution réaliste : majorité 3–6, parfois haut).
        """
        r = random.random()
        if r < 0.10:
            return random.randint(1, 2)
        if r < 0.80:
            return random.randint(3, 6)
        if r < 0.95:
            return random.randint(7, 8)
        return random.randint(9, 10)

    def circadian_offset(self, ts: datetime) -> Tuple[float, float]:
        """
        Rythme circadien simple :
        - pression un peu plus haute en journée, plus basse la nuit.
        """
        h = ts.astimezone(timezone.utc).hour + ts.minute / 60.0
        # sinus centré (min ~ 3–5h, max ~ 15–17h)
        phase = 2 * math.pi * (h - 4) / 24.0
        sys_amp = 6.0 if self.age_group != "jeune" else 4.0
        dia_amp = 3.0 if self.age_group != "jeune" else 2.0
        return sys_amp * math.sin(phase), dia_amp * math.sin(phase)

    def stress_offset(self, stress_level: int) -> Tuple[float, float]:
        """
        Stress → hausse SYS/DIA (plus marqué sur SYS).
        """
        lvl = _clamp(float(stress_level), 1, 10)
        if lvl <= 2:
            base = 0.0
        elif lvl <= 5:
            base = (lvl - 2) * 2.0       # 2..6
        else:
            base = 6.0 + (lvl - 5) * 3.0 # 9..21

        sys = base * float(np.random.normal(1.0, 0.12))
        dia = (base * 0.45) * float(np.random.normal(1.0, 0.12))
        return sys, dia

    def correlated_noise(self) -> Tuple[float, float]:
        """
        Bruit SYS/DIA corrélé (bivarié normal).
        """
        cov = [
            [self.sd_sys ** 2, self.corr * self.sd_sys * self.sd_dia],
            [self.corr * self.sd_sys * self.sd_dia, self.sd_dia ** 2],
        ]
        n = np.random.multivariate_normal([0.0, 0.0], cov)
        return float(n[0]), float(n[1])

    def evolve_baseline(self) -> None:
        """
        Drift lent (sur longue durée). Ici on fait un petit random walk pour le réalisme.
        """
        self.baseline_sys += float(np.random.normal(self.drift_per_hour_sys, 0.02))
        self.baseline_dia += float(np.random.normal(self.drift_per_hour_dia, 0.02))

    def simulate_bp(self, ts: datetime) -> Tuple[int, int, int, bool, bool]:
        """
        Retour :
        (sys, dia, stress_level, is_crisis, is_hypo)
        """
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        # 1) état lent
        self.evolve_baseline()

        # 2) stress
        stress_level = self.simulate_stress_level()
        sys_stress, dia_stress = self.stress_offset(stress_level)

        # 3) circadien
        sys_circ, dia_circ = self.circadian_offset(ts)

        # 4) bruit corrélé
        sys_noise, dia_noise = self.correlated_noise()

        # 5) événements rares
        is_crisis = random.random() < self.crisis_prob
        is_hypo = (not is_crisis) and (random.random() < self.hypo_prob)

        sys = self.baseline_sys + sys_circ + sys_stress + sys_noise
        dia = self.baseline_dia + dia_circ + dia_stress + dia_noise

        if is_crisis:
            sys += random.uniform(35, 60)
            dia += random.uniform(20, 40)

        if is_hypo:
            sys -= random.uniform(20, 35)
            dia -= random.uniform(10, 20)

        # garde-fous physiologiques
        sys = _clamp(sys, 70, 240)
        dia = _clamp(dia, 40, 140)
        if dia >= sys - 15:
            dia = sys - 15

        return int(round(sys)), int(round(dia)), int(stress_level), bool(is_crisis), bool(is_hypo)


# =========================
# 3) Generator principal
# =========================

class FHIRDataGenerator:
    """
    API utilisée par tes producers :
    - generate_practitioners()
    - generate_patients()
    - assign_practitioners_to_patients()
    - generate_blood_pressure_observation(...)
    """

    def __init__(self) -> None:
        self.patients: List[PatientProfile] = []
        self.practitioners: List[Dict[str, str]] = []

    # ---------- practitioners

    def generate_practitioners(self) -> List[Dict[str, str]]:
        self.practitioners = [
            {"id": "practitioner-001", "name": "Dr Jean-Pierre Martin", "specialty": "cardiologue"},
            {"id": "practitioner-002", "name": "Dr Catherine Dubois", "specialty": "cardiologue"},
        ]
        return self.practitioners

    # ---------- patients

    def _mk_patient(self, age: int, gender_fhir: str) -> PatientProfile:
        first = fake.first_name_male() if gender_fhir == "male" else fake.first_name_female()
        last = fake.last_name()
        pid = f"patient-{uuid.uuid4().hex[:8]}"

        gender_short = "M" if gender_fhir == "male" else "F"
        if age < 18:
            age_group = "jeune"
        elif age < 65:
            age_group = "adulte"
        else:
            age_group = "senior"

        # paramètres par groupe d'âge
        if age_group == "jeune":
            mean_sys, mean_dia = random.uniform(95, 110), random.uniform(55, 70)
            sd_sys, sd_dia = 5.5, 4.0
            crisis_prob, hypo_prob = 0.002, 0.010
            drift_sys, drift_dia = 0.00, 0.00
        elif age_group == "adulte":
            # mix réaliste (normaux / élevés / HTA)
            r = random.random()
            if r < 0.55:
                mean_sys, mean_dia = random.uniform(110, 123), random.uniform(70, 80)
                crisis_prob = 0.003
            elif r < 0.80:
                mean_sys, mean_dia = random.uniform(124, 135), random.uniform(78, 86)
                crisis_prob = 0.012
            else:
                mean_sys, mean_dia = random.uniform(136, 155), random.uniform(85, 95)
                crisis_prob = 0.030
            sd_sys, sd_dia = 8.0, 6.0
            hypo_prob = 0.004
            drift_sys, drift_dia = 0.01, 0.006
        else:  # senior
            r = random.random()
            if r < 0.40:
                mean_sys, mean_dia = random.uniform(120, 135), random.uniform(72, 82)
                crisis_prob = 0.015
            elif r < 0.70:
                mean_sys, mean_dia = random.uniform(136, 150), random.uniform(80, 90)
                crisis_prob = 0.030
            else:
                mean_sys, mean_dia = random.uniform(151, 170), random.uniform(88, 100)
                crisis_prob = 0.050
            sd_sys, sd_dia = 10.0, 7.0
            hypo_prob = 0.006
            drift_sys, drift_dia = 0.015, 0.010

        return PatientProfile(
            patient_id=pid,
            first_name=first,
            last_name=last,
            gender_fhir=gender_fhir,
            gender_short=gender_short,
            age=age,
            age_group=age_group,
            mean_sys=mean_sys,
            mean_dia=mean_dia,
            sd_sys=sd_sys,
            sd_dia=sd_dia,
            corr=0.6,
            drift_per_hour_sys=drift_sys,
            drift_per_hour_dia=drift_dia,
            crisis_prob=crisis_prob,
            hypo_prob=hypo_prob,
        )

    def generate_patients(self) -> List[PatientProfile]:
        """Distribution avec âges et sexes fixes :
        - 2 jeunes (5-12)
        - 4 adultes (30-55)
        - 2 seniors (65-80)"""
        # Définir les patients avec âge et sexe fixes
        patients_config = [
            # Jeunes
            {"age": 8, "gender": "male"},
            {"age": 11, "gender": "female"},
            
            # Adultes
            {"age": 32, "gender": "male"},
            {"age": 38, "gender": "female"},
            {"age": 45, "gender": "male"},
            {"age": 52, "gender": "female"},
            
            # Seniors
            {"age": 68, "gender": "male"},
            {"age": 75, "gender": "female"},
        ]
        
        self.patients = []
        for config in patients_config:
            self.patients.append(self._mk_patient(age=config["age"], gender_fhir=config["gender"]))
        
        return self.patients


    def assign_practitioners_to_patients(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Simple, réaliste, sans sur-ingénierie :
        - jeunes : plutôt pract 1
        - adultes : 50/50
        - seniors : parfois double suivi
        """
        if not self.practitioners:
            self.generate_practitioners()

        assignments: Dict[str, List[Dict[str, str]]] = {}
        for p in self.patients:
            if p.age_group == "jeune":
                assignments[p.patient_id] = [self.practitioners[0]]
            elif p.age_group == "adulte":
                assignments[p.patient_id] = [random.choice(self.practitioners)]
            else:
                assignments[p.patient_id] = self.practitioners[:] if random.random() < 0.5 else [self.practitioners[1]]
        return assignments

    # ---------- observation

    def _categorize_bp(self, sys: int, dia: int) -> Tuple[str, str, bool, float]:
        """
        Catégorisation d'hypertension selon AHA 2017.
        - interpretation codes simplifiés : N/A/H/HH
        """
        if sys >= 180 or dia >= 120:
            return "HYPERTENSIVE_CRISIS", "HH", True, 0.95
        if sys >= 140 or dia >= 90:
            return "HYPERTENSION_STAGE_2", "H", True, 0.75
        if sys >= 130 or dia >= 80:
            return "HYPERTENSION_STAGE_1", "H", True, 0.50
        if sys >= 120:
            return "ELEVATED", "A", True, 0.25
        return "NORMAL", "N", False, 0.05

    def generate_blood_pressure_observation(
        self,
        patient_profile: PatientProfile,
        practitioner_id: str,
        practitioner_name: str,
        timestamp: Optional[datetime] = None,
    ) -> Observation:
        """
        Génère une Observation BP FHIR R4.

        """
        ts = timestamp or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        sys, dia, stress_level, _, _ = patient_profile.simulate_bp(ts)
        category, interp, anomaly, risk = self._categorize_bp(sys, dia)

        obs_id = f"bp-obs-{uuid.uuid4().hex[:12]}"

        # ---- FHIR code panel
        panel_code = CodeableConcept(
            coding=[Coding(system=LOINC_SYSTEM, code=LOINC_BP_PANEL, display="Blood pressure panel")],
            text="Blood pressure systolic & diastolic",
        )

        subject = Reference(reference=f"Patient/{patient_profile.patient_id}", display=patient_profile.full_name)
        performer = Reference(reference=f"Practitioner/{practitioner_id}", display=practitioner_name)

        # ---- Components
        def _component(loinc_code: str, display: str, value: int) -> ObservationComponent:
            return ObservationComponent(
                code=CodeableConcept(coding=[Coding(system=LOINC_SYSTEM, code=loinc_code, display=display)]),
                valueQuantity=Quantity(
                    value=value,
                    unit="mm[Hg]", # milimètre de mercure (unité de pression artérielle)
                    system=UCUM_SYSTEM,# UCUM = Unified Code for Units of Measure: il s'agit d'un standard pour les unités de mesure utilisées dans les données de santé.
                    code=UCUM_MMHG,
                ),
            )

        comp_sys = _component(LOINC_SYS, "Systolic blood pressure", sys)
        comp_dia = _component(LOINC_DIA, "Diastolic blood pressure", dia)

        # ---- Extensions (consumer-friendly URLs : contient les mots-clés)
        ext_anomaly = Extension(url=f"{FHIR_EXT_BASE}/anomaly-detected", valueBoolean=anomaly)
        ext_risk = Extension(url=f"{FHIR_EXT_BASE}/risk-score", valueDecimal=Decimal(str(risk)))
        ext_category = Extension(url=f"{FHIR_EXT_BASE}/blood-pressure-category", valueString=category)

        ext_age = Extension(url=f"{FHIR_EXT_BASE}/patient-age", valueInteger=int(patient_profile.age))
        ext_age_group = Extension(url=f"{FHIR_EXT_BASE}/patient-age_group", valueString=patient_profile.age_group)
        ext_gender = Extension(url=f"{FHIR_EXT_BASE}/patient-gender", valueString=patient_profile.gender_short)
        ext_first = Extension(url=f"{FHIR_EXT_BASE}/patient-first_name", valueString=patient_profile.first_name)
        ext_last = Extension(url=f"{FHIR_EXT_BASE}/patient-last_name", valueString=patient_profile.last_name)
        ext_stress = Extension(url=f"{FHIR_EXT_BASE}/patient-stress_level", valueInteger=int(stress_level))

        # ---- Observation
        obs = Observation(
            id=obs_id,
            status="final",
            category=[CodeableConcept(
                coding=[Coding(system="http://terminology.hl7.org/CodeSystem/observation-category",
                               code="vital-signs",
                               display="Vital Signs")]
            )],
            code=panel_code,
            subject=subject,
            effectiveDateTime=_iso(ts),
            issued=_iso(ts),
            performer=[performer],
            component=[comp_sys, comp_dia],
            extension=[
                ext_anomaly,
                ext_risk,
                ext_category,   # <= garde l'index 2 comme dans ton producer actuel
                ext_age,
                ext_age_group,
                ext_gender,
                ext_first,
                ext_last,
                ext_stress,
            ],
        )
        return obs


# =========================
# 4) Mini test local
# =========================

if __name__ == "__main__":
    print(" Test generator BP\n")

    gen = FHIRDataGenerator()
    gen.generate_practitioners()
    patients = gen.generate_patients()
    assignments = gen.assign_practitioners_to_patients()

    # 3 patients, 2 mesures chacun
    for p in patients[:3]:
        pract = assignments[p.patient_id][0]
        for _ in range(2):
            obs = gen.generate_blood_pressure_observation(p, pract["id"], pract["name"])
            sys = obs.component[0].valueQuantity.value
            dia = obs.component[1].valueQuantity.value
            cat = obs.extension[2].valueString  # compat producer actuel
            stress = obs.extension[-1].valueInteger
            print(f"- {p.full_name:20s} ({p.age_group}) {sys}/{dia} mm[Hg]  cat={cat} stress={stress}")
