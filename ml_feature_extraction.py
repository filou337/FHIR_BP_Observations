"""
ML Training Pipeline - XGBoost Blood Pressure Classification
============================================================
Entraîne un modèle de classification multiclasse pour la pression artérielle.

Task: 5-class classification (NORMAL → ELEVATED → STAGE_1 → STAGE_2 → CRISIS)
Features: [systolic, diastolic, age, gender, trend, risk_score, hour_of_day]

Usage:
    python ml_training.py [csv_file]
"""

import json
import logging
from pathlib import Path
from typing import Dict, Tuple

import joblib
import numpy as np
import pandas as pd
from colorlog import ColoredFormatter
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report,
    label_binarize
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

# ============================================================================
# CONFIG
# ============================================================================

CLASS_NAMES = {
    0: "NORMAL",
    1: "ELEVATED",
    2: "HYPERTENSION_STAGE_1",
    3: "HYPERTENSION_STAGE_2",
    4: "HYPERTENSIVE_CRISIS",
}

FEATURE_COLS = ["systolic", "diastolic", "age", "gender", "trend", "risk_score", "hour_of_day"]
TARGET_COL = "blood_pressure_category"

HYPERPARAMS = {
    "n_estimators": 200,
    "max_depth": 7,
    "learning_rate": 0.2,
    "objective": "multi:softmax",
    "num_class": 5,
    "random_state": 42,
    "eval_metric": "mlogloss",
    "tree_method": "hist",
}

# ============================================================================
# LOGGING
# ============================================================================

def setup_logger():
    """Configure colored logging."""
    formatter = ColoredFormatter(
        "%(log_color)s[%(levelname)s]%(reset)s %(message)s",
        log_colors={"INFO": "green", "WARNING": "yellow", "ERROR": "red"},
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

logger = setup_logger()

# ============================================================================
# MODEL TRAINER
# ============================================================================

class MLTrainer:
    """Train and evaluate XGBoost classifier for blood pressure classification."""

    def __init__(self, output_dir: str = "ml_models"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.model = None
        self.scaler = None
        self.X_train = self.X_test = None
        self.y_train = self.y_test = None
        self.X_train_scaled = self.X_test_scaled = None
        self.metrics = {}

    def load_and_prepare(self, csv_file: str, test_size: float = 0.2) -> Tuple[np.ndarray, np.ndarray]:
        """Load CSV, validate columns, and split data."""
        logger.info("=" * 70)
        logger.info("LOADING AND PREPARING DATA")
        logger.info("=" * 70)

        # Load
        logger.info(f"Loading {csv_file}...")
        df = pd.read_csv(csv_file)
        logger.info(f"✓ Loaded: {len(df)} rows × {len(df.columns)} cols")

        # Validate columns
        required = set(FEATURE_COLS + [TARGET_COL])
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        logger.info(f"✓ All required columns present")

        # Split
        X = df[FEATURE_COLS].copy()
        y = df[TARGET_COL].copy()

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y,
            test_size=test_size,
            random_state=42,
            stratify=y
        )

        logger.info(f"✓ Train: {len(self.X_train)} ({len(self.X_train)/len(df)*100:.1f}%)")
        logger.info(f"✓ Test: {len(self.X_test)} ({len(self.X_test)/len(df)*100:.1f}%)")

        # Scale
        self.scaler = StandardScaler()
        self.X_train_scaled = self.scaler.fit_transform(self.X_train)
        self.X_test_scaled = self.scaler.transform(self.X_test)
        logger.info("✓ Scaling completed")

        # Class distribution
        logger.info("Class distribution (train):")
        for cat_id, count in self.y_train.value_counts().sort_index().items():
            pct = count / len(self.y_train) * 100
            logger.info(f"  {CLASS_NAMES[cat_id]:25s}: {count:6d} ({pct:5.1f}%)")

        return self.X_train_scaled, self.y_train

    def train(self):
        """Train XGBoost model."""
        logger.info("\n" + "=" * 70)
        logger.info("TRAINING XGBOOST")
        logger.info("=" * 70)

        logger.info(f"Hyperparameters: {HYPERPARAMS}")

        self.model = XGBClassifier(**HYPERPARAMS, verbosity=0)

        logger.info("Training...")
        self.model.fit(
            self.X_train_scaled,
            self.y_train,
            eval_set=[(self.X_test_scaled, self.y_test)],
            verbose=False,
        )
        logger.info("✓ Training completed")

    def evaluate(self) -> Dict:
        """Evaluate model and return metrics."""
        logger.info("\n" + "=" * 70)
        logger.info("EVALUATION")
        logger.info("=" * 70)

        y_pred = self.model.predict(self.X_test_scaled)
        y_pred_proba = self.model.predict_proba(self.X_test_scaled)

        # Global metrics
        acc = accuracy_score(self.y_test, y_pred)
        prec = precision_score(self.y_test, y_pred, average="macro", zero_division=0)
        rec = recall_score(self.y_test, y_pred, average="macro", zero_division=0)
        f1 = f1_score(self.y_test, y_pred, average="macro", zero_division=0)

        logger.info("Global metrics:")
        logger.info(f"  Accuracy:  {acc:.4f}")
        logger.info(f"  Precision: {prec:.4f}")
        logger.info(f"  Recall:    {rec:.4f}")
        logger.info(f"  F1-Score:  {f1:.4f}")

        # Classification report
        logger.info("\nPer-class report:")
        logger.info("-" * 70)
        report = classification_report(
            self.y_test, y_pred,
            target_names=[CLASS_NAMES[i] for i in range(5)],
            digits=4
        )
        logger.info(report)

        # Confusion matrix
        cm = confusion_matrix(self.y_test, y_pred)
        logger.info("Confusion matrix:")
        logger.info("-" * 70)
        for i in range(5):
            row = " ".join(f"{cm[i, j]:6d}" for j in range(5))
            logger.info(f"  {CLASS_NAMES[i][:15]:15s} | {row}")

        # ROC-AUC
        roc_auc = None
        try:
            y_test_bin = label_binarize(self.y_test, classes=range(5))
            auc_scores = []
            for i in range(5):
                try:
                    score = roc_auc_score(y_test_bin[:, i], y_pred_proba[:, i])
                    auc_scores.append(score)
                    logger.info(f"  ROC-AUC {CLASS_NAMES[i]:25s}: {score:.4f}")
                except:
                    pass
            roc_auc = np.mean([s for s in auc_scores if not np.isnan(s)])
            logger.info(f"  ROC-AUC (macro):                 {roc_auc:.4f}")
        except Exception as e:
            logger.warning(f"ROC-AUC calculation failed: {e}")

        # Feature importance
        logger.info("\nTop 5 features:")
        logger.info("-" * 70)
        importance = self.model.feature_importances_
        sorted_idx = np.argsort(importance)[::-1]

        for rank, idx in enumerate(sorted_idx[:5], 1):
            feat_name = FEATURE_COLS[idx]
            imp = importance[idx]
            bar = "█" * int(imp * 50)
            logger.info(f"  {rank}. {feat_name:20s}: {imp:.4f} {bar}")

        # Store metrics
        self.metrics = {
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1": f1,
            "roc_auc": roc_auc or acc,
            "confusion_matrix": cm.tolist(),
            "feature_importance": dict(zip(FEATURE_COLS, importance)),
        }

        return self.metrics

    def save(self, name: str = "blood_pressure_classifier"):
        """Save model, scaler, and metadata."""
        logger.info("\n" + "=" * 70)
        logger.info("SAVING MODEL")
        logger.info("=" * 70)

        model_file = self.output_dir / f"{name}.pkl"
        scaler_file = self.output_dir / f"{name}_scaler.pkl"
        meta_file = self.output_dir / f"{name}_metadata.json"

        joblib.dump(self.model, model_file)
        logger.info(f"✓ Model: {model_file}")

        joblib.dump(self.scaler, scaler_file)
        logger.info(f"✓ Scaler: {scaler_file}")

        metadata = {
            "name": name,
            "version": "1.0.0",
            "features": FEATURE_COLS,
            "target": TARGET_COL,
            "classes": CLASS_NAMES,
            "metrics": self.metrics,
            "hyperparameters": HYPERPARAMS,
        }

        with open(meta_file, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"✓ Metadata: {meta_file}")

    def run(self, csv_file: str, model_name: str = "blood_pressure_classifier"):
        """Complete pipeline: load → train → evaluate → save."""
        logger.info("=" * 70)
        logger.info("ML TRAINING PIPELINE")
        logger.info("=" * 70 + "\n")

        self.load_and_prepare(csv_file)
        self.train()
        self.evaluate()
        self.save(model_name)

        logger.info("\n" + "=" * 70)
        logger.info("✓ TRAINING COMPLETED")
        logger.info("=" * 70)
        logger.info(f"Model: {model_name}")
        logger.info(f"  Accuracy: {self.metrics['accuracy']:.4f}")
        logger.info(f"  F1-Score: {self.metrics['f1']:.4f}")
        logger.info(f"  ROC-AUC:  {self.metrics['roc_auc']:.4f}\n")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import sys

    csv_file = sys.argv[1] if len(sys.argv) > 1 else "ml_data/blood_pressure_features.csv"

    trainer = MLTrainer(output_dir="ml_models")
    trainer.run(csv_file)
