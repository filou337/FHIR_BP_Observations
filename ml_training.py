"""
ML PIPELINE - MODEL TRAINING
═════════════════════════════════════════════════════════════════════

RESPONSABILITÉ : Entraîner un modèle XGBoost pour classification TA
- Charger le dataset CSV
- Split Train/Test (80/20)
- Entraîner XGBoost Classifier
- Évaluer : Accuracy, Precision, Recall, F1, ROC-AUC, Confusion Matrix
- Sauvegarder le modèle
- Feature importance analysis

MODEL : XGBoost Classifier
TASK : Classification multiclasse (5 catégories)
CLASSES : NORMAL (0) → ELEVATED (1) → STAGE_1 (2) → STAGE_2 (3) → CRISIS (4)
"""

import json
import logging
import pickle
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from colorlog import ColoredFormatter
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report,
    roc_curve, auc
)
import xgboost as xgb
from xgboost import XGBClassifier


# ═══════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════

def setup_logging():
    """Configure le logging avec couleurs"""
    formatter = ColoredFormatter(
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        fmt='%(log_color)s[%(asctime)s %(levelname)s]%(reset)s %(message)s',
        datefmt='%H:%M:%S'
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

logger = setup_logging()


# ═══════════════════════════════════════════════════════════════════
# MODEL TRAINER CLASS
# ═══════════════════════════════════════════════════════════════════

class MLModelTrainer:
    """
    Entraîne et évalue un modèle XGBoost pour la classification TA
    """
    
    # Noms des classes pour affichage
    CLASS_NAMES = {
        0: 'NORMAL',
        1: 'ELEVATED',
        2: 'HYPERTENSION_STAGE_1',
        3: 'HYPERTENSION_STAGE_2',
        4: 'HYPERTENSIVE_CRISIS'
    }
    
    # Features utilisées pour l'entraînement
    FEATURE_COLUMNS = [
        'systolic',
        'diastolic',
        'age',
        'gender',
        'trend',
        'risk_score',
        'hour_of_day'
    ]
    
    # Target
    TARGET_COLUMN = 'blood_pressure_category'
    
    def __init__(self, model_dir: str = 'ml_models'):
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(exist_ok=True)
        
        self.model = None
        self.scaler = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.X_train_scaled = None
        self.X_test_scaled = None
        
        self.metrics = {}
    
    def load_dataset(self, csv_file: str) -> pd.DataFrame:
        """
        Charge le dataset CSV
        
        Args:
            csv_file: Chemin du fichier CSV
            
        Returns:
            DataFrame
        """
        logger.info("=" * 70)
        logger.info("CHARGEMENT DU DATASET")
        logger.info("=" * 70)
        
        logger.info(f"📖 Chargement {csv_file}...")
        df = pd.read_csv(csv_file)
        logger.info(f"✅ Dataset chargé: {len(df)} lignes x {len(df.columns)} colonnes")
        
        # Vérifier les colonnes requises
        missing_cols = set(self.FEATURE_COLUMNS + [self.TARGET_COLUMN]) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Colonnes manquantes: {missing_cols}")
        
        logger.info(f"✅ Toutes les colonnes requises présentes")
        return df
    
    def prepare_data(self, df: pd.DataFrame, test_size: float = 0.2, random_state: int = 42):
        """
        Prépare les données : split train/test + scaling
        
        Args:
            df: DataFrame complet
            test_size: Pourcentage test
            random_state: Seed pour reproductibilité
        """
        logger.info("=" * 70)
        logger.info("PRÉPARATION DES DONNÉES")
        logger.info("=" * 70)
        
        # ═══════════════════════════════════════════════════════════════════
        # 1. FEATURES & TARGET
        # ═══════════════════════════════════════════════════════════════════
        X = df[self.FEATURE_COLUMNS].copy()
        y = df[self.TARGET_COLUMN].copy()
        
        logger.info(f"📊 Features shape: {X.shape}")
        logger.info(f"🏷️  Target shape: {y.shape}")
        
        # ═══════════════════════════════════════════════════════════════════
        # 2. TRAIN/TEST SPLIT
        # ═══════════════════════════════════════════════════════════════════
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y,
            test_size=test_size,
            random_state=random_state,
            stratify=y  # Important pour les datasets déséquilibrés
        )
        
        logger.info(f"✅ Train size: {len(self.X_train)} ({len(self.X_train)/len(X)*100:.1f}%)")
        logger.info(f"✅ Test size: {len(self.X_test)} ({len(self.X_test)/len(X)*100:.1f}%)")
        
        # ═══════════════════════════════════════════════════════════════════
        # 3. SCALING (IMPORTANT pour XGBoost avec certains hyperparams)
        # ═══════════════════════════════════════════════════════════════════
        self.scaler = StandardScaler()
        self.X_train_scaled = self.scaler.fit_transform(self.X_train)
        self.X_test_scaled = self.scaler.transform(self.X_test)
        
        logger.info("✅ Scaling complété")
        
        # ═══════════════════════════════════════════════════════════════════
        # 4. DISTRIBUTION DES CLASSES
        # ═══════════════════════════════════════════════════════════════════
        logger.info("🏷️  Distribution des classes (train):")
        for class_id, count in self.y_train.value_counts().sort_index().items():
            class_name = self.CLASS_NAMES[class_id]
            pct = count / len(self.y_train) * 100
            logger.info(f"   {class_name:25s}: {count:5d} ({pct:5.1f}%)")
    
    def train_model(self, n_estimators: int = 200, max_depth: int = 7, learning_rate: float = 0.1):
        """
        Entraîne le modèle XGBoost
        
        Args:
            n_estimators: Nombre d'arbres
            max_depth: Profondeur max des arbres
            learning_rate: Taux d'apprentissage
        """
        logger.info("=" * 70)
        logger.info("ENTRAÎNEMENT DU MODÈLE XGBOOST")
        logger.info("=" * 70)
        
        logger.info(f"⚙️  Hyperparamètres:")
        logger.info(f"   n_estimators: {n_estimators}")
        logger.info(f"   max_depth: {max_depth}")
        logger.info(f"   learning_rate: {learning_rate}")
        
        # Créer et entraîner le modèle
        self.model = XGBClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            objective='multi:softmax',  # Classification multiclasse
            num_class=5,  # 5 catégories
            random_state=42,
            verbosity=1,
            eval_metric='mlogloss',
            tree_method='hist'  # Fast GPU training if available
        )
        
        logger.info("🚀 Entraînement en cours...")
        self.model.fit(
            self.X_train_scaled,
            self.y_train,
            eval_set=[(self.X_test_scaled, self.y_test)],
            verbose=False
        )
        
        logger.info("✅ Entraînement complété!")
    
    def evaluate_model(self) -> Dict:
        """
        Évalue le modèle sur le test set
        
        Returns:
            Dictionnaire avec les métriques
        """
        logger.info("=" * 70)
        logger.info("ÉVALUATION DU MODÈLE")
        logger.info("=" * 70)
        
        # ═══════════════════════════════════════════════════════════════════
        # 1. PREDICTIONS
        # ═══════════════════════════════════════════════════════════════════
        y_pred = self.model.predict(self.X_test_scaled)
        y_pred_proba = self.model.predict_proba(self.X_test_scaled)
        
        # ═══════════════════════════════════════════════════════════════════
        # 2. MÉTRIQUES GLOBALES
        # ═══════════════════════════════════════════════════════════════════
        accuracy = accuracy_score(self.y_test, y_pred)
        precision_macro = precision_score(self.y_test, y_pred, average='macro', zero_division=0)
        recall_macro = recall_score(self.y_test, y_pred, average='macro', zero_division=0)
        f1_macro = f1_score(self.y_test, y_pred, average='macro', zero_division=0)
        
        logger.info("📊 Métriques globales:")
        logger.info(f"   Accuracy  : {accuracy:.4f}")
        logger.info(f"   Precision : {precision_macro:.4f}")
        logger.info(f"   Recall    : {recall_macro:.4f}")
        logger.info(f"   F1-Score  : {f1_macro:.4f}")
        
        # ═══════════════════════════════════════════════════════════════════
        # 3. RAPPORT DÉTAILLÉ PAR CLASSE
        # ═══════════════════════════════════════════════════════════════════
        logger.info("\n📋 Rapport détaillé par classe:")
        logger.info("-" * 70)
        report = classification_report(
            self.y_test, y_pred,
            target_names=[self.CLASS_NAMES[i] for i in range(5)],
            digits=4
        )
        logger.info(report)
        
        # ═══════════════════════════════════════════════════════════════════
        # 4. CONFUSION MATRIX
        # ═══════════════════════════════════════════════════════════════════
        cm = confusion_matrix(self.y_test, y_pred)
        logger.info("📊 Matrice de confusion:")
        logger.info("-" * 70)
        
        # Format la matrice
        header = "Predicted →"
        logger.info(f"     {header:60s}")
        for i, class_id in enumerate(range(5)):
            class_name = self.CLASS_NAMES[class_id][:15]
            row_str = f"{class_name:15s} | " + " ".join(f"{cm[i, j]:6d}" for j in range(5))
            logger.info(row_str)
        
        # ═══════════════════════════════════════════════════════════════════
        # 5. ROC-AUC (ONE-VS-REST)
        # ═══════════════════════════════════════════════════════════════════
        try:
            # Binariser les labels pour ROC-AUC multi-classe
            from sklearn.preprocessing import label_binarize
            y_test_bin = label_binarize(self.y_test, classes=range(5))
            
            roc_auc_scores = []
            for i in range(5):
                try:
                    roc_auc = roc_auc_score(y_test_bin[:, i], y_pred_proba[:, i])
                    roc_auc_scores.append(roc_auc)
                    logger.info(f"   ROC-AUC {self.CLASS_NAMES[i]:25s}: {roc_auc:.4f}")
                except:
                    logger.warning(f"   ROC-AUC {self.CLASS_NAMES[i]:25s}: N/A")
            
            roc_auc_macro = np.mean([s for s in roc_auc_scores if not np.isnan(s)])
            logger.info(f"   ROC-AUC (macro)                  : {roc_auc_macro:.4f}")
        except Exception as e:
            logger.warning(f"Erreur calcul ROC-AUC: {e}")
            roc_auc_macro = None
        
        # ═══════════════════════════════════════════════════════════════════
        # 6. FEATURE IMPORTANCE
        # ═══════════════════════════════════════════════════════════════════
        logger.info("\n🔍 Feature Importance (top 5):")
        logger.info("-" * 70)
        
        feature_importance = self.model.feature_importances_
        sorted_idx = np.argsort(feature_importance)[::-1]
        
        for rank, idx in enumerate(sorted_idx[:5], 1):
            feature_name = self.FEATURE_COLUMNS[idx]
            importance = feature_importance[idx]
            bar = "█" * int(importance * 50)
            logger.info(f"   {rank}. {feature_name:20s}: {importance:.4f} {bar}")
        
        # ═══════════════════════════════════════════════════════════════════
        # 7. RETOUR DES MÉTRIQUES
        # ═══════════════════════════════════════════════════════════════════
        self.metrics = {
            'accuracy': accuracy,
            'precision': precision_macro,
            'recall': recall_macro,
            'f1': f1_macro,
            'roc_auc': roc_auc_macro if roc_auc_macro else accuracy,
            'confusion_matrix': cm.tolist(),
            'feature_importance': dict(zip(self.FEATURE_COLUMNS, feature_importance))
        }
        
        return self.metrics
    
    def save_model(self, name: str = 'blood_pressure_classifier'):
        """
        Sauvegarde le modèle et les métadonnées
        
        Args:
            name: Nom du modèle
        """
        logger.info("=" * 70)
        logger.info("SAUVEGARDE DU MODÈLE")
        logger.info("=" * 70)
        
        # Chemin des fichiers
        model_file = self.model_dir / f'{name}.pkl'
        scaler_file = self.model_dir / f'{name}_scaler.pkl'
        metadata_file = self.model_dir / f'{name}_metadata.json'
        
        # Sauvegarder le modèle
        joblib.dump(self.model, model_file)
        logger.info(f"✅ Modèle sauvegardé: {model_file}")
        
        # Sauvegarder le scaler
        joblib.dump(self.scaler, scaler_file)
        logger.info(f"✅ Scaler sauvegardé: {scaler_file}")
        
        # Sauvegarder les métadonnées
        metadata = {
            'model_name': name,
            'model_version': '1.0.0',
            'feature_columns': self.FEATURE_COLUMNS,
            'target_column': self.TARGET_COLUMN,
            'class_names': self.CLASS_NAMES,
            'metrics': self.metrics,
            'hyperparameters': {
                'n_estimators': self.model.n_estimators,
                'max_depth': self.model.max_depth,
                'learning_rate': self.model.learning_rate
            }
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"✅ Métadonnées sauvegardées: {metadata_file}")
        
        logger.info(f"\n📁 Tous les fichiers du modèle:")
        logger.info(f"   - {model_file}")
        logger.info(f"   - {scaler_file}")
        logger.info(f"   - {metadata_file}")
    
    def run_full_training(self, csv_file: str, model_name: str = 'blood_pressure_classifier'):
        """
        Pipeline complet : Load → Prepare → Train → Evaluate → Save
        
        Args:
            csv_file: Chemin du CSV avec features
            model_name: Nom du modèle à sauvegarder
        """
        logger.info("\n" + "=" * 70)
        logger.info("🚀 DÉMARRAGE DU PIPELINE D'ENTRAÎNEMENT")
        logger.info("=" * 70 + "\n")
        
        # Étape 1: Charger
        df = self.load_dataset(csv_file)
        
        # Étape 2: Préparer
        self.prepare_data(df)
        
        # Étape 3: Entraîner
        self.train_model()
        
        # Étape 4: Évaluer
        self.evaluate_model()
        
        # Étape 5: Sauvegarder
        self.save_model(model_name)
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ ENTRAÎNEMENT COMPLÉTÉ AVEC SUCCÈS!")
        logger.info("=" * 70)
        logger.info(f"📊 Modèle prêt: {model_name}")
        logger.info(f"   Accuracy: {self.metrics['accuracy']:.4f}")
        logger.info(f"   F1-Score: {self.metrics['f1']:.4f}")
        logger.info(f"   ROC-AUC : {self.metrics['roc_auc']:.4f}\n")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    
    # Chemin du CSV (argument ou default)
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'ml_data/blood_pressure_features.csv'
    
    # Lancer l'entraînement
    trainer = MLModelTrainer(model_dir='ml_models')
    trainer.run_full_training(csv_file)
