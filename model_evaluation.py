import torch
import torch.nn as nn

import os
import sys
import glob
import time
import json
import joblib

import numpy as np
import pandas as pd


# ============================================================
# PHASE 12 - MODEL EVALUATION
# ============================================================
#
# Responsibilities:
#
#   1. Load current production model.
#   2. Load latest candidate model.
#   3. Load historical evaluation data.
#   4. Evaluate both models on the same data.
#   5. Calculate:
#        - Accuracy
#        - Detection Rate
#        - False Alarm Rate
#        - Precision
#        - F1
#   6. Compare old vs candidate.
#   7. Produce ACCEPTED / REJECTED decision.
#   8. Save evaluation metadata/reports.
#
# IMPORTANT:
#
#   THIS FILE DOES NOT PROMOTE MODELS.
#
#   Promotion is handled ONLY by:
#
#       model_lifecycle.py
#
# Pipeline:
#
# ADWIN
#    ↓
# automatic_retraining_orchestrator.py
#    ↓
# pyspark_retraining.py
#    ↓
# model_evaluation.py
#    ↓
# model_lifecycle.py
#    ↓
# production/vN
#
# ============================================================


print(
    "=" * 90
)

print(
    "PHASE 12 - OLD VS CANDIDATE MODEL EVALUATION"
)

print(
    "=" * 90
)


# ============================================================
# BASE PATHS
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

HISTORY_DIR = os.path.join(
    BASE_DIR,
    "data",
    "history"
)

MODEL_DIR = os.path.join(
    BASE_DIR,
    "models",
    "saved_models"
)

PRODUCTION_DIR = os.path.join(
    MODEL_DIR,
    "production"
)

CANDIDATES_DIR = os.path.join(
    MODEL_DIR,
    "candidates"
)

PROCESSED_DIR = os.path.join(
    BASE_DIR,
    "data",
    "processed"
)

REGISTRY_PATH = os.path.join(
    MODEL_DIR,
    "model_registry.json"
)


os.makedirs(
    PROCESSED_DIR,
    exist_ok=True
)

os.makedirs(
    PRODUCTION_DIR,
    exist_ok=True
)

os.makedirs(
    CANDIDATES_DIR,
    exist_ok=True
)


# ============================================================
# LOCKED CONFIGURATION
# ============================================================

EXPECTED_FEATURES = 59

IF_WEIGHT = 0.2

AE_WEIGHT = 0.8

FUSION_THRESHOLD = 0.85

F1_TOLERANCE = 0.02

FAR_TOLERANCE = 0.02


# ============================================================
# AUTOENCODER ARCHITECTURE
# ============================================================

class Autoencoder(
    nn.Module
):

    def __init__(
        self,
        input_dim
    ):

        super().__init__()


        self.encoder = nn.Sequential(

            nn.Linear(
                input_dim,
                64
            ),

            nn.ReLU(),

            nn.Linear(
                64,
                32
            ),

            nn.ReLU(),

            nn.Linear(
                32,
                16
            ),

            nn.ReLU()
        )


        self.decoder = nn.Sequential(

            nn.Linear(
                16,
                32
            ),

            nn.ReLU(),

            nn.Linear(
                32,
                64
            ),

            nn.ReLU(),

            nn.Linear(
                64,
                input_dim
            )
        )


    def forward(
        self,
        x
    ):

        latent = self.encoder(
            x
        )

        reconstructed = self.decoder(
            latent
        )

        return reconstructed


# ============================================================
# JSON HELPERS
# ============================================================

def load_json(
    path
):

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(
            file
        )


def save_json(
    path,
    data
):

    directory = os.path.dirname(
        path
    )

    if directory:

        os.makedirs(
            directory,
            exist_ok=True
        )


    temporary_path = (
        path
        +
        ".tmp"
    )


    with open(
        temporary_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            indent=4
        )


    os.replace(
        temporary_path,
        path
    )


# ============================================================
# FIND VERSION DIRECTORIES
# ============================================================

def find_versions(
    directory
):

    versions = []


    if not os.path.exists(
        directory
    ):

        return versions


    for name in os.listdir(
        directory
    ):

        if not name.startswith(
            "v"
        ):

            continue


        path = os.path.join(
            directory,
            name
        )


        if not os.path.isdir(
            path
        ):

            continue


        try:

            version = int(
                name[1:]
            )

        except ValueError:

            continue


        versions.append(
            (
                version,
                path
            )
        )


    versions.sort(
        key=lambda item: item[0]
    )


    return versions


# ============================================================
# FIND CURRENT PRODUCTION
# ============================================================

def find_current_production():

    registry_version = None


    # --------------------------------------------------------
    # First preference:
    # model_registry.json
    # --------------------------------------------------------

    if os.path.exists(
        REGISTRY_PATH
    ):

        try:

            registry = load_json(
                REGISTRY_PATH
            )

            registry_version = (
                registry.get(
                    "current_production_version"
                )
            )

            if registry_version is not None:

                registry_version = int(
                    registry_version
                )

        except Exception as exc:

            print(
                "WARNING: Could not read "
                f"model registry: {exc}"
            )


    # --------------------------------------------------------
    # Registry version
    # --------------------------------------------------------

    if registry_version is not None:

        version_path = os.path.join(

            PRODUCTION_DIR,

            f"v{registry_version}"
        )


        if os.path.isdir(
            version_path
        ):

            return (
                registry_version,
                version_path
            )


        print(
            "WARNING: Registry points to "
            f"production v{registry_version}, "
            "but directory does not exist."
        )


    # --------------------------------------------------------
    # Fallback:
    # highest production version
    # --------------------------------------------------------

    versions = find_versions(
        PRODUCTION_DIR
    )


    if versions:

        return versions[-1]


    return None


# ============================================================
# FIND LATEST CANDIDATE
# ============================================================

def find_latest_candidate():

    versions = find_versions(
        CANDIDATES_DIR
    )


    if not versions:

        return None


    return versions[-1]


# ============================================================
# REQUIRED MODEL FILES
# ============================================================

MODEL_FILES = [

    "isolation_forest.pkl",

    "autoencoder.pth",

    "ae_threshold.pkl",

    "ae_metadata.pkl",

    "model_metadata.json"

]


# ============================================================
# VERIFY MODEL DIRECTORY
# ============================================================

def verify_model_directory(
    directory,
    label
):

    print(
        f"\nChecking {label}..."
    )


    for filename in MODEL_FILES:

        path = os.path.join(
            directory,
            filename
        )


        if not os.path.exists(
            path
        ):

            raise FileNotFoundError(

                f"{label} is missing "
                f"required file: {filename}\n"
                f"Expected path: {path}"
            )


        print(
            f"  OK: {filename}"
        )


# ============================================================
# LOAD HISTORICAL DATA
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "LOADING HISTORICAL EVALUATION DATA"
)

print(
    "=" * 90
)


parquet_files = sorted(
    glob.glob(
        os.path.join(
            HISTORY_DIR,
            "*.parquet"
        )
    )
)


print(
    f"Parquet files found: "
    f"{len(parquet_files)}"
)


if not parquet_files:

    print(
        "ERROR: No Parquet history found."
    )

    sys.exit(1)


frames = []


for filepath in parquet_files:

    try:

        df = pd.read_parquet(
            filepath
        )


        if (
            "features"
            in df.columns
        ):

            frames.append(
                df
            )


    except Exception as exc:

        print(
            f"WARNING: Could not read "
            f"{filepath}: {exc}"
        )


if not frames:

    print(
        "ERROR: No Parquet files contain features."
    )

    sys.exit(1)


history = pd.concat(
    frames,
    ignore_index=True
)


print(
    f"Historical records loaded: "
    f"{len(history)}"
)


# ============================================================
# VALIDATE FEATURES
# ============================================================

print(
    "\nValidating feature vectors..."
)


valid_features = []

valid_status = []


for _, row in history.iterrows():

    features = row[
        "features"
    ]


    if not isinstance(

        features,

        (
            list,
            tuple,
            np.ndarray
        )

    ):

        continue


    if len(features) != EXPECTED_FEATURES:

        continue


    try:

        vector = np.asarray(

            features,

            dtype=np.float32
        )


        if not np.all(
            np.isfinite(
                vector
            )
        ):

            continue


        valid_features.append(
            vector
        )


        status = str(
            row.get(
                "status",
                "UNKNOWN"
            )
        ).upper().strip()


        valid_status.append(
            status
        )


    except Exception:

        continue


if not valid_features:

    print(
        "ERROR: No valid feature vectors."
    )

    sys.exit(1)


X = np.asarray(
    valid_features,
    dtype=np.float32
)


statuses = np.asarray(
    valid_status
)


print(
    f"Valid records : "
    f"{len(X)}"
)


print(
    f"Feature count : "
    f"{X.shape[1]}"
)


print(
    f"Evaluation matrix: "
    f"{X.shape}"
)


if X.shape[1] != EXPECTED_FEATURES:

    print(
        "ERROR: Historical feature count "
        "does not equal 59."
    )

    sys.exit(1)


# ============================================================
# STATUS MASKS
# ============================================================

normal_mask = (
    statuses == "NORMAL"
)

anomaly_mask = (
    statuses == "ANOMALY"
)


print(
    "\nHistorical status distribution:"
)


print(
    pd.Series(
        statuses
    ).value_counts()
)


if not np.any(
    anomaly_mask
):

    print(
        "WARNING: No ANOMALY records "
        "were found in evaluation data."
    )


# ============================================================
# FIND MODELS
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "LOCATING PRODUCTION AND CANDIDATE MODELS"
)

print(
    "=" * 90
)


production = (
    find_current_production()
)


if production is None:

    print(
        "ERROR: No production model found."
    )

    print(
        "Expected: models/saved_models/production/vN/"
    )

    sys.exit(1)


old_version, old_model_dir = (
    production
)


candidate = (
    find_latest_candidate()
)


if candidate is None:

    print(
        "ERROR: No candidate model found."
    )

    print(
        "Expected: models/saved_models/candidates/vN/"
    )

    sys.exit(1)


candidate_version, candidate_model_dir = (
    candidate
)


print(
    f"Current production: "
    f"v{old_version}"
)


print(
    f"Latest candidate: "
    f"v{candidate_version}"
)


if (
    candidate_version
    <=
    old_version
):

    print(
        "WARNING: Candidate version is not "
        "newer than current production."
    )


# ============================================================
# VERIFY MODEL FILES
# ============================================================

verify_model_directory(
    old_model_dir,
    "CURRENT PRODUCTION"
)


verify_model_directory(
    candidate_model_dir,
    "CANDIDATE"
)


# ============================================================
# MODEL PATHS
# ============================================================

OLD_IF_PATH = os.path.join(
    old_model_dir,
    "isolation_forest.pkl"
)

OLD_AE_PATH = os.path.join(
    old_model_dir,
    "autoencoder.pth"
)

OLD_THRESHOLD_PATH = os.path.join(
    old_model_dir,
    "ae_threshold.pkl"
)

OLD_METADATA_PATH = os.path.join(
    old_model_dir,
    "ae_metadata.pkl"
)


CANDIDATE_IF_PATH = os.path.join(
    candidate_model_dir,
    "isolation_forest.pkl"
)

CANDIDATE_AE_PATH = os.path.join(
    candidate_model_dir,
    "autoencoder.pth"
)

CANDIDATE_THRESHOLD_PATH = os.path.join(
    candidate_model_dir,
    "ae_threshold.pkl"
)

CANDIDATE_METADATA_PATH = os.path.join(
    candidate_model_dir,
    "ae_metadata.pkl"
)


# ============================================================
# DEVICE
# ============================================================

device = torch.device(

    "cuda"
    if torch.cuda.is_available()
    else "cpu"

)


print(
    f"\nEvaluation device: "
    f"{device}"
)


# ============================================================
# LOAD OLD PRODUCTION
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "LOADING CURRENT PRODUCTION MODEL"
)

print(
    "=" * 90
)


old_if = joblib.load(
    OLD_IF_PATH
)


old_metadata = joblib.load(
    OLD_METADATA_PATH
)


old_threshold = float(
    joblib.load(
        OLD_THRESHOLD_PATH
    )
)


old_input_dim = int(
    old_metadata[
        "input_dim"
    ]
)


print(
    f"Production version: "
    f"v{old_version}"
)


print(
    f"Old AE input dimension: "
    f"{old_input_dim}"
)


print(
    f"Old AE threshold: "
    f"{old_threshold}"
)


old_ae = Autoencoder(
    old_input_dim
).to(
    device
)


old_ae.load_state_dict(
    torch.load(
        OLD_AE_PATH,
        map_location=device
    )
)


old_ae.eval()


# ============================================================
# LOAD CANDIDATE
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "LOADING CANDIDATE MODEL"
)

print(
    "=" * 90
)


candidate_if = joblib.load(
    CANDIDATE_IF_PATH
)


candidate_metadata = joblib.load(
    CANDIDATE_METADATA_PATH
)


candidate_threshold = float(
    joblib.load(
        CANDIDATE_THRESHOLD_PATH
    )
)


candidate_input_dim = int(
    candidate_metadata[
        "input_dim"
    ]
)


print(
    f"Candidate version: "
    f"v{candidate_version}"
)


print(
    f"Candidate AE input dimension: "
    f"{candidate_input_dim}"
)


print(
    f"Candidate AE threshold: "
    f"{candidate_threshold}"
)


candidate_ae = Autoencoder(
    candidate_input_dim
).to(
    device
)


candidate_ae.load_state_dict(
    torch.load(
        CANDIDATE_AE_PATH,
        map_location=device
    )
)


candidate_ae.eval()


# ============================================================
# FEATURE DIMENSION CHECK
# ============================================================

if (
    old_input_dim
    !=
    X.shape[1]
):

    print(
        "\nERROR: Old production model "
        "feature dimension does not match "
        "historical data."
    )

    sys.exit(1)


if (
    candidate_input_dim
    !=
    X.shape[1]
):

    print(
        "\nERROR: Candidate model "
        "feature dimension does not match "
        "historical data."
    )

    sys.exit(1)


# ============================================================
# SCORE FUNCTION
# ============================================================

def calculate_scores(

    if_model,

    ae_model,

    ae_threshold

):

    # --------------------------------------------------------
    # Isolation Forest
    # --------------------------------------------------------

    if_scores = -if_model.score_samples(
        X
    )


    if_scores = np.asarray(

        if_scores,

        dtype=np.float64
    )


    # --------------------------------------------------------
    # Normalize IF score
    # --------------------------------------------------------

    if_scores = np.clip(

        if_scores,

        0.0,

        1.0
    )


    # --------------------------------------------------------
    # Autoencoder
    # --------------------------------------------------------

    ae_errors = []


    batch_size = 512


    with torch.no_grad():

        for start in range(

            0,

            len(X),

            batch_size
        ):

            end = min(

                start + batch_size,

                len(X)
            )


            batch = torch.tensor(

                X[start:end],

                dtype=torch.float32,

                device=device
            )


            reconstructed = ae_model(
                batch
            )


            errors = torch.mean(

                (
                    batch
                    -
                    reconstructed
                ) ** 2,

                dim=1
            )


            ae_errors.extend(

                errors
                .cpu()
                .numpy()
                .tolist()
            )


    ae_errors = np.asarray(

        ae_errors,

        dtype=np.float64
    )


    # --------------------------------------------------------
    # Normalize AE score
    # --------------------------------------------------------

    if ae_threshold <= 0:

        ae_scores = np.ones_like(
            ae_errors
        )

    else:

        ae_scores = np.minimum(

            ae_errors
            /
            ae_threshold,

            1.0
        )


    # --------------------------------------------------------
    # Fusion
    # --------------------------------------------------------

    fusion_scores = (

        IF_WEIGHT
        *
        if_scores

        +

        AE_WEIGHT
        *
        ae_scores

    )


    # --------------------------------------------------------
    # Prediction
    # --------------------------------------------------------

    predictions = (

        fusion_scores
        >=
        FUSION_THRESHOLD

    )


    return (

        if_scores,

        ae_errors,

        ae_scores,

        fusion_scores,

        predictions

    )


# ============================================================
# METRICS
# ============================================================

def calculate_metrics(
    predictions
):

    actual = (
        anomaly_mask
    )

    predicted = (
        predictions
    )


    true_normal = int(
        np.sum(
            (~actual)
            &
            (~predicted)
        )
    )


    false_alarm = int(
        np.sum(
            (~actual)
            &
            predicted
        )
    )


    missed_attack = int(
        np.sum(
            actual
            &
            (~predicted)
        )
    )


    true_attack = int(
        np.sum(
            actual
            &
            predicted
        )
    )


    total = len(
        actual
    )


    accuracy = (

        true_normal
        +
        true_attack

    ) / max(
        total,
        1
    )


    detection_rate = (

        true_attack

        /

        max(

            true_attack
            +
            missed_attack,

            1
        )
    )


    false_alarm_rate = (

        false_alarm

        /

        max(

            false_alarm
            +
            true_normal,

            1
        )
    )


    precision = (

        true_attack

        /

        max(

            true_attack
            +
            false_alarm,

            1
        )
    )


    f1 = (

        2
        *
        precision
        *
        detection_rate

        /

        max(

            precision
            +
            detection_rate,

            1e-12
        )
    )


    return {

        "accuracy":
            float(accuracy),

        "true_normal":
            true_normal,

        "false_alarm":
            false_alarm,

        "missed_attack":
            missed_attack,

        "true_attack":
            true_attack,

        "detection_rate":
            float(detection_rate),

        "false_alarm_rate":
            float(false_alarm_rate),

        "precision":
            float(precision),

        "f1":
            float(f1)

    }


# ============================================================
# EVALUATE OLD MODEL
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "EVALUATING CURRENT PRODUCTION MODEL"
)

print(
    "=" * 90
)


old_start = time.perf_counter()


(

    old_if_scores,

    old_ae_errors,

    old_ae_scores,

    old_fusion_scores,

    old_predictions

) = calculate_scores(

    old_if,

    old_ae,

    old_threshold

)


old_metrics = calculate_metrics(
    old_predictions
)


old_eval_time = (

    time.perf_counter()
    -
    old_start

)


print(
    f"Evaluation time: "
    f"{old_eval_time:.3f} seconds"
)


for key, value in old_metrics.items():

    print(
        f"{key}: {value}"
    )


# ============================================================
# EVALUATE CANDIDATE
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "EVALUATING CANDIDATE MODEL"
)

print(
    "=" * 90
)


candidate_start = (
    time.perf_counter()
)


(

    candidate_if_scores,

    candidate_ae_errors,

    candidate_ae_scores,

    candidate_fusion_scores,

    candidate_predictions

) = calculate_scores(

    candidate_if,

    candidate_ae,

    candidate_threshold

)


candidate_metrics = calculate_metrics(
    candidate_predictions
)


candidate_eval_time = (

    time.perf_counter()
    -
    candidate_start

)


print(
    f"Evaluation time: "
    f"{candidate_eval_time:.3f} seconds"
)


for key, value in candidate_metrics.items():

    print(
        f"{key}: {value}"
    )


# ============================================================
# METRIC CHANGES
# ============================================================

accuracy_change = (

    candidate_metrics[
        "accuracy"
    ]

    -

    old_metrics[
        "accuracy"
    ]

)


detection_change = (

    candidate_metrics[
        "detection_rate"
    ]

    -

    old_metrics[
        "detection_rate"
    ]

)


precision_change = (

    candidate_metrics[
        "precision"
    ]

    -

    old_metrics[
        "precision"
    ]

)


f1_change = (

    candidate_metrics[
        "f1"
    ]

    -

    old_metrics[
        "f1"
    ]

)


far_change = (

    candidate_metrics[
        "false_alarm_rate"
    ]

    -

    old_metrics[
        "false_alarm_rate"
    ]

)


metric_changes = {

    "accuracy":
        float(accuracy_change),

    "detection_rate":
        float(detection_change),

    "precision":
        float(precision_change),

    "f1":
        float(f1_change),

    "false_alarm_rate":
        float(far_change)

}


# ============================================================
# DECISION
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "MODEL EVALUATION DECISION"
)

print(
    "=" * 90
)


f1_ok = (

    candidate_metrics[
        "f1"
    ]

    >=

    old_metrics[
        "f1"
    ]
    -
    F1_TOLERANCE

)


far_ok = (

    candidate_metrics[
        "false_alarm_rate"
    ]

    <=

    old_metrics[
        "false_alarm_rate"
    ]
    +
    FAR_TOLERANCE

)


strong_improvement = (

    candidate_metrics[
        "f1"
    ]

    >

    old_metrics[
        "f1"
    ]

    and

    far_ok

)


safe_maintenance = (

    f1_ok

    and

    far_ok

)


if strong_improvement:

    decision = (
        "ACCEPTED"
    )

    reason = (

        "Candidate improved F1 "
        "while maintaining an "
        "acceptable false alarm rate."

    )


elif safe_maintenance:

    decision = (
        "ACCEPTED"
    )

    reason = (

        "Candidate safely maintains "
        "performance within the "
        "configured tolerance."

    )


else:

    decision = (
        "REJECTED"
    )

    reason = (

        "Candidate did not satisfy "
        "the required performance "
        "criteria. Production model "
        "must remain unchanged."

    )


print(
    f"Decision: {decision}"
)


print(
    f"Reason: {reason}"
)


# ============================================================
# EVALUATION TIME
# ============================================================

total_evaluation_time = (

    old_eval_time

    +

    candidate_eval_time

)


# ============================================================
# PHASE 12 METADATA
# ============================================================
#
# IMPORTANT:
#
# This file is intentionally written so that
# model_lifecycle.py can make the actual
# promotion decision.
#
# No model files are copied here.
#
# ============================================================

metadata = {

    "phase":
        "PHASE_12",

    "evaluation_type":
        "OLD_VS_CANDIDATE",

    "decision":
        decision,

    "decision_reason":
        reason,

    "old_production_version":
        int(old_version),

    "candidate_version":
        int(candidate_version),

    "evaluation_records":
        int(len(X)),

    "feature_count":
        int(X.shape[1]),

    "normal_records":
        int(np.sum(
            normal_mask
        )),

    "anomaly_records":
        int(np.sum(
            anomaly_mask
        )),

    "if_weight":
        IF_WEIGHT,

    "ae_weight":
        AE_WEIGHT,

    "fusion_threshold":
        FUSION_THRESHOLD,

    "old_ae_threshold":
        float(old_threshold),

    "candidate_ae_threshold":
        float(candidate_threshold),

    "old_metrics":
        old_metrics,

    "candidate_metrics":
        candidate_metrics,

    "metric_changes":
        metric_changes,

    "f1_tolerance":
        F1_TOLERANCE,

    "false_alarm_tolerance":
        FAR_TOLERANCE,

    "old_evaluation_time_seconds":
        float(old_eval_time),

    "candidate_evaluation_time_seconds":
        float(candidate_eval_time),

    "evaluation_time":
        float(total_evaluation_time),

    "roc_auc":
        None,

    "trigger":
        "PHASE_11_CANDIDATE_EVALUATION",

    "promotion_owner":
        "model_lifecycle.py",

    "production_modified":
        False,

    "timestamp":
        time.time()

}


# ============================================================
# SAVE PHASE 12 METADATA
# ============================================================

metadata_path = os.path.join(

    PROCESSED_DIR,

    "phase12_model_metadata.json"

)


save_json(
    metadata_path,
    metadata
)


# ============================================================
# SAVE COMPARISON CSV
# ============================================================

comparison = pd.DataFrame(

    [

        {

            "model":
                "OLD_PRODUCTION",

            "version":
                int(old_version),

            **old_metrics

        },

        {

            "model":
                "CANDIDATE",

            "version":
                int(candidate_version),

            **candidate_metrics

        }

    ]

)


comparison_path = os.path.join(

    PROCESSED_DIR,

    "phase12_model_comparison.csv"

)


comparison.to_csv(

    comparison_path,

    index=False

)


# ============================================================
# SAVE DECISION REPORT
# ============================================================

report = pd.DataFrame(

    [

        {

            "production_version_before":
                int(old_version),

            "candidate_version":
                int(candidate_version),

            "decision":
                decision,

            "reason":
                reason,

            "evaluation_records":
                len(X),

            "old_f1":
                old_metrics[
                    "f1"
                ],

            "candidate_f1":
                candidate_metrics[
                    "f1"
                ],

            "old_accuracy":
                old_metrics[
                    "accuracy"
                ],

            "candidate_accuracy":
                candidate_metrics[
                    "accuracy"
                ],

            "old_detection_rate":
                old_metrics[
                    "detection_rate"
                ],

            "candidate_detection_rate":
                candidate_metrics[
                    "detection_rate"
                ],

            "old_false_alarm_rate":
                old_metrics[
                    "false_alarm_rate"
                ],

            "candidate_false_alarm_rate":
                candidate_metrics[
                    "false_alarm_rate"
                ],

            "old_precision":
                old_metrics[
                    "precision"
                ],

            "candidate_precision":
                candidate_metrics[
                    "precision"
                ],

            "if_weight":
                IF_WEIGHT,

            "ae_weight":
                AE_WEIGHT,

            "fusion_threshold":
                FUSION_THRESHOLD,

            "old_ae_threshold":
                old_threshold,

            "candidate_ae_threshold":
                candidate_threshold,

            "old_evaluation_time_seconds":
                old_eval_time,

            "candidate_evaluation_time_seconds":
                candidate_eval_time,

            "evaluation_time":
                total_evaluation_time,

            "timestamp":
                time.time()

        }

    ]

)


report_path = os.path.join(

    PROCESSED_DIR,

    "phase12_promotion_report.csv"

)


report.to_csv(

    report_path,

    index=False

)


# ============================================================
# DO NOT PROMOTE HERE
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "IMPORTANT: PHASE 12 DOES NOT PROMOTE MODELS"
)

print(
    "=" * 90
)

print(
    "Evaluation decision has been saved."
)

print(
    "Actual promotion is delegated to:"
)

print(
    "model_lifecycle.py"
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print(
    "\n" + "=" * 90
)

print(
    "PHASE 12 COMPLETE"
)

print(
    "=" * 90
)


print(
    f"Current production : v{old_version}"
)


print(
    f"Candidate          : v{candidate_version}"
)


print(
    f"Decision           : {decision}"
)


print(
    f"Evaluation records : {len(X)}"
)


print(
    f"Old F1             : "
    f"{old_metrics['f1']:.4f}"
)


print(
    f"Candidate F1       : "
    f"{candidate_metrics['f1']:.4f}"
)


print(
    f"Old FAR            : "
    f"{old_metrics['false_alarm_rate']:.4f}"
)


print(
    f"Candidate FAR      : "
    f"{candidate_metrics['false_alarm_rate']:.4f}"
)


print(
    f"Evaluation time    : "
    f"{total_evaluation_time:.3f}s"
)


print(
    "\nComparison report:"
)


print(
    comparison_path
)


print(
    "\nDecision report:"
)


print(
    report_path
)


print(
    "\nMetadata:"
)


print(
    metadata_path
)


print(
    "\nProduction models were NOT modified."
)


print(
    "Phase 14 model_lifecycle.py is responsible "
    "for promotion/rejection."
)


print(
    "=" * 90
)