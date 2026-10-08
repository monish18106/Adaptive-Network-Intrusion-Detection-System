# ============================================================
# DASHBOARD DATA MODULE
# Real-Time Network Intrusion Detection
# ============================================================

import os
import glob
import json
import math
import time
from threading import RLock

import pandas as pd


# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

HISTORY_DIR = os.path.join(
    BASE_DIR,
    "data",
    "history"
)

PROCESSED_DIR = os.path.join(
    BASE_DIR,
    "data",
    "processed"
)

SAVED_MODELS_DIR = os.path.join(
    BASE_DIR,
    "models",
    "saved_models"
)

PRODUCTION_DIR = os.path.join(
    SAVED_MODELS_DIR,
    "production"
)

CANDIDATES_DIR = os.path.join(
    SAVED_MODELS_DIR,
    "candidates"
)

ARCHIVE_DIR = os.path.join(
    SAVED_MODELS_DIR,
    "archive"
)

REGISTRY_PATH = os.path.join(
    SAVED_MODELS_DIR,
    "model_registry.json"
)


# ============================================================
# RETRAINING
# ============================================================

PHASE11_REPORT = os.path.join(
    PROCESSED_DIR,
    "phase11_retraining_report.csv"
)

PYSPARK_REPORT = os.path.join(
    PROCESSED_DIR,
    "pyspark_retraining_report.csv"
)


# ============================================================
# EVALUATION
# ============================================================

PHASE12_COMPARISON = os.path.join(
    PROCESSED_DIR,
    "phase12_model_comparison.csv"
)

PHASE12_PROMOTION = os.path.join(
    PROCESSED_DIR,
    "phase12_promotion_report.csv"
)

PHASE12_METADATA = os.path.join(
    PROCESSED_DIR,
    "phase12_model_metadata.json"
)


# ============================================================
# ADWIN
# ============================================================

ADWIN_RESULTS = os.path.join(
    PROCESSED_DIR,
    "adwin_drift_results.csv"
)

ADWIN_LATEST = os.path.join(
    PROCESSED_DIR,
    "adwin_latest_result.json"
)

ADWIN_STATE = os.path.join(
    PROCESSED_DIR,
    "adwin_state.json"
)

RETRAINING_TRIGGER = os.path.join(
    PROCESSED_DIR,
    "retraining_trigger.json"
)


# ============================================================
# PIPELINE STATE
# ============================================================

PIPELINE_STATUS = os.path.join(
    PROCESSED_DIR,
    "automatic_pipeline_status.json"
)

AUTOMATIC_STATE = os.path.join(
    PROCESSED_DIR,
    "automatic_retraining_state.json"
)

SELF_CORRECTION = os.path.join(
    PROCESSED_DIR,
    "self_correction_state.json"
)

PIPELINE_LOG = os.path.join(
    PROCESSED_DIR,
    "retraining_pipeline.log"
)


# ============================================================
# CONFIGURATION
# ============================================================

ANOMALY_LIMIT = 50
DRIFT_LIMIT = 100
PIPELINE_LOG_LIMIT = 150


# ============================================================
# CACHE
# ============================================================

_CACHE_LOCK = RLock()

_HISTORY_CACHE = {
    "signature": None,
    "dataframe": None,
}

_JSON_CACHE = {}
_CSV_CACHE = {}


# ============================================================
# FILE HELPERS
# ============================================================

def get_file_signature(path):
    if not path or not os.path.exists(path):
        return None

    try:
        stat = os.stat(path)

        return (
            stat.st_mtime_ns,
            stat.st_size,
        )

    except Exception:
        return None


def get_directory_signature(directory):
    if not os.path.exists(directory):
        return ()

    try:
        files = glob.glob(
            os.path.join(
                directory,
                "**",
                "*.parquet",
            ),
            recursive=True,
        )
    except Exception:
        return ()

    signature = []

    for path in files:
        try:
            stat = os.stat(path)

            signature.append(
                (
                    path,
                    stat.st_mtime_ns,
                    stat.st_size,
                )
            )

        except Exception:
            continue

    signature.sort(
        key=lambda item: item[0]
    )

    return tuple(signature)


def file_mtime(path):
    try:
        if os.path.exists(path):
            return os.path.getmtime(path)
    except Exception:
        pass

    return 0


# ============================================================
# SAFE JSON
# ============================================================

def safe_read_json(path):
    if not path or not os.path.exists(path):
        return {}

    signature = get_file_signature(path)

    with _CACHE_LOCK:
        cached = _JSON_CACHE.get(path)

        if (
            cached is not None
            and cached.get("signature") == signature
        ):
            return cached.get("value", {})

        try:
            with open(
                path,
                "r",
                encoding="utf-8",
            ) as file:
                value = json.load(file)

            if value is None:
                value = {}

        except Exception:
            value = {}

        _JSON_CACHE[path] = {
            "signature": signature,
            "value": value,
        }

        return value


# ============================================================
# SAFE CSV
# ============================================================

def safe_read_csv(path):
    if not path or not os.path.exists(path):
        return pd.DataFrame()

    signature = get_file_signature(path)

    with _CACHE_LOCK:
        cached = _CSV_CACHE.get(path)

        if (
            cached is not None
            and cached.get("signature") == signature
        ):
            return cached.get(
                "value",
                pd.DataFrame(),
            )

        try:
            value = pd.read_csv(path)
        except Exception:
            value = pd.DataFrame()

        _CSV_CACHE[path] = {
            "signature": signature,
            "value": value,
        }

        return value


# ============================================================
# VALUE CLEANING
# ============================================================

def clean_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    if (
        hasattr(value, "tolist")
        and not isinstance(
            value,
            (
                str,
                bytes,
                dict,
            ),
        )
    ):
        try:
            value = value.tolist()
        except Exception:
            pass

    if isinstance(value, float):
        if (
            math.isnan(value)
            or math.isinf(value)
        ):
            return None

    return value


def clean_record(record):
    if not isinstance(record, dict):
        return record

    return {
        key: clean_value(value)
        for key, value in record.items()
    }


def safe_float(value):
    if value is None:
        return None

    try:
        if isinstance(value, str):
            if not value.strip():
                return None

        value = float(value)

        if (
            math.isnan(value)
            or math.isinf(value)
        ):
            return None

        return value

    except Exception:
        return None


def safe_int(value):
    if value is None:
        return None

    try:
        if isinstance(value, str):
            if not value.strip():
                return None

        return int(float(value))

    except Exception:
        return None


def first_value(
    source,
    *keys,
    default=None,
):
    if not isinstance(source, dict):
        return default

    for key in keys:
        if key in source:
            value = source.get(key)

            if value is not None:
                return value

    return default


# ============================================================
# HISTORY
# ============================================================

def load_history(force_reload=False):
    if not os.path.exists(HISTORY_DIR):
        return pd.DataFrame()

    signature = get_directory_signature(
        HISTORY_DIR
    )

    with _CACHE_LOCK:

        if not force_reload:
            if (
                _HISTORY_CACHE["signature"]
                == signature
                and
                _HISTORY_CACHE["dataframe"]
                is not None
            ):
                return _HISTORY_CACHE["dataframe"]

        files = [
            item[0]
            for item in signature
        ]

        frames = []

        for path in files:
            try:
                frame = pd.read_parquet(path)

                if not frame.empty:
                    frames.append(frame)

            except Exception:
                continue

        if not frames:
            result = pd.DataFrame()

        else:
            try:
                result = pd.concat(
                    frames,
                    ignore_index=True,
                )
            except Exception:
                result = pd.DataFrame()

        _HISTORY_CACHE["signature"] = signature
        _HISTORY_CACHE["dataframe"] = result

        return result


def sort_history(df):
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    result = df.copy()

    if "record_index" in result.columns:

        result["_record_sort"] = pd.to_numeric(
            result["record_index"],
            errors="coerce",
        )

        result = result.sort_values(
            "_record_sort",
            ascending=True,
            na_position="last",
        )

        result = result.drop(
            columns=["_record_sort"],
            errors="ignore",
        )

        return result

    if "timestamp" in result.columns:

        result["_time_sort"] = pd.to_datetime(
            result["timestamp"],
            errors="coerce",
        )

        result = result.sort_values(
            "_time_sort",
            ascending=True,
            na_position="last",
        )

        result = result.drop(
            columns=["_time_sort"],
            errors="ignore",
        )

    return result


# ============================================================
# SYSTEM SUMMARY
# ============================================================

def get_system_summary(history=None):

    if history is None:
        history = load_history()

    df = sort_history(history)

    total = int(len(df))
    normal = 0
    anomalies = 0

    if not df.empty:

        if "status" in df.columns:

            status = (
                df["status"]
                .astype(str)
                .str.upper()
                .str.strip()
            )

            normal = int(
                (status == "NORMAL").sum()
            )

            anomalies = int(
                (status == "ANOMALY").sum()
            )

        elif "prediction" in df.columns:

            prediction = (
                df["prediction"]
                .astype(str)
                .str.upper()
                .str.strip()
            )

            anomalies = int(
                (prediction == "ANOMALY").sum()
            )

            normal = max(
                total - anomalies,
                0,
            )

    registry = safe_read_json(
        REGISTRY_PATH
    )

    pipeline = safe_read_json(
        PIPELINE_STATUS
    )

    return {
        "total_logs": total,
        "logs_processed": total,
        "normal_logs": normal,
        "anomalies_detected": anomalies,

        "current_production_version":
            first_value(
                registry,
                "current_production_version",
            ),

        "latest_candidate_version":
            first_value(
                registry,
                "latest_candidate_version",
            ),

        "last_model_decision":
            first_value(
                registry,
                "last_decision",
            ),

        "system_status": "RUNNING",

        "pipeline_status":
            first_value(
                pipeline,
                "status",
                default="WAITING",
            ),

        "pipeline_stage":
            first_value(
                pipeline,
                "stage",
                default="MONITORING",
            ),

        "timestamp": time.time(),
    }


# ============================================================
# ANOMALIES
# ============================================================

def get_anomalies(
    limit=ANOMALY_LIMIT,
    history=None,
):

    if history is None:
        history = load_history()

    if (
        history is None
        or history.empty
        or "status" not in history.columns
    ):
        return []

    work = history.copy()

    status = (
        work["status"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    work = work[
        status == "ANOMALY"
    ].copy()

    if work.empty:
        return []

    if "record_index" in work.columns:

        work["_sort_record"] = pd.to_numeric(
            work["record_index"],
            errors="coerce",
        )

        work = work.sort_values(
            "_sort_record",
            ascending=False,
            na_position="last",
        )

    elif "timestamp" in work.columns:

        work["_sort_time"] = pd.to_datetime(
            work["timestamp"],
            errors="coerce",
        )

        work = work.sort_values(
            "_sort_time",
            ascending=False,
            na_position="last",
        )

    work = work.head(limit)

    records = []

    for _, row in work.iterrows():

        features = row.get(
            "features",
            [],
        )

        if hasattr(features, "tolist"):
            try:
                features = features.tolist()
            except Exception:
                features = []

        if features is None:
            features = []

        try:
            feature_count = len(features)
        except Exception:
            feature_count = 0

        record = {
            "record_index":
                clean_value(
                    row.get("record_index")
                ),

            "timestamp":
                clean_value(
                    row.get("timestamp")
                ),

            "status": "ANOMALY",

            "if_score":
                safe_float(
                    row.get("if_score")
                ),

            "ae_score":
                safe_float(
                    row.get("ae_score")
                ),

            "final_score":
                safe_float(
                    row.get("final_score")
                ),

            "threshold":
                safe_float(
                    row.get("threshold")
                ),

            "feature_count":
                feature_count,
        }

        for field in [
            "source_ip",
            "destination_ip",
            "src_ip",
            "dst_ip",
            "protocol",
            "src_port",
            "dst_port",
            "label",
            "prediction",
            "attack_type",
        ]:

            if field in row.index:
                record[field] = clean_value(
                    row.get(field)
                )

        records.append(
            clean_record(record)
        )

    return records


# ============================================================
# ADWIN DRIFT
# ============================================================

def get_drift():

    df = safe_read_csv(
        ADWIN_RESULTS
    )

    latest_result = safe_read_json(
        ADWIN_LATEST
    )

    state = safe_read_json(
        ADWIN_STATE
    )

    trigger = safe_read_json(
        RETRAINING_TRIGGER
    )

    pipeline = safe_read_json(
        PIPELINE_STATUS
    )

    if df.empty:

        total_drifts = safe_int(
            first_value(
                latest_result,
                "total_drifts",
                "drift_count",
                default=0,
            )
        ) or 0

        new_drift_count = safe_int(
            first_value(
                pipeline,
                "new_drift_count",
                "new_drift_events",
                default=first_value(
                    latest_result,
                    "new_drift_count",
                    default=0,
                ),
            )
        ) or 0

        return {
            "available":
                bool(
                    latest_result
                    or state
                ),

            "total_drifts":
                total_drifts,

            "new_drift_count":
                new_drift_count,

            "latest_drift": None,

            "latest_result":
                clean_record(
                    latest_result
                ),

            "state":
                clean_record(state),

            "trigger":
                clean_record(trigger),

            "records": [],
        }

    drift_column = None

    for candidate in [
        "drift_detected",
        "drift",
        "is_drift",
        "change_detected",
    ]:

        if candidate in df.columns:
            drift_column = candidate
            break

    if drift_column is None:

        for column in df.columns:

            if "drift" in str(column).lower():
                drift_column = column
                break

    if drift_column is None:
        return {
            "available": True,
            "total_drifts": 0,
            "new_drift_count": 0,
            "latest_drift": None,
            "latest_result":
                clean_record(
                    latest_result
                ),
            "state":
                clean_record(state),
            "trigger":
                clean_record(trigger),
            "records": [],
        }

    values = (
        df[drift_column]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    drift_df = df[
        values.isin(
            [
                "true",
                "1",
                "yes",
                "detected",
            ]
        )
    ].copy()

    if "record_index" in drift_df.columns:

        drift_df["_record_sort"] = pd.to_numeric(
            drift_df["record_index"],
            errors="coerce",
        )

        drift_df = drift_df.sort_values(
            "_record_sort",
            ascending=True,
            na_position="last",
        )

    elif "timestamp" in drift_df.columns:

        drift_df["_time_sort"] = pd.to_datetime(
            drift_df["timestamp"],
            errors="coerce",
        )

        drift_df = drift_df.sort_values(
            "_time_sort",
            ascending=True,
            na_position="last",
        )

    records = [
        clean_record(row)
        for row in
        drift_df.tail(
            DRIFT_LIMIT
        ).to_dict(
            orient="records"
        )
    ]

    latest = (
        records[-1]
        if records
        else None
    )

    new_drift_count = safe_int(
        first_value(
            pipeline,
            "new_drift_count",
            "new_drift_events",
            default=first_value(
                latest_result,
                "new_drift_count",
                default=0,
            ),
        )
    ) or 0

    return {
        "available": True,

        "total_drifts":
            int(len(drift_df)),

        "new_drift_count":
            new_drift_count,

        "latest_drift":
            latest,

        "latest_result":
            clean_record(
                latest_result
            ),

        "state":
            clean_record(state),

        "trigger":
            clean_record(trigger),

        "records":
            records,
    }


# ============================================================
# VERSION HELPERS
# ============================================================

def find_version_directories(directory):

    versions = []

    if not os.path.exists(directory):
        return versions

    try:
        names = os.listdir(directory)
    except Exception:
        return versions

    for name in names:

        if not name.lower().startswith("v"):
            continue

        path = os.path.join(
            directory,
            name,
        )

        if not os.path.isdir(path):
            continue

        try:
            version = int(
                name[1:]
            )
        except Exception:
            continue

        versions.append(
            (
                version,
                path,
            )
        )

    versions.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    return versions


def latest_version(directory):

    versions = find_version_directories(
        directory
    )

    if not versions:
        return None

    return versions[0][0]


# ============================================================
# MODEL REGISTRY
# ============================================================

def get_models():

    registry = safe_read_json(
        REGISTRY_PATH
    )

    raw_models = registry.get(
        "models",
        [],
    )

    if not isinstance(raw_models, list):
        raw_models = []

    models = []

    for model in raw_models:

        if not isinstance(model, dict):
            continue

        item = dict(model)

        metrics = item.get(
            "metrics",
            {},
        )

        if not isinstance(metrics, dict):
            metrics = {}

        item["metrics"] = clean_record(
            metrics
        )

        changes = item.get(
            "metric_changes",
            {},
        )

        if isinstance(changes, dict):
            item["metric_changes"] = (
                clean_record(changes)
            )
        else:
            item["metric_changes"] = {}

        models.append(
            clean_record(item)
        )

    current_production = registry.get(
        "current_production_version"
    )

    if current_production in (
        None,
        "",
    ):
        current_production = latest_version(
            PRODUCTION_DIR
        )

    latest_candidate = registry.get(
        "latest_candidate_version"
    )

    if latest_candidate in (
        None,
        "",
    ):
        latest_candidate = latest_version(
            CANDIDATES_DIR
        )

    return {
        "current_production_version":
            current_production,

        "latest_candidate_version":
            latest_candidate,

        "previous_production_version":
            registry.get(
                "previous_production_version"
            ),

        "last_decision":
            registry.get(
                "last_decision"
            ),

        "last_updated":
            registry.get(
                "last_updated"
            ),

        "models":
            models,
    }


# ============================================================
# PRODUCTION MODEL METADATA
# ============================================================

def get_model_metadata(history=None):

    registry = safe_read_json(
        REGISTRY_PATH
    )

    current_version = safe_int(
        first_value(
            registry,
            "current_production_version",
        )
    )

    metadata = {}

    if current_version is not None:

        production_dir = os.path.join(
            PRODUCTION_DIR,
            f"v{current_version}",
        )

        metadata_path = os.path.join(
            production_dir,
            "model_metadata.json",
        )

        metadata = safe_read_json(
            metadata_path
        )

    # Legacy fallback
    if not metadata:

        metadata_path = os.path.join(
            SAVED_MODELS_DIR,
            "model_metadata.json",
        )

        metadata = safe_read_json(
            metadata_path
        )

    if not isinstance(metadata, dict):
        metadata = {}

    features = first_value(
        metadata,
        "feature_count",
        "input_dim",
        "features",
    )

    if features is None:
        features = 59

    threshold = first_value(
        metadata,
        "candidate_ae_threshold",
        "ae_threshold",
        "old_ae_threshold",
        "threshold",
    )

    return {
        "available":
            bool(metadata),

        "model_type":
            first_value(
                metadata,
                "model_type",
                "model",
                default=
                    "Isolation Forest + Autoencoder",
            ),

        "version":
            current_version
            if current_version is not None
            else first_value(
                metadata,
                "version",
                "production_version",
            ),

        "created":
            first_value(
                metadata,
                "created_at",
                "created",
                "promoted_at",
                "timestamp",
            ),

        "features":
            safe_int(features),

        "threshold":
            safe_float(threshold),

        "status":
            first_value(
                metadata,
                "status",
                default="ACTIVE",
            ),

        "metadata":
            clean_record(metadata),
    }


# ============================================================
# RETRAINING HELPERS
# ============================================================

def _latest_existing_file(paths):

    existing = [
        path
        for path in paths
        if os.path.exists(path)
    ]

    if not existing:
        return None

    existing.sort(
        key=file_mtime,
        reverse=True,
    )

    return existing[0]


def _find_latest_candidate_metadata():

    versions = []

    if not os.path.exists(
        CANDIDATES_DIR
    ):
        return None

    for name in os.listdir(
        CANDIDATES_DIR
    ):

        if not name.lower().startswith("v"):
            continue

        directory = os.path.join(
            CANDIDATES_DIR,
            name,
        )

        if not os.path.isdir(directory):
            continue

        try:
            version = int(name[1:])
        except Exception:
            continue

        metadata_path = os.path.join(
            directory,
            "candidate_training_metadata.json",
        )

        model_metadata_path = os.path.join(
            directory,
            "model_metadata.json",
        )

        metadata = safe_read_json(
            metadata_path
        )

        if not metadata:
            metadata = safe_read_json(
                model_metadata_path
            )

        versions.append(
            (
                version,
                directory,
                metadata,
            )
        )

    if not versions:
        return None

    versions.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    return versions[0]


# ============================================================
# RETRAINING
# ============================================================

def get_retraining(history=None):

    if history is None:
        history = load_history()

    registry = safe_read_json(
        REGISTRY_PATH
    )

    current_version = safe_int(
        first_value(
            registry,
            "current_production_version",
        )
    )

    candidate_version = safe_int(
        first_value(
            registry,
            "latest_candidate_version",
        )
    )

    candidate_info = (
        _find_latest_candidate_metadata()
    )

    candidate_metadata = {}
    candidate_dir = None

    if candidate_info:

        (
            discovered_version,
            candidate_dir,
            candidate_metadata,
        ) = candidate_info

        if candidate_version is None:
            candidate_version = discovered_version

    report_path = _latest_existing_file(
        [
            PYSPARK_REPORT,
            PHASE11_REPORT,
        ]
    )

    report = pd.DataFrame()

    if report_path:
        report = safe_read_csv(
            report_path
        )

    latest_report = {}

    if not report.empty:
        latest_report = clean_record(
            report.iloc[-1].to_dict()
        )

    # IMPORTANT:
    # These are training records, NOT evaluation records.
    training_records = first_value(
        candidate_metadata,
        "training_records",
        "records_used",
        "training_samples",
        "normal_records",
    )

    if training_records is None:
        training_records = first_value(
            latest_report,
            "training_records",
            "records_used",
            "training_samples",
            "normal_records",
        )

    training_records = safe_int(
        training_records
    )

    training_time = first_value(
        candidate_metadata,
        "training_time_seconds",
        "training_time",
        "elapsed_seconds",
    )

    if training_time is None:
        training_time = first_value(
            latest_report,
            "training_time_seconds",
            "training_time",
            "elapsed_seconds",
        )

    training_time = safe_float(
        training_time
    )

    candidate_path = None

    if candidate_version is not None:
        candidate_path = os.path.join(
            CANDIDATES_DIR,
            f"v{candidate_version}",
        )

    retraining_timestamp = first_value(
        candidate_metadata,
        "timestamp",
        "created_at",
        "trained_at",
    )

    if retraining_timestamp is None:
        retraining_timestamp = first_value(
            latest_report,
            "timestamp",
            "created_at",
            "trained_at",
        )

    available = (
        bool(candidate_metadata)
        or not report.empty
    )

    return {
        "available":
            available,

        "source":
            "candidate_training_metadata"
            if candidate_metadata
            else (
                "pyspark_retraining_report"
                if not report.empty
                else None
            ),

        "source_path":
            candidate_dir
            if candidate_dir
            else report_path,

        "metadata_source":
            (
                os.path.join(
                    candidate_dir,
                    "candidate_training_metadata.json",
                )
                if (
                    candidate_dir
                    and
                    os.path.exists(
                        os.path.join(
                            candidate_dir,
                            "candidate_training_metadata.json",
                        )
                    )
                )
                else None
            ),

        "current_production_version":
            current_version,

        "latest_candidate_version":
            candidate_version,

        "training_records":
            training_records,

        "normal_records_available":
            training_records,

        "records":
            training_records,

        "training_time":
            training_time,

        "training_time_seconds":
            training_time,

        "timestamp":
            retraining_timestamp,

        "latest":
            latest_report,

        "metadata":
            clean_record(
                candidate_metadata
            ),
    }


# ============================================================
# MODEL EVALUATION
# ============================================================

def get_model_evaluation():

    comparison = safe_read_csv(
        PHASE12_COMPARISON
    )

    promotion = safe_read_csv(
        PHASE12_PROMOTION
    )

    metadata = safe_read_json(
        PHASE12_METADATA
    )

    comparison_records = []

    if not comparison.empty:
        comparison_records = [
            clean_record(row)
            for row in comparison.to_dict(
                orient="records"
            )
        ]

    promotion_records = []

    if not promotion.empty:
        promotion_records = [
            clean_record(row)
            for row in promotion.to_dict(
                orient="records"
            )
        ]

    decision = first_value(
        metadata,
        "decision",
        default=(
            str(
                promotion_records[-1].get(
                    "decision"
                )
            ).upper()
            if promotion_records
            else None
        ),
    )

    old_metrics = first_value(
        metadata,
        "old_metrics",
        default={},
    )

    candidate_metrics = first_value(
        metadata,
        "candidate_metrics",
        default={},
    )

    metric_changes = first_value(
        metadata,
        "metric_changes",
        default={},
    )

    return {
        "available":
            bool(
                metadata
                or comparison_records
                or promotion_records
            ),

        "decision":
            decision,

        "reason":
            first_value(
                metadata,
                "decision_reason",
                "reason",
            ),

        "old_production_version":
            safe_int(
                first_value(
                    metadata,
                    "old_production_version",
                )
            ),

        "candidate_version":
            safe_int(
                first_value(
                    metadata,
                    "candidate_version",
                )
            ),

        "comparison":
            comparison_records,

        "promotion":
            promotion_records,

        "metadata":
            clean_record(metadata),

        "old_metrics":
            clean_record(old_metrics),

        "candidate_metrics":
            clean_record(candidate_metrics),

        "metric_changes":
            clean_record(metric_changes),

        "evaluation_time":
            safe_float(
                first_value(
                    metadata,
                    "evaluation_time",
                    "total_evaluation_time",
                )
            ),

        # Phase 12 does not calculate ROC-AUC.
        "roc_auc":
            first_value(
                metadata,
                "roc_auc",
                default=None,
            ),
    }


# ============================================================
# VERSION MANAGEMENT
# ============================================================

def get_version_management():

    registry = safe_read_json(
        REGISTRY_PATH
    )

    current_version = safe_int(
        first_value(
            registry,
            "current_production_version",
        )
    )

    production_versions = []
    candidate_versions = []
    archive_versions = []

    for version, path in find_version_directories(
        PRODUCTION_DIR
    ):

        production_versions.append(
            {
                "version": version,
                "path": path,
                "status":
                    (
                        "ACTIVE"
                        if version == current_version
                        else "INACTIVE"
                    ),
            }
        )

    for version, path in find_version_directories(
        CANDIDATES_DIR
    ):

        candidate_versions.append(
            {
                "version": version,
                "path": path,
                "status": "CANDIDATE",
            }
        )

    for version, path in find_version_directories(
        ARCHIVE_DIR
    ):

        archive_versions.append(
            {
                "version": version,
                "path": path,
                "status": "ARCHIVED",
            }
        )

    return {
        "available":
            bool(
                registry
                or production_versions
                or candidate_versions
                or archive_versions
            ),

        "current_production_version":
            current_version,

        "previous_production_version":
            safe_int(
                first_value(
                    registry,
                    "previous_production_version",
                )
            ),

        "latest_candidate_version":
            safe_int(
                first_value(
                    registry,
                    "latest_candidate_version",
                )
            ),

        "last_decision":
            first_value(
                registry,
                "last_decision",
            ),

        "production_versions":
            production_versions,

        "candidate_versions":
            candidate_versions,

        "archive_versions":
            archive_versions,

        "registry":
            clean_record(registry),
    }


# ============================================================
# TOP MODEL
# ============================================================

def get_top_model():

    registry = safe_read_json(
        REGISTRY_PATH
    )

    current_version = safe_int(
        first_value(
            registry,
            "current_production_version",
        )
    )

    models = registry.get(
        "models",
        [],
    )

    if not isinstance(models, list):
        models = []

    selected = None

    for model in models:

        if not isinstance(model, dict):
            continue

        if (
            safe_int(
                model.get("version")
            )
            == current_version
        ):
            selected = model
            break

    if selected is None:

        production_metadata = {}

        if current_version is not None:

            production_metadata = safe_read_json(
                os.path.join(
                    PRODUCTION_DIR,
                    f"v{current_version}",
                    "model_metadata.json",
                )
            )

        return {
            "available":
                bool(production_metadata),

            "name":
                first_value(
                    production_metadata,
                    "model_type",
                    default=
                        "Isolation Forest + Autoencoder",
                ),

            "version":
                current_version,

            "score": None,

            "status": "ACTIVE",
        }

    metrics = selected.get(
        "metrics",
        {},
    )

    return {
        "available": True,

        "name":
            first_value(
                selected,
                "model_type",
                default=
                    "Isolation Forest + Autoencoder",
            ),

        "version":
            current_version,

        "score":
            safe_float(
                first_value(
                    metrics,
                    "f1",
                    "accuracy",
                )
            ),

        "status":
            first_value(
                selected,
                "status",
                default="ACTIVE",
            ),
    }


# ============================================================
# PIPELINE STATUS
# ============================================================

def get_pipeline_status(history=None):

    status = safe_read_json(
        PIPELINE_STATUS
    )

    registry = safe_read_json(
        REGISTRY_PATH
    )

    retraining = get_retraining(
        history=history
    )

    evaluation = get_model_evaluation()

    current_production = safe_int(
        first_value(
            registry,
            "current_production_version",
        )
    )

    latest_candidate = safe_int(
        first_value(
            registry,
            "latest_candidate_version",
        )
    )

    lifecycle = first_value(
        status,
        "lifecycle",
        default="WAITING",
    )

    decision = first_value(
        status,
        "decision",
        default=first_value(
            registry,
            "last_decision",
        ),
    )

    return {
        "available":
            bool(status),

        "status":
            first_value(
                status,
                "status",
                default="WAITING",
            ),

        "stage":
            first_value(
                status,
                "stage",
                default="MONITORING",
            ),

        "message":
            first_value(
                status,
                "message",
                default="",
            ),

        "lifecycle":
            lifecycle,

        "decision":
            decision,

        "reason":
            first_value(
                status,
                "reason",
                default=evaluation.get("reason"),
            ),

        "production_version":
            first_value(
                status,
                "production_version",
                default=current_production,
            ),

        "candidate_version":
            first_value(
                status,
                "candidate_version",
                default=latest_candidate,
            ),

        "current_production_version":
            current_production,

        "latest_candidate_version":
            latest_candidate,

        "retraining":
            retraining,

        "evaluation":
            evaluation,

        "last_updated":
            first_value(
                status,
                "last_updated",
            ),

        "raw":
            status,
    }


# ============================================================
# SELF CORRECTION
# Compatibility only.
# Canonical controller is automatic_retraining_orchestrator.py
# ============================================================

def get_self_correction():

    state = safe_read_json(
        SELF_CORRECTION
    )

    automatic = safe_read_json(
        AUTOMATIC_STATE
    )

    if not state and not automatic:

        return {
            "available": False,
            "enabled": False,
            "status": "INACTIVE",
            "state": {},
            "automatic_state": {},
        }

    return {
        "available": True,

        "enabled":
            bool(
                first_value(
                    automatic,
                    "enabled",
                    default=True,
                )
            ),

        "status":
            first_value(
                automatic,
                "status",
                default=first_value(
                    state,
                    "status",
                    default="ACTIVE",
                ),
            ),

        "state":
            clean_record(state),

        "automatic_state":
            clean_record(automatic),
    }


# ============================================================
# PIPELINE LOG
# ============================================================

def get_pipeline_log(
    limit=PIPELINE_LOG_LIMIT
):

    if not os.path.exists(
        PIPELINE_LOG
    ):
        return []

    try:
        with open(
            PIPELINE_LOG,
            "r",
            encoding="utf-8",
            errors="replace",
        ) as file:

            lines = file.readlines()

    except Exception:
        return []

    lines = lines[-limit:]

    return [
        line.rstrip()
        for line in lines
    ]


# ============================================================
# COMPLETE DASHBOARD DATA
# ============================================================

def get_dashboard_data():

    # --------------------------------------------------------
    # ONE HISTORY SNAPSHOT
    # --------------------------------------------------------

    history = load_history()

    # --------------------------------------------------------
    # Build all sections from the same snapshot
    # --------------------------------------------------------

    summary = get_system_summary(
        history=history
    )

    anomalies = get_anomalies(
        history=history
    )

    drift = get_drift()

    retraining = get_retraining(
        history=history
    )

    models = get_models()

    model_metadata = get_model_metadata(
        history=history
    )

    top_model = get_top_model()

    pipeline_status = get_pipeline_status(
        history=history
    )

    self_correction = get_self_correction()

    pipeline_log = get_pipeline_log()

    evaluation = get_model_evaluation()

    version_management = (
        get_version_management()
    )

    return {
        "timestamp":
            time.time(),

        "summary":
            summary,

        "system":
            summary,

        "anomalies":
            anomalies,

        "anomaly_count":
            len(anomalies),

        "drift":
            drift,

        "retraining":
            retraining,

        "models":
            models,

        "model_metadata":
            model_metadata,

        "top_model":
            top_model,

        "pipeline":
            pipeline_status,

        "pipeline_status":
            pipeline_status,

        "self_correction":
            self_correction,

        "pipeline_log":
            pipeline_log,

        "evaluation":
            evaluation,

        "model_evaluation":
            evaluation,

        "version_management":
            version_management,
    }


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":

    print("=" * 80)
    print("DASHBOARD DATA MODULE TEST")
    print("=" * 80)

    data = get_dashboard_data()

    print()
    print("Dashboard data generated successfully.")

    print()
    print("System summary:")
    print(
        json.dumps(
            data.get(
                "summary",
                {},
            ),
            indent=4,
            default=str,
        )
    )

    print()
    print("Models:")
    print(
        json.dumps(
            data.get(
                "models",
                {},
            ),
            indent=4,
            default=str,
        )
    )

    print()
    print("Retraining:")
    print(
        json.dumps(
            data.get(
                "retraining",
                {},
            ),
            indent=4,
            default=str,
        )
    )

    print()
    print("Evaluation:")
    print(
        json.dumps(
            data.get(
                "evaluation",
                {},
            ),
            indent=4,
            default=str,
        )
    )

    print()
    print("Pipeline:")
    print(
        json.dumps(
            data.get(
                "pipeline",
                {},
            ),
            indent=4,
            default=str,
        )
    )

    print()
    print("Dashboard data test complete.")

    print("=" * 80)