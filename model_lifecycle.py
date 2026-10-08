import os
import json
import shutil
import time


# ============================================================
# PHASE 14 - AUTOMATIC MODEL PROMOTION & ROLLBACK
# ============================================================
#
# Pipeline:
#
# ADWIN
#   ↓
# PySpark Retraining
#   ↓
# Model Evaluation
#   ↓
# Model Lifecycle
#
# Responsibilities:
#
#   1. Read Phase 12 evaluation decision.
#   2. Verify the exact evaluated candidate.
#   3. Promote candidate if ACCEPTED.
#   4. Keep production unchanged if REJECTED.
#   5. Archive previous production.
#   6. Create a new production/vN directory.
#   7. Update model_registry.json.
#   8. Update automatic_pipeline_status.json.
#
# IMPORTANT:
#
# model_lifecycle.py is the ONLY component that promotes models.
#
# model_evaluation.py:
#       evaluates only
#
# pyspark_retraining.py:
#       trains candidate only
#
# inference_worker.py:
#       reads current production from registry
#
# ============================================================


print("=" * 90)
print("PHASE 14 - AUTOMATIC MODEL PROMOTION & ROLLBACK")
print("=" * 90)


# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
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

ARCHIVE_DIR = os.path.join(
    MODEL_DIR,
    "archive"
)

REGISTRY_FILE = os.path.join(
    MODEL_DIR,
    "model_registry.json"
)

PHASE12_METADATA = os.path.join(
    BASE_DIR,
    "data",
    "processed",
    "phase12_model_metadata.json"
)

PIPELINE_STATUS_FILE = os.path.join(
    BASE_DIR,
    "data",
    "processed",
    "automatic_pipeline_status.json"
)


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
# HELPERS
# ============================================================

def load_json(path):

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(
            file
        )


def save_json(path, data):

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


def safe_value(value):

    if value is None:
        return None

    try:

        if hasattr(
            value,
            "item"
        ):

            return value.item()

    except Exception:
        pass

    return value


def ensure_directories():

    os.makedirs(
        PRODUCTION_DIR,
        exist_ok=True
    )

    os.makedirs(
        CANDIDATES_DIR,
        exist_ok=True
    )

    os.makedirs(
        ARCHIVE_DIR,
        exist_ok=True
    )


# ============================================================
# PIPELINE STATUS
# ============================================================

def update_pipeline_status(

    status=None,

    stage=None,

    message=None,

    lifecycle=None,

    decision=None,

    production_version=None,

    candidate_version=None,

    reason=None,

    evaluation=None

):

    existing = {}

    if os.path.exists(
        PIPELINE_STATUS_FILE
    ):

        try:

            existing = load_json(
                PIPELINE_STATUS_FILE
            )

        except Exception:

            existing = {}


    if status is not None:

        existing[
            "status"
        ] = status


    if stage is not None:

        existing[
            "stage"
        ] = stage


    if message is not None:

        existing[
            "message"
        ] = message


    if lifecycle is not None:

        existing[
            "lifecycle"
        ] = lifecycle


    if decision is not None:

        existing[
            "decision"
        ] = decision


    if production_version is not None:

        existing[
            "production_version"
        ] = production_version


    if candidate_version is not None:

        existing[
            "candidate_version"
        ] = candidate_version


    if reason is not None:

        existing[
            "reason"
        ] = reason


    if evaluation is not None:

        existing[
            "evaluation"
        ] = evaluation


    existing[
        "last_updated"
    ] = time.time()


    save_json(
        PIPELINE_STATUS_FILE,
        existing
    )


# ============================================================
# FIND VERSION DIRECTORIES
# ============================================================

def find_versions(directory):

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
# FIND ACTIVE PRODUCTION
# ============================================================

def find_active_production():

    versions = find_versions(
        PRODUCTION_DIR
    )


    if not versions:

        return None


    return versions[-1]


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
# FIND SPECIFIC VERSION
# ============================================================

def find_candidate_version(
    version
):

    if version is None:

        return None


    candidate_dir = os.path.join(

        CANDIDATES_DIR,

        f"v{version}"

    )


    if not os.path.isdir(
        candidate_dir
    ):

        return None


    return (
        int(version),
        candidate_dir
    )


# ============================================================
# REGISTRY
# ============================================================

def create_or_load_registry():

    if os.path.exists(
        REGISTRY_FILE
    ):

        try:

            registry = load_json(
                REGISTRY_FILE
            )


            if "models" not in registry:

                registry[
                    "models"
                ] = []


            return registry


        except Exception as exc:

            print(
                "WARNING: Could not load "
                f"existing registry: {exc}"
            )


    return {

        "current_production_version":
            None,

        "previous_production_version":
            None,

        "latest_candidate_version":
            None,

        "last_decision":
            None,

        "last_updated":
            time.time(),

        "models":
            []

    }


# ============================================================
# UPDATE MODEL STATUS IN REGISTRY
# ============================================================

def update_registry_model_status(

    registry,

    version,

    status

):

    if version is None:

        return


    found = False


    for model in registry.get(
        "models",
        []
    ):

        try:

            model_version = int(
                model.get(
                    "version"
                )
            )

        except Exception:

            continue


        if model_version == int(
            version
        ):

            model[
                "status"
            ] = status

            model[
                "status_updated_at"
            ] = time.time()

            found = True


    return found


# ============================================================
# REGISTER MODEL
# ============================================================

def register_model(

    registry,

    version,

    model_type,

    status,

    metadata=None,

    metrics=None,

    reason=None

):

    # Remove an existing entry for the same version.
    #
    # This prevents duplicate registry entries.

    registry[
        "models"
    ] = [

        model

        for model in registry.get(
            "models",
            []
        )

        if str(
            model.get(
                "version"
            )
        ) != str(version)

    ]


    entry = {

        "version":
            int(version),

        "model_type":
            model_type,

        "status":
            status,

        "timestamp":
            time.time()

    }


    if metadata is not None:

        entry[
            "metadata"
        ] = metadata


    if metrics is not None:

        entry[
            "metrics"
        ] = metrics


    if reason is not None:

        entry[
            "reason"
        ] = reason


    registry[
        "models"
    ].append(
        entry
    )


# ============================================================
# VERIFY MODEL DIRECTORY
# ============================================================

def verify_model_directory(

    directory,

    label

):

    if not os.path.isdir(
        directory
    ):

        return False


    print(
        f"\nVerifying {label}:"
    )


    valid = True


    for filename in MODEL_FILES:

        path = os.path.join(

            directory,

            filename

        )


        if os.path.exists(
            path
        ):

            print(
                f"  OK: {filename}"
            )

        else:

            print(
                f"  MISSING: {filename}"
            )

            valid = False


    return valid


# ============================================================
# COPY MODEL DIRECTORY
# ============================================================

def copy_model_directory(

    source_dir,

    destination_dir

):

    os.makedirs(

        destination_dir,

        exist_ok=True

    )


    for filename in MODEL_FILES:

        source = os.path.join(

            source_dir,

            filename

        )


        destination = os.path.join(

            destination_dir,

            filename

        )


        shutil.copy2(

            source,

            destination

        )


        print(
            f"  Copied: {filename}"
        )


# ============================================================
# START
# ============================================================

ensure_directories()


# ============================================================
# INITIAL PIPELINE STATUS
# ============================================================

update_pipeline_status(

    status="RUNNING",

    stage="LIFECYCLE",

    message=(
        "Model lifecycle decision started."
    ),

    lifecycle="RUNNING",

    decision="PENDING"

)


# ============================================================
# LOAD REGISTRY
# ============================================================

registry = (
    create_or_load_registry()
)


# ============================================================
# LOAD PHASE 12
# ============================================================

if not os.path.exists(
    PHASE12_METADATA
):

    message = (

        "Phase 12 metadata not found. "
        "Model lifecycle cannot continue."

    )


    update_pipeline_status(

        status="ERROR",

        stage="LIFECYCLE",

        message=message,

        lifecycle="ERROR",

        decision="ERROR",

        reason=message

    )


    print()
    print(
        "ERROR:",
        message
    )

    print(
        PHASE12_METADATA
    )

    raise SystemExit(1)


try:

    phase12 = load_json(
        PHASE12_METADATA
    )

except Exception as exc:

    message = (

        "Could not read Phase 12 metadata: "
        f"{exc}"

    )


    update_pipeline_status(

        status="ERROR",

        stage="LIFECYCLE",

        message=message,

        lifecycle="ERROR",

        decision="ERROR",

        reason=message

    )


    print(
        "ERROR:",
        message
    )

    raise SystemExit(1)


# ============================================================
# READ PHASE 12 DECISION
# ============================================================

decision = str(

    phase12.get(
        "decision",
        "UNKNOWN"
    )

).upper()


decision_reason = phase12.get(

    "decision_reason",

    "No decision reason provided."

)


evaluated_candidate_version = (
    phase12.get(
        "candidate_version"
    )
)


evaluated_production_version = (
    phase12.get(
        "old_production_version"
    )
)


try:

    if evaluated_candidate_version is not None:

        evaluated_candidate_version = int(
            evaluated_candidate_version
        )

except Exception:

    evaluated_candidate_version = None


try:

    if evaluated_production_version is not None:

        evaluated_production_version = int(
            evaluated_production_version
        )

except Exception:

    evaluated_production_version = None


print()
print(
    "Phase 12 decision:",
    decision
)

print(
    "Phase 12 production:",
    evaluated_production_version
)

print(
    "Phase 12 candidate:",
    evaluated_candidate_version
)


# ============================================================
# FIND CURRENT PRODUCTION
# ============================================================

production = (
    find_active_production()
)


if production:

    current_version, current_dir = (
        production
    )

    print(
        "Current production:",
        f"v{current_version}"
    )

else:

    current_version = None
    current_dir = None

    print(
        "Current production: NONE"
    )


# ============================================================
# FIND EXACT CANDIDATE EVALUATED BY PHASE 12
# ============================================================

candidate = (
    find_candidate_version(
        evaluated_candidate_version
    )
)


if candidate:

    candidate_version, candidate_dir = (
        candidate
    )

    print(
        "Evaluated candidate:",
        f"v{candidate_version}"
    )

else:

    candidate_version = None
    candidate_dir = None

    print(
        "Evaluated candidate: NONE"
    )


# ============================================================
# EVALUATION SUMMARY
# ============================================================

candidate_metrics = phase12.get(

    "candidate_metrics",

    {}

)


old_metrics = phase12.get(

    "old_metrics",

    {}

)


metric_changes = phase12.get(

    "metric_changes",

    {}

)


evaluation_summary = {

    "evaluation_records":
        safe_value(
            phase12.get(
                "evaluation_records"
            )
        ),

    "old_metrics":
        old_metrics,

    "candidate_metrics":
        candidate_metrics,

    "metric_changes":
        metric_changes,

    "evaluation_time":
        safe_value(
            phase12.get(
                "evaluation_time"
            )
        ),

    "old_production_version":
        evaluated_production_version,

    "candidate_version":
        evaluated_candidate_version

}


# ============================================================
# SAFETY CHECK:
# EVALUATED PRODUCTION MUST MATCH CURRENT PRODUCTION
# ============================================================

if (

    evaluated_production_version is not None

    and

    current_version is not None

    and

    evaluated_production_version
    !=
    current_version

):

    message = (

        "Production version changed after evaluation. "
        f"Phase 12 evaluated v{evaluated_production_version}, "
        f"but current production is v{current_version}. "
        "Promotion cancelled for safety."

    )


    update_pipeline_status(

        status="ERROR",

        stage="LIFECYCLE",

        message=message,

        lifecycle="ERROR",

        decision=decision,

        production_version=
            current_version,

        candidate_version=
            evaluated_candidate_version,

        reason=message,

        evaluation=
            evaluation_summary

    )


    print()
    print(
        "ERROR:",
        message
    )

    raise SystemExit(1)


# ============================================================
# REJECTED CANDIDATE
# ============================================================

if decision != "ACCEPTED":

    print()
    print(
        "=" * 90
    )

    print(
        "PROMOTION DECISION: REJECTED"
    )

    print(
        "=" * 90
    )


    print()
    print(
        "Reason:",
        decision_reason
    )


    print(
        "Production model will remain unchanged."
    )


    if candidate_version is not None:

        print(
            f"Candidate v{candidate_version} "
            "remains preserved."
        )


    registry[
        "current_production_version"
    ] = current_version


    registry[
        "latest_candidate_version"
    ] = evaluated_candidate_version


    registry[
        "last_decision"
    ] = "REJECTED"


    registry[
        "last_updated"
    ] = time.time()


    if current_version is not None:

        update_registry_model_status(

            registry,

            current_version,

            "ACTIVE"

        )


    if candidate_version is not None:

        update_registry_model_status(

            registry,

            candidate_version,

            "REJECTED"

        )


    save_json(

        REGISTRY_FILE,

        registry

    )


    update_pipeline_status(

        status="COMPLETED",

        stage="LIFECYCLE",

        message=(

            "Candidate rejected. "
            "Production model retained."

        ),

        lifecycle="REJECTED",

        decision="REJECTED",

        production_version=
            current_version,

        candidate_version=
            evaluated_candidate_version,

        reason=
            decision_reason,

        evaluation=
            evaluation_summary

    )


    print()
    print(
        "Registry updated."
    )

    print(
        "Phase 14 completed safely."
    )

    print(
        "=" * 90
    )

    raise SystemExit(0)


# ============================================================
# ACCEPTED BUT CANDIDATE MISSING
# ============================================================

if candidate_dir is None:

    message = (

        "Phase 12 accepted candidate "
        f"v{evaluated_candidate_version}, "
        "but that exact candidate directory "
        "does not exist. Promotion cancelled."

    )


    update_pipeline_status(

        status="ERROR",

        stage="LIFECYCLE",

        message=message,

        lifecycle="ERROR",

        decision="ACCEPTED",

        production_version=
            current_version,

        candidate_version=
            evaluated_candidate_version,

        reason=message,

        evaluation=
            evaluation_summary

    )


    print()
    print(
        "ERROR:",
        message
    )

    raise SystemExit(1)


# ============================================================
# ACCEPTED BUT CANDIDATE MUST BE NEWER
# ============================================================

if (

    current_version is not None

    and

    candidate_version
    <=
    current_version

):

    message = (

        f"Candidate v{candidate_version} "
        f"is not newer than current production "
        f"v{current_version}. "
        "Promotion cancelled."

    )


    update_pipeline_status(

        status="ERROR",

        stage="LIFECYCLE",

        message=message,

        lifecycle="ERROR",

        decision="ACCEPTED",

        production_version=
            current_version,

        candidate_version=
            candidate_version,

        reason=message,

        evaluation=
            evaluation_summary

    )


    print()
    print(
        "ERROR:",
        message
    )

    raise SystemExit(1)


# ============================================================
# VERIFY CANDIDATE
# ============================================================

print()
print(
    "=" * 90
)

print(
    "VERIFYING ACCEPTED CANDIDATE"
)

print(
    "=" * 90
)


update_pipeline_status(

    status="RUNNING",

    stage="LIFECYCLE",

    message=(
        f"Verifying candidate v{candidate_version}."
    ),

    lifecycle="VERIFYING",

    decision="ACCEPTED",

    production_version=
        current_version,

    candidate_version=
        candidate_version,

    evaluation=
        evaluation_summary

)


candidate_valid = (
    verify_model_directory(

        candidate_dir,

        f"CANDIDATE v{candidate_version}"

    )
)


if not candidate_valid:

    message = (

        f"Candidate v{candidate_version} "
        "failed required file validation. "
        "Promotion cancelled."

    )


    update_pipeline_status(

        status="ERROR",

        stage="LIFECYCLE",

        message=message,

        lifecycle="ERROR",

        decision="ACCEPTED",

        production_version=
            current_version,

        candidate_version=
            candidate_version,

        reason=message,

        evaluation=
            evaluation_summary

    )


    print(
        "ERROR:",
        message
    )

    raise SystemExit(1)


# ============================================================
# DETERMINE NEW PRODUCTION VERSION
# ============================================================

if current_version is None:

    new_version = 1

else:

    new_version = (
        current_version + 1
    )


new_production_dir = os.path.join(

    PRODUCTION_DIR,

    f"v{new_version}"

)


# Never overwrite an existing production version.

if os.path.exists(
    new_production_dir
):

    message = (

        f"Production directory "
        f"v{new_version} already exists. "
        "Promotion cancelled to prevent overwrite."

    )


    update_pipeline_status(

        status="ERROR",

        stage="LIFECYCLE",

        message=message,

        lifecycle="ERROR",

        decision="ACCEPTED",

        production_version=
            current_version,

        candidate_version=
            candidate_version,

        reason=message,

        evaluation=
            evaluation_summary

    )


    print(
        "ERROR:",
        message
    )

    raise SystemExit(1)


print()
print(
    "New production version:",
    f"v{new_version}"
)


# ============================================================
# ARCHIVE CURRENT PRODUCTION
# ============================================================

archive_version_dir = None


if current_dir is not None:

    print()
    print(
        "=" * 90
    )

    print(
        "ARCHIVING CURRENT PRODUCTION"
    )

    print(
        "=" * 90
    )


    archive_version_dir = os.path.join(

        ARCHIVE_DIR,

        f"v{current_version}"

    )


    if os.path.exists(
        archive_version_dir
    ):

        archive_version_dir = os.path.join(

            ARCHIVE_DIR,

            f"v{current_version}_"
            f"{int(time.time())}"

        )


    shutil.copytree(

        current_dir,

        archive_version_dir

    )


    print(
        f"Production v{current_version} archived."
    )


    print(
        archive_version_dir
    )


# ============================================================
# PROMOTION
# ============================================================

print()
print(
    "=" * 90
)

print(
    "PROMOTING CANDIDATE"
)

print(
    "=" * 90
)


update_pipeline_status(

    status="RUNNING",

    stage="LIFECYCLE",

    message=(

        f"Promoting candidate v{candidate_version} "
        f"to production v{new_version}."

    ),

    lifecycle="PROMOTING",

    decision="ACCEPTED",

    production_version=
        current_version,

    candidate_version=
        candidate_version,

    evaluation=
        evaluation_summary

)


try:

    os.makedirs(

        new_production_dir,

        exist_ok=False

    )


    # --------------------------------------------------------
    # Copy candidate artifacts
    # --------------------------------------------------------

    for filename in MODEL_FILES:

        source = os.path.join(

            candidate_dir,

            filename

        )


        destination = os.path.join(

            new_production_dir,

            filename

        )


        shutil.copy2(

            source,

            destination

        )


        print(
            f"Copied: {filename}"
        )


except Exception as exc:

    # --------------------------------------------------------
    # Roll back partially created production directory
    # --------------------------------------------------------

    if os.path.exists(
        new_production_dir
    ):

        shutil.rmtree(
            new_production_dir
        )


    message = (

        "Promotion failed while copying "
        f"candidate files: {exc}"

    )


    update_pipeline_status(

        status="ERROR",

        stage="LIFECYCLE",

        message=message,

        lifecycle="ERROR",

        decision="ACCEPTED",

        production_version=
            current_version,

        candidate_version=
            candidate_version,

        reason=message,

        evaluation=
            evaluation_summary

    )


    print(
        "ERROR:",
        message
    )

    raise SystemExit(1)


# ============================================================
# PRODUCTION METADATA
# ============================================================

production_metadata = {

    "version":
        int(new_version),

    "status":
        "ACTIVE",

    "previous_production_version":
        current_version,

    "candidate_version":
        candidate_version,

    "promotion_decision":
        "ACCEPTED",

    "promotion_reason":
        decision_reason,

    "promoted_at":
        time.time(),

    "evaluation_records":
        phase12.get(
            "evaluation_records"
        ),

    "old_metrics":
        old_metrics,

    "candidate_metrics":
        candidate_metrics,

    "metric_changes":
        metric_changes,

    "if_weight":
        phase12.get(
            "if_weight"
        ),

    "ae_weight":
        phase12.get(
            "ae_weight"
        ),

    "fusion_threshold":
        phase12.get(
            "fusion_threshold"
        ),

    "old_ae_threshold":
        phase12.get(
            "old_ae_threshold"
        ),

    "candidate_ae_threshold":
        phase12.get(
            "candidate_ae_threshold"
        ),

    "evaluation_time":
        phase12.get(
            "evaluation_time"
        ),

    "trigger":
        phase12.get(
            "trigger"
        ),

    "lifecycle_owner":
        "model_lifecycle.py"

}


save_json(

    os.path.join(

        new_production_dir,

        "model_metadata.json"

    ),

    production_metadata

)


# ============================================================
# UPDATE REGISTRY
# ============================================================

# Previous production becomes ARCHIVED.

if current_version is not None:

    update_registry_model_status(

        registry,

        current_version,

        "ARCHIVED"

    )


# Candidate becomes PROMOTED.

update_registry_model_status(

    registry,

    candidate_version,

    "PROMOTED"

)


# Register the new production version.

register_model(

    registry,

    new_version,

    "Isolation Forest + Autoencoder",

    "ACTIVE",

    metadata=production_metadata,

    metrics=candidate_metrics,

    reason=decision_reason

)


# Current production.

registry[
    "current_production_version"
] = new_version


# Previous production.

registry[
    "previous_production_version"
] = current_version


# Latest evaluated candidate.

registry[
    "latest_candidate_version"
] = candidate_version


# Last decision.

registry[
    "last_decision"
] = "ACCEPTED"


registry[
    "last_updated"
] = time.time()


save_json(

    REGISTRY_FILE,

    registry

)


# ============================================================
# FINAL PIPELINE STATUS
# ============================================================

update_pipeline_status(

    status="COMPLETED",

    stage="LIFECYCLE",

    message=(

        f"Candidate v{candidate_version} "
        f"promoted to production v{new_version}."

    ),

    lifecycle="PROMOTED",

    decision="ACCEPTED",

    production_version=
        new_version,

    candidate_version=
        candidate_version,

    reason=
        decision_reason,

    evaluation=
        evaluation_summary

)


# ============================================================
# FINAL OUTPUT
# ============================================================

print()
print(
    "=" * 90
)

print(
    "MODEL PROMOTION SUCCESSFUL"
)

print(
    "=" * 90
)


if current_version is not None:

    print(
        f"Previous production : v{current_version}"
    )

else:

    print(
        "Previous production : NONE"
    )


print(
    f"Evaluated candidate : v{candidate_version}"
)


print(
    f"New production      : v{new_version}"
)


print(
    "Decision            : ACCEPTED"
)


print(
    "Status              : ACTIVE"
)


print(
    "Reason              :",
    decision_reason
)


print()
print(
    "Production path:"
)


print(
    new_production_dir
)


print()
print(
    "Registry:"
)


print(
    REGISTRY_FILE
)


print()
print(
    "Pipeline status:"
)


print(
    PIPELINE_STATUS_FILE
)


print()
print(
    "Previous production was archived."
)


print(
    "Phase 14 completed successfully."
)


print(
    "=" * 90
)