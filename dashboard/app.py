from flask import Flask, jsonify, render_template

from pathlib import Path

import json

from datetime import datetime, timezone


from dashboard.dashboard_data import (
    get_self_correction,
    get_system_summary,
    get_anomalies,
    get_drift,
    get_retraining,
    get_models,
    get_model_evaluation,
    get_dashboard_data
)


# ============================================================
# PATH CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ============================================================
# LIVE ALERT FILE
# ============================================================

LIVE_FILE = (
    PROJECT_ROOT
    / "data"
    / "live"
    / "latest_alert.json"
)


# ============================================================
# AUTOMATIC PIPELINE STATUS FILE
# ============================================================

PIPELINE_STATUS_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "automatic_pipeline_status.json"
)


# ============================================================
# LIVE ADWIN RESULT FILE
# ============================================================

ADWIN_LATEST_RESULT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "adwin_latest_result.json"
)


# ============================================================
# FLASK APP
# ============================================================

app = Flask(
    __name__,
    template_folder="templates"
)


# ============================================================
# DASHBOARD PAGE
# ============================================================

@app.route("/")
def home():

    return render_template("dashboard.html")


# ============================================================
# STANDARD API ROUTES
# ============================================================

@app.route("/api/system")
def system():

    return jsonify(
        get_system_summary()
    )


@app.route("/api/anomalies")
def anomalies():

    return jsonify(
        get_anomalies()
    )


@app.route("/api/drift")
def drift():

    return jsonify(
        get_drift()
    )


@app.route("/api/retraining")
def retraining():

    return jsonify(
        get_retraining()
    )


@app.route("/api/models")
def models():

    return jsonify(
        get_models()
    )


@app.route("/api/evaluation")
def evaluation():

    return jsonify(
        get_model_evaluation()
    )


@app.route("/api/self-correction")
def self_correction():

    return jsonify(
        get_self_correction()
    )


# ============================================================
# FULL DASHBOARD API
# ============================================================

@app.route("/api/dashboard")
def dashboard():

    """
    Full dashboard endpoint.

    Used for the initial dashboard load and historical data.
    """

    return jsonify(
        get_dashboard_data()
    )


# ============================================================
# LIVE ALERT API
# ============================================================

@app.route("/api/live")
def live():

    """
    Lightweight realtime alert endpoint.

    Reads only:

        data/live/latest_alert.json
    """

    try:

        # ----------------------------------------------------
        # No live alert file yet
        # ----------------------------------------------------

        if not LIVE_FILE.exists():

            response = {
                "available": False,
                "latest_alert": None,
                "live_file": str(LIVE_FILE),
                "server_timestamp": datetime.now(
                    timezone.utc
                ).isoformat()
            }

            result = jsonify(response)

            result.headers["Cache-Control"] = (
                "no-store, no-cache, "
                "must-revalidate, max-age=0"
            )

            result.headers["Pragma"] = "no-cache"

            result.headers["Expires"] = "0"

            return result


        # ----------------------------------------------------
        # Read latest alert
        # ----------------------------------------------------

        with open(
            LIVE_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            latest_alert = json.load(file)


        # ----------------------------------------------------
        # Live alert file timestamp
        # ----------------------------------------------------

        file_stat = LIVE_FILE.stat()

        modified_time = datetime.fromtimestamp(
            file_stat.st_mtime,
            tz=timezone.utc
        ).isoformat()


        # ----------------------------------------------------
        # Response
        # ----------------------------------------------------

        response = {
            "available": True,
            "latest_alert": latest_alert,
            "live_file_updated": modified_time,
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }


        result = jsonify(response)

        result.headers["Cache-Control"] = (
            "no-store, no-cache, "
            "must-revalidate, max-age=0"
        )

        result.headers["Pragma"] = "no-cache"

        result.headers["Expires"] = "0"

        return result


    except json.JSONDecodeError:

        response = {
            "available": False,
            "latest_alert": None,
            "error": (
                "Live alert file is temporarily unavailable"
            ),
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }

        result = jsonify(response)

        result.status_code = 503

        result.headers["Cache-Control"] = "no-store"

        return result


    except Exception as exc:

        response = {
            "available": False,
            "latest_alert": None,
            "error": str(exc),
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }

        result = jsonify(response)

        result.status_code = 500

        result.headers["Cache-Control"] = "no-store"

        return result


# ============================================================
# LIVE AUTOMATIC PIPELINE STATUS API
# ============================================================

@app.route("/api/pipeline-live")
def pipeline_live():

    """
    Lightweight realtime endpoint for the automatic
    self-correction pipeline.

    Source:

        data/processed/automatic_pipeline_status.json

    Written by:

        automatic_retraining_orchestrator.py
    """

    try:

        # ----------------------------------------------------
        # Pipeline status file does not exist
        # ----------------------------------------------------

        if not PIPELINE_STATUS_FILE.exists():

            response = {
                "available": False,
                "pipeline_status": None,
                "file": str(PIPELINE_STATUS_FILE),
                "server_timestamp": datetime.now(
                    timezone.utc
                ).isoformat()
            }

            result = jsonify(response)

            result.headers["Cache-Control"] = (
                "no-store, no-cache, "
                "must-revalidate, max-age=0"
            )

            result.headers["Pragma"] = "no-cache"

            result.headers["Expires"] = "0"

            return result


        # ----------------------------------------------------
        # Read current orchestrator status
        # ----------------------------------------------------

        with open(
            PIPELINE_STATUS_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            pipeline_status = json.load(file)


        # ----------------------------------------------------
        # Pipeline status file timestamp
        # ----------------------------------------------------

        file_stat = PIPELINE_STATUS_FILE.stat()

        modified_time = datetime.fromtimestamp(
            file_stat.st_mtime,
            tz=timezone.utc
        ).isoformat()


        # ----------------------------------------------------
        # Response
        # ----------------------------------------------------

        response = {
            "available": True,
            "pipeline_status": pipeline_status,
            "pipeline_file_updated": modified_time,
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }


        result = jsonify(response)

        result.headers["Cache-Control"] = (
            "no-store, no-cache, "
            "must-revalidate, max-age=0"
        )

        result.headers["Pragma"] = "no-cache"

        result.headers["Expires"] = "0"

        return result


    except json.JSONDecodeError:

        response = {
            "available": False,
            "pipeline_status": None,
            "error": (
                "Pipeline status file is temporarily unavailable"
            ),
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }

        result = jsonify(response)

        result.status_code = 503

        result.headers["Cache-Control"] = "no-store"

        return result


    except Exception as exc:

        response = {
            "available": False,
            "pipeline_status": None,
            "error": str(exc),
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }

        result = jsonify(response)

        result.status_code = 500

        result.headers["Cache-Control"] = "no-store"

        return result


# ============================================================
# LIVE ADWIN MONITORING API
# ============================================================

@app.route("/api/adwin-live")
def adwin_live():

    """
    Lightweight realtime endpoint for the ADWIN monitoring panel.

    Source:

        data/processed/adwin_latest_result.json

    Written by:

        adwin_drift_detection.py

    This endpoint reads the existing ADWIN result file directly.

    It does not modify the existing JSON registry/state design.
    """

    try:

        # ----------------------------------------------------
        # ADWIN result file does not exist yet
        # ----------------------------------------------------

        if not ADWIN_LATEST_RESULT_FILE.exists():

            response = {
                "available": False,
                "adwin_result": None,
                "file": str(
                    ADWIN_LATEST_RESULT_FILE
                ),
                "server_timestamp": datetime.now(
                    timezone.utc
                ).isoformat()
            }

            result = jsonify(response)

            result.headers["Cache-Control"] = (
                "no-store, no-cache, "
                "must-revalidate, max-age=0"
            )

            result.headers["Pragma"] = "no-cache"

            result.headers["Expires"] = "0"

            return result


        # ----------------------------------------------------
        # Read latest ADWIN result
        # ----------------------------------------------------

        with open(
            ADWIN_LATEST_RESULT_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            adwin_result = json.load(file)


        # ----------------------------------------------------
        # ADWIN result file timestamp
        # ----------------------------------------------------

        file_stat = ADWIN_LATEST_RESULT_FILE.stat()

        modified_time = datetime.fromtimestamp(
            file_stat.st_mtime,
            tz=timezone.utc
        ).isoformat()


        # ----------------------------------------------------
        # Response
        # ----------------------------------------------------

        response = {
            "available": True,
            "adwin_result": adwin_result,
            "adwin_file_updated": modified_time,
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }


        result = jsonify(response)

        result.headers["Cache-Control"] = (
            "no-store, no-cache, "
            "must-revalidate, max-age=0"
        )

        result.headers["Pragma"] = "no-cache"

        result.headers["Expires"] = "0"

        return result


    except json.JSONDecodeError:

        response = {
            "available": False,
            "adwin_result": None,
            "error": (
                "ADWIN result file is temporarily unavailable"
            ),
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }

        result = jsonify(response)

        result.status_code = 503

        result.headers["Cache-Control"] = "no-store"

        return result


    except Exception as exc:

        response = {
            "available": False,
            "adwin_result": None,
            "error": str(exc),
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }

        result = jsonify(response)

        result.status_code = 500

        result.headers["Cache-Control"] = "no-store"

        return result


# ============================================================
# LIVE PIPELINE LOG API
# ============================================================

@app.route("/api/pipeline-log")
def pipeline_log():

    """
    Lightweight realtime endpoint for the backend pipeline terminal.

    Source:

        data/processed/retraining_pipeline.log

    Written by:

        automatic_retraining_orchestrator.py

    This endpoint is intentionally separate from /api/live so
    live alerts are never mixed with backend pipeline activity.
    """

    pipeline_log_file = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "retraining_pipeline.log"
    )

    try:

        if not pipeline_log_file.exists():

            response = {
                "available": False,
                "records": [],
                "count": 0,
                "file": str(pipeline_log_file),
                "server_timestamp": datetime.now(
                    timezone.utc
                ).isoformat()
            }

            result = jsonify(response)

            result.headers["Cache-Control"] = (
                "no-store, no-cache, must-revalidate, max-age=0"
            )

            result.headers["Pragma"] = "no-cache"

            result.headers["Expires"] = "0"

            return result


        limit = 150


        with open(
            pipeline_log_file,
            "r",
            encoding="utf-8",
            errors="replace"
        ) as file:

            lines = file.readlines()[-limit:]


        records = []


        for line in lines:

            line = line.strip()


            if not line:
                continue


            timestamp = "SYSTEM"

            message = line


            if line.startswith("["):

                closing = line.find("]")


                if closing > 0:

                    timestamp = line[
                        1:closing
                    ].strip()

                    message = line[
                        closing + 1:
                    ].strip()


            records.append({
                "timestamp": timestamp,
                "message": message
            })


        file_stat = pipeline_log_file.stat()


        modified_time = datetime.fromtimestamp(
            file_stat.st_mtime,
            tz=timezone.utc
        ).isoformat()


        response = {
            "available": True,
            "records": records,
            "count": len(records),
            "pipeline_log_updated": modified_time,
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }


        result = jsonify(response)


        result.headers["Cache-Control"] = (
            "no-store, no-cache, "
            "must-revalidate, max-age=0"
        )

        result.headers["Pragma"] = "no-cache"

        result.headers["Expires"] = "0"


        return result


    except Exception as exc:

        response = {
            "available": False,
            "records": [],
            "count": 0,
            "error": str(exc),
            "server_timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }


        result = jsonify(response)

        result.status_code = 500

        result.headers["Cache-Control"] = "no-store"


        return result


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":

    print("=" * 80)

    print(
        "PHASE 15 - NETWORK INTRUSION DETECTION DASHBOARD"
    )

    print("=" * 80)


    print()

    print("Dashboard:")

    print("http://127.0.0.1:5000")


    print()

    print("Live API:")

    print("http://127.0.0.1:5000/api/live")


    print()

    print("ADWIN Live API:")

    print("http://127.0.0.1:5000/api/adwin-live")


    print()

    print("Pipeline Live API:")

    print("http://127.0.0.1:5000/api/pipeline-live")


    print()

    print("Live alert file:")

    print(str(LIVE_FILE))


    print()

    print("Pipeline status file:")

    print(str(PIPELINE_STATUS_FILE))


    print()

    print("ADWIN latest result file:")

    print(str(ADWIN_LATEST_RESULT_FILE))


    print()

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False
    )