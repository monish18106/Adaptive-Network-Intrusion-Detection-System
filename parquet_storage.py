from kafka import KafkaConsumer
import json
import os
import time
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_TOPIC = "alerts"
BOOTSTRAP_SERVERS = "localhost:9092"

# Historical storage
OUTPUT_DIR = "data/history"

# Live dashboard storage
LIVE_DIR = "data/live"
LIVE_ALERT_PATH = os.path.join(
    LIVE_DIR,
    "latest_alert.json"
)

# Keep historical Parquet batching.
BATCH_SIZE = 100


# ============================================================
# CREATE DIRECTORIES
# ============================================================

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)

os.makedirs(
    LIVE_DIR,
    exist_ok=True
)


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    ),
    auto_offset_reset="latest",
    group_id="parquet-storage-worker-v1"
)


# ============================================================
# VARIABLES
# ============================================================

records = []

processed = 0


# ============================================================
# FIND NEXT PARQUET FILE NUMBER
# ============================================================

def get_next_file_counter():

    existing_files = []

    try:
        for filename in os.listdir(
            OUTPUT_DIR
        ):

            if (
                filename.startswith(
                    "alerts_"
                )
                and
                filename.endswith(
                    ".parquet"
                )
            ):

                existing_files.append(
                    filename
                )

    except Exception:
        return 0

    if not existing_files:
        return 0

    highest = -1

    for filename in existing_files:

        try:

            name = os.path.splitext(
                filename
            )[0]

            counter_text = name.split(
                "_"
            )[-1]

            counter = int(
                counter_text
            )

            highest = max(
                highest,
                counter
            )

        except Exception:
            continue

    return highest + 1


file_counter = get_next_file_counter()


# ============================================================
# CONSOLE
# ============================================================

print("=" * 80)
print("PARQUET STORAGE WORKER")
print("=" * 80)

print(
    f"Kafka Topic : {INPUT_TOPIC}"
)

print(
    f"Output Dir  : {OUTPUT_DIR}"
)

print(
    f"Live File   : {LIVE_ALERT_PATH}"
)

print(
    f"Batch Size  : {BATCH_SIZE}"
)

print(
    f"Starting File Counter : {file_counter}"
)

print(
    "\nWaiting for alert records...\n"
)


# ============================================================
# WRITE LIVE ALERT
# ============================================================

def write_live_alert(record):
    """
    Write the newest alert immediately so the dashboard can
    display it without waiting for the Parquet batch.

    Atomic write:
        1. write temporary JSON
        2. replace old JSON

    This prevents the dashboard from reading a partially
    written JSON file.
    """

    temp_path = (
        LIVE_ALERT_PATH
        + ".tmp"
    )

    try:

        with open(
            temp_path,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                record,
                f,
                ensure_ascii=False
            )

            f.flush()

            os.fsync(
                f.fileno()
            )

        os.replace(
            temp_path,
            LIVE_ALERT_PATH
        )

    except Exception as exc:

        print(
            "WARNING: Could not update live alert: "
            f"{exc}"
        )

        try:

            if os.path.exists(
                temp_path
            ):
                os.remove(
                    temp_path
                )

        except Exception:
            pass


# ============================================================
# SAVE BATCH TO PARQUET
# ============================================================

def save_batch(batch):

    global file_counter

    if not batch:
        return

    df = pd.DataFrame(
        batch
    )

    # Include milliseconds so files created very close
    # together are easier to distinguish.
    timestamp = time.strftime(
        "%Y%m%d_%H%M%S"
    )

    filename = (
        f"alerts_{timestamp}_"
        f"{file_counter:04d}.parquet"
    )

    filepath = os.path.join(
        OUTPUT_DIR,
        filename
    )

    # --------------------------------------------------------
    # Write to a temporary file first.
    #
    # This prevents dashboard_data.py from attempting to read
    # a Parquet file while it is still being written.
    # --------------------------------------------------------

    temp_filepath = (
        filepath
        + ".tmp"
    )

    try:

        df.to_parquet(
            temp_filepath,
            index=False
        )

        os.replace(
            temp_filepath,
            filepath
        )

        file_counter += 1

        print(
            f"Saved {len(df)} records → {filepath}"
        )

    except Exception as exc:

        print(
            f"ERROR saving Parquet batch: {exc}"
        )

        try:

            if os.path.exists(
                temp_filepath
            ):
                os.remove(
                    temp_filepath
                )

        except Exception:
            pass


# ============================================================
# CONSUME ALERTS
# ============================================================

try:

    for msg in consumer:

        data = msg.value

        # ----------------------------------------------------
        # Validate alert record
        # ----------------------------------------------------

        if "final_score" not in data:
            continue

        if "status" not in data:
            continue

        # ----------------------------------------------------
        # Generate record number
        # ----------------------------------------------------

        record_index = (
            processed + 1
        )

        # ----------------------------------------------------
        # Store record
        #
        # Keep the same ML fields used by the existing system.
        # ----------------------------------------------------

        record = {
            "record_index":
                record_index,

            "features":
                data.get(
                    "features"
                ),

            "if_score":
                data.get(
                    "if_score"
                ),

            "ae_score":
                data.get(
                    "ae_score"
                ),

            "final_score":
                data.get(
                    "final_score"
                ),

            "threshold":
                data.get(
                    "threshold"
                ),

            "status":
                data.get(
                    "status"
                ),

            "timestamp":
                data.get(
                    "timestamp",
                    time.time()
                )
        }

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Write to the live dashboard state IMMEDIATELY.
        #
        # This happens for every alert, not every 100 alerts.
        # ----------------------------------------------------

        write_live_alert(
            record
        )

        # ----------------------------------------------------
        # Add to historical batch.
        # ----------------------------------------------------

        records.append(
            record
        )

        processed += 1

        # ----------------------------------------------------
        # Save historical Parquet every 100 records.
        # ----------------------------------------------------

        if len(records) >= BATCH_SIZE:

            save_batch(
                records
            )

            records.clear()

            print(
                f"Total processed: {processed}"
            )


except KeyboardInterrupt:

    print(
        "\nStopping worker..."
    )

    # --------------------------------------------------------
    # Save remaining historical records.
    # --------------------------------------------------------

    if records:

        save_batch(
            records
        )

    print(
        f"\nTotal records processed: {processed}"
    )

    print(
        "Parquet Storage Worker stopped."
    )


finally:

    try:
        consumer.close()
    except Exception:
        pass