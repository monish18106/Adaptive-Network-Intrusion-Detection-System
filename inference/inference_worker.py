from kafka import KafkaConsumer, KafkaProducer
import torch
import torch.nn as nn
import numpy as np
import joblib
import json
import time
import os


# ==========================================
# Paths
# ==========================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

MODEL_DIR = os.path.join(
    BASE_DIR,
    "models",
    "saved_models"
)

REGISTRY_PATH = os.path.join(
    MODEL_DIR,
    "model_registry.json"
)

PRODUCTION_DIR = os.path.join(
    MODEL_DIR,
    "production"
)


# ==========================================
# Kafka Configuration
# ==========================================

BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "processed_features"
OUTPUT_TOPIC = "anomaly_scores"


# ==========================================
# Autoencoder Architecture
# ==========================================

class Autoencoder(nn.Module):

    def __init__(self, input_dim):

        super().__init__()

        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),

            nn.Linear(64, 32),
            nn.ReLU(),

            nn.Linear(32, 16),
            nn.ReLU()
        )

        self.decoder = nn.Sequential(
            nn.Linear(16, 32),
            nn.ReLU(),

            nn.Linear(32, 64),
            nn.ReLU(),

            nn.Linear(64, input_dim)
        )

    def forward(self, x):

        latent = self.encoder(x)

        reconstructed = self.decoder(
            latent
        )

        return reconstructed


# ==========================================
# Model Loading Helpers
# ==========================================

def read_registry():

    if not os.path.exists(
        REGISTRY_PATH
    ):

        return {}

    try:

        with open(
            REGISTRY_PATH,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        return (
            data
            if isinstance(data, dict)
            else {}
        )

    except Exception as exc:

        print(
            "WARNING: Could not read model registry:",
            exc
        )

        return {}


def get_active_model_directory():

    registry = read_registry()

    version = registry.get(
        "current_production_version"
    )

    if version is not None:

        version_dir = os.path.join(
            PRODUCTION_DIR,
            f"v{version}"
        )

        required = [
            "isolation_forest.pkl",
            "autoencoder.pth",
            "ae_threshold.pkl",
            "ae_metadata.pkl"
        ]

        if all(
            os.path.exists(
                os.path.join(
                    version_dir,
                    filename
                )
            )
            for filename in required
        ):

            return (
                version_dir,
                version
            )

        print(
            "WARNING: Registry points to production",
            f"v{version}, but required files are missing."
        )

    # --------------------------------------------------
    # First-run compatibility
    #
    # Before the first lifecycle promotion, the project
    # may still have the original production artifacts
    # directly inside models/saved_models/.
    # --------------------------------------------------

    legacy_files = [
        "isolation_forest.pkl",
        "autoencoder.pth",
        "ae_threshold.pkl",
        "ae_metadata.pkl"
    ]

    if all(
        os.path.exists(
            os.path.join(
                MODEL_DIR,
                filename
            )
        )
        for filename in legacy_files
    ):

        return (
            MODEL_DIR,
            None
        )

    raise FileNotFoundError(
        "No valid production model was found. "
        "Expected models/saved_models/production/vN/ "
        "or the legacy root production artifacts."
    )


def load_models():

    (
        model_directory,
        version
    ) = get_active_model_directory()

    print()
    print(
        "Loading production model..."
    )

    if version is None:

        print(
            "Production version: LEGACY ROOT"
        )

    else:

        print(
            "Production version:",
            f"v{version}"
        )

    # --------------------------------------------------
    # Isolation Forest
    # --------------------------------------------------

    if_model = joblib.load(
        os.path.join(
            model_directory,
            "isolation_forest.pkl"
        )
    )

    # --------------------------------------------------
    # Autoencoder metadata
    # --------------------------------------------------

    metadata = joblib.load(
        os.path.join(
            model_directory,
            "ae_metadata.pkl"
        )
    )

    # --------------------------------------------------
    # AE threshold
    # --------------------------------------------------

    threshold = float(
        joblib.load(
            os.path.join(
                model_directory,
                "ae_threshold.pkl"
            )
        )
    )

    input_dim = int(
        metadata["input_dim"]
    )

    # --------------------------------------------------
    # Autoencoder
    # --------------------------------------------------

    ae_model = Autoencoder(
        input_dim
    ).to(device)

    ae_model.load_state_dict(
        torch.load(
            os.path.join(
                model_directory,
                "autoencoder.pth"
            ),
            map_location=device
        )
    )

    ae_model.eval()

    print(
        "Production model loaded successfully."
    )

    return (
        if_model,
        ae_model,
        threshold,
        input_dim,
        version
    )


# ==========================================
# Device
# ==========================================

device = torch.device(
    "cuda"
    if torch.cuda.is_available()
    else "cpu"
)

print(
    f"Using Device: {device}"
)


# ==========================================
# Initial Model Load
# ==========================================

(
    if_model,
    ae_model,
    threshold,
    input_dim,
    active_version
) = load_models()


# ==========================================
# Track Registry Changes
# ==========================================

try:

    last_registry_mtime = os.path.getmtime(
        REGISTRY_PATH
    )

except OSError:

    last_registry_mtime = None


# ==========================================
# Kafka Consumer
# ==========================================

consumer = KafkaConsumer(
    INPUT_TOPIC,

    bootstrap_servers=BOOTSTRAP_SERVERS,

    value_deserializer=lambda x:
        json.loads(
            x.decode("utf-8")
        ),

    auto_offset_reset="latest",

    group_id="inference-worker-v2"
)


# ==========================================
# Kafka Producer
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,

    value_serializer=lambda v:
        json.dumps(v).encode(
            "utf-8"
        )
)


processed = 0


print(
    "Inference Worker Started..."
)


# ==========================================
# Inference Loop
# ==========================================

try:

    for msg in consumer:

        # ==========================================
        # Detect Model Promotion
        # ==========================================

        try:

            current_registry_mtime = os.path.getmtime(
                REGISTRY_PATH
            )

        except OSError:

            current_registry_mtime = None

        if (
            current_registry_mtime is not None
            and
            current_registry_mtime !=
                last_registry_mtime
        ):

            registry = read_registry()

            registry_version = registry.get(
                "current_production_version"
            )

            if registry_version != active_version:

                try:

                    (
                        if_model,
                        ae_model,
                        threshold,
                        input_dim,
                        active_version
                    ) = load_models()

                    print(
                        "Inference switched to production",
                        f"v{active_version}."
                    )

                except Exception as exc:

                    print(
                        "WARNING: New production model "
                        "could not be loaded:",
                        exc
                    )

            last_registry_mtime = (
                current_registry_mtime
            )


        # ==========================================
        # Read Processed Features
        # ==========================================

        features = msg.value[
            "features"
        ]

        X = np.array(
            features,
            dtype=np.float32
        ).reshape(
            1,
            -1
        )


        # ==========================================
        # Validate Feature Dimension
        # ==========================================

        if X.shape[1] != input_dim:

            print(
                "WARNING: Feature dimension mismatch.",
                f"Received={X.shape[1]}, "
                f"expected={input_dim}."
            )

            continue


        # ==========================================
        # Isolation Forest Score
        # ==========================================

        if_score = float(
            -if_model.score_samples(
                X
            )[0]
        )


        # ==========================================
        # Autoencoder Score
        # ==========================================

        tensor_x = torch.tensor(
            X,
            dtype=torch.float32
        ).to(device)

        with torch.no_grad():

            reconstructed = ae_model(
                tensor_x
            )


        reconstruction_error = float(
            torch.mean(
                (
                    tensor_x -
                    reconstructed
                ) ** 2
            )
            .cpu()
            .item()
        )


        # ==========================================
        # Normalize AE Score
        # ==========================================

        if threshold > 0:

            ae_score = min(
                reconstruction_error /
                threshold,
                1.0
            )

        else:

            ae_score = 1.0


        ae_anomaly = int(
            reconstruction_error >
            threshold
        )


        # ==========================================
        # Output Message
        # ==========================================

        result = {

            # Original processed features
            "features":
                features,

            # Isolation Forest
            "if_score":
                if_score,

            # Autoencoder reconstruction error
            "ae_error":
                reconstruction_error,

            # Normalized AE score
            "ae_score":
                ae_score,

            # AE anomaly flag
            "ae_anomaly":
                ae_anomaly,

            # AE threshold
            "threshold":
                threshold,

            # Active production version
            "model_version":
                active_version,

            # Timestamp
            "timestamp":
                time.time()
        }


        # ==========================================
        # Send To anomaly_scores
        # ==========================================

        producer.send(
            OUTPUT_TOPIC,
            value=result
        )


        processed += 1


        # ==========================================
        # Progress
        # ==========================================

        if processed % 1000 == 0:

            print(
                f"Scored {processed} records "
                f"using model v{active_version}"
            )

            producer.flush()


except KeyboardInterrupt:

    print(
        "\nStopping inference worker..."
    )


finally:

    producer.flush()

    producer.close()

    consumer.close()

    print(
        "Inference worker stopped."
    )