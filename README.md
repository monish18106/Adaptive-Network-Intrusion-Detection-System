# 🛡️ Adaptive Network Intrusion Detection System

> **A real-time, self-correcting network intrusion detection platform** that streams network traffic through Apache Kafka, scores flows using dual machine learning models (Isolation Forest + Deep Autoencoder), detects statistical concept drift with ADWIN, autonomously retrains candidate models with PySpark, performs champion-challenger quality gate evaluations, executes transactional zero-downtime model promotions, and visualizes the complete pipeline on an enterprise Flask telemetry dashboard.

---

## 📑 Table of Contents

- [System Overview](#-system-overview)
- [Key Features & Recent Enhancements](#-key-features--recent-enhancements)
- [System Architecture](#-system-architecture)
- [Data Pipeline Flow](#-data-pipeline-flow)
- [Project Structure](#-project-structure)
- [Module Deep-Dive](#-module-deep-dive)
  - [1. One-Click Pipeline Launcher](#1-one-click-pipeline-launcher)
  - [2. Kafka Streaming Layer](#2-kafka-streaming-layer)
  - [3. Feature Processing Layer](#3-feature-processing-layer)
  - [4. Real-Time Inference & Zero-Downtime Hot-Reloading](#4-real-time-inference--zero-downtime-hot-reloading)
  - [5. Score Fusion & Alerting](#5-score-fusion--alerting)
  - [6. Two-Tier Storage: Live Alerts & Parquet Batches](#6-two-tier-storage-live-alerts--parquet-batches)
  - [7. Concept Drift Detection (ADWIN)](#7-concept-drift-detection-adwin)
  - [8. Distributed Candidate Retraining (PySpark)](#8-distributed-candidate-retraining-pyspark)
  - [9. Champion/Challenger Model Evaluation](#9-championchallenger-model-evaluation)
  - [10. Transactional Model Lifecycle & Promotion](#10-transactional-model-lifecycle--promotion)
  - [11. Autonomous Retraining Orchestrator](#11-autonomous-retraining-orchestrator)
  - [12. Telemetry Dashboard & Real-Time APIs](#12-telemetry-dashboard--real-time-apis)
- [Machine Learning Architecture](#-machine-learning-architecture)
- [Kafka Topics & Payload Schemas](#-kafka-topics--payload-schemas)
- [Model Registry & Lifecycle State Machine](#-model-registry--lifecycle-state-machine)
- [Data Directory Layout](#-data-directory-layout)
- [Model Artifacts Directory Structure](#-model-artifacts-directory-structure)
- [System Configuration Reference](#-system-configuration-reference)
- [Getting Started & Execution Guide](#-getting-started--execution-guide)
  - [Prerequisites](#prerequisites)
  - [Kafka Quick Setup (KRaft Mode)](#kafka-quick-setup-kraft-mode)
  - [Option A: One-Click Startup (Recommended for Windows)](#option-a-one-click-startup-recommended-for-windows)
  - [Option B: Manual Terminal-by-Terminal Execution](#option-b-manual-terminal-by-terminal-execution)
- [Dataset Specifications](#-dataset-specifications)
- [Dependencies](#-dependencies)

---

## 🌟 System Overview

Modern network attacks evolve dynamically, rendering static signature rules and fixed ML models ineffective over time. The **Adaptive Network IDS** overcomes this challenge through a **closed-loop autonomous self-correction architecture**:

| Core Capability | Technical Implementation | Purpose |
|---|---|---|
| **High-Throughput Ingestion** | Apache Kafka (KRaft mode) | Streams millions of network flow records across decoupled broker topics |
| **Dual-Model Inference** | Scikit-Learn Isolation Forest + PyTorch Autoencoder | Captures both statistical outlier properties and non-linear latent reconstructions |
| **Weighted Score Fusion** | $S_{\text{final}} = 0.2 \cdot S_{\text{IF}} + 0.8 \cdot S_{\text{AE}}$ | Balances fast boundary separation with deep reconstruction fidelity |
| **Zero-Downtime Hot-Reloading** | Filesystem registry modification monitoring in `inference_worker.py` | Automatically swaps production models in memory without worker restart or dropped packets |
| **Two-Tier Alert Persistence** | Atomic sub-second JSON dispatch + 100-record Parquet batching | Powers sub-second dashboard alerts while building an efficient historical analytics data lake |
| **Statistical Concept Drift** | River ADWIN (Adaptive Windowing) with $\delta=0.002$ | Evaluates drift at 10,000-record checkpoints without arbitrary sliding windows |
| **Autonomous Retraining** | Distributed PySpark + PyTorch training pipeline | Trains candidate models exclusively on confirmed normal historical network flows |
| **Champion/Challenger Gates** | F1 tolerance ($\le 2\%$) and False Alarm Rate tolerance ($\le 2\%$) | Prevents model degradation and regression before promotion |
| **Transactional Promotion** | Atomic archive-then-promote pattern with automatic rollback | Zero corruption risk: archives old version, stages candidate, updates registry |
| **Live Telemetry Dashboard** | Flask REST APIs + Real-time AJAX polling + Embedded Backend Terminal | Real-time monitoring of live flows, drift events, model metadata, and training logs |

---

## 🚀 Key Features & Recent Enhancements

- **⚡ One-Click Full Pipeline Launcher (`start_project.bat`)**: Sequentially initializes all 10 pipeline components in isolated console sessions with built-in 5-second graceful start delays and virtual environment verification.
- **🔄 Zero-Downtime Hot-Reloading Inference**: The inference worker monitors `models/saved_models/model_registry.json`. Upon model promotion, it automatically reloads the new production version (`v1`, `v2`, etc.) in memory without interrupting streaming inference.
- **🏷️ Model Version Tagging**: Scored records in the `anomaly_scores` topic are now stamped with the active `model_version`, providing end-to-end model governance for every classified packet.
- **⏱️ Two-Tier Storage (Live + Batched)**: Alerts are written immediately via atomic filesystem swap (`latest_alert.json.tmp` $\to$ `latest_alert.json`) for instantaneous dashboard visibility, alongside resilient 100-record Parquet batching (`alerts_*.parquet`).
- **📊 Real-Time ADWIN State Publishing**: ADWIN publishes continuous state updates (`adwin_latest_result.json`) at all times—including `WAITING_FOR_CHECKPOINT`—providing window width, estimated mean, and progress towards the next 10,000-record checkpoint.
- **📁 Isolated Candidate Versioning (`candidates/v{N}`)**: Retrained candidate models are saved under dedicated version directories (`models/saved_models/candidates/v{version}/`) alongside comprehensive training metadata.
- **🛡️ Strict Separation of Concerns in Model Evaluation**: Phase 12 (`model_evaluation.py`) is strictly analytical; it computes champion vs. candidate metrics and issues an `ACCEPTED` or `REJECTED` decision without mutating production files. Promotion is exclusively handled by Phase 14 (`model_lifecycle.py`).
- **🔒 Transactional Promotion & Rollback**: Model promotions use a safe directory swap pattern. If candidate copying encounters an issue, changes roll back immediately, preserving production stability.
- **💻 Interactive Backend Pipeline Terminal**: The Flask dashboard features a live terminal streaming `retraining_pipeline.log` in real time, giving instant visibility into backend orchestrator events.
- **⚡ Lightweight Real-Time REST APIs**: Added dedicated `/api/live`, `/api/pipeline-live`, `/api/adwin-live`, and `/api/pipeline-log` endpoints with HTTP `no-cache` headers, backed by thread-safe memory caching.

---

## 🏗️ System Architecture

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        ADAPTIVE NETWORK IDS ARCHITECTURE                               │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│   ┌──────────────┐      ┌─────────────────┐      ┌───────────────────────────────┐     │
│   │ Kafka Record │ ───▶ │ Feature Scaling │ ───▶ │     Dual-Model Inference      │     │
│   │   Producer   │      │ (scaler.pkl)    │      │  (Isolation Forest + PyTorch) │     │
│   └──────────────┘      └─────────────────┘      └──────────────┬────────────────┘     │
│     raw_traffic         processed_features                      │                      │
│                                                                 │ anomaly_scores       │
│                                                                 ▼ (model_version tagged│
│   ┌─────────────────────────────────────────────────────────────────────────────┐      │
│   │                             SCORE FUSION & ALERTING                         │      │
│   │  Fusion Worker: S_final = 0.2 · IF + 0.8 · AE       [fusion_scores]         │      │
│   │  Alert Worker:  S_final >= 0.85 -> ANOMALY / NORMAL [alerts]                │      │
│   └──────────────────────────────────────┬──────────────────────────────────────┘      │
│                                          │ alerts                                      │
│                  ┌───────────────────────┴───────────────────────┐                     │
│                  ▼                                               ▼                     │
│      ┌───────────────────────┐                       ┌───────────────────────┐         │
│      │  Instant Live Alert   │                       │  Batch Parquet Store  │         │
│      │ (latest_alert.json)   │                       │  (batch=100 records)  │         │
│      └───────────┬───────────┘                       └───────────┬───────────┘         │
│                  │                                               │ data/history/*.pq   │
│                  │                                               ▼                     │
│                  │       ┌──────────────────────────────────────────────────────────┐  │
│                  │       │          AUTONOMOUS SELF-CORRECTION LOOP (Phase 16)      │  │
│                  │       │                                                          │  │
│                  │       │   ADWIN Concept Drift Detection (10k checkpoints)        │  │
│                  │       │   [adwin_latest_result.json, adwin_drift_results.csv]   │  │
│                  │       │                           │                              │  │
│                  │       │                   Drift Threshold?                       │  │
│                  │       │                   (50 drift events)                      │  │
│                  │       │                     /           \                        │  │
│                  │       │                 YES               NO                     │  │
│                  │       │                  ▼                 └──▶ Wait 10k recs    │  │
│                  │       │   PySpark Candidate Retraining (NORMAL traffic only)     │  │
│                  │       │   [models/saved_models/candidates/v{N}/]                 │  │
│                  │       │                  ▼                                       │  │
│                  │       │   Model Evaluation (F1 <= 2% drop, FAR <= 2% rise)       │  │
│                  │       │   [phase12_model_comparison.csv, Decision Report]        │  │
│                  │       │                  ▼                                       │  │
│                  │       │   Model Lifecycle (Transactional Promotion / Rollback)   │  │
│                  │       │   [archive/v{old} -> production/v{new} -> registry.json] │  │
│                  │       └───────────────────────────┬──────────────────────────────┘  │
│                  │                                   │                                 │
│                  │      ┌────────────────────────────┘                                 │
│                  │      ▼ (hot-reloads new production models)                          │
│                  │   Inference Worker Hot-Reload                                       │
│                  │                                                                     │
│                  ▼                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐      │
│   │                    FLASK TELEMETRY DASHBOARD (Port 5000)                    │      │
│   │  • System Overview    • Self-Correction Stepper   • Backend Pipeline Log    │      │
│   │  • ADWIN Concept Drift • PySpark Metrics           • Top Production Model   │      │
│   │  • Model Metadata     • Version Management        • Quality Evaluation     │      │
│   │  • Sub-second Live Anomaly Event Stream                                     │      │
│   └─────────────────────────────────────────────────────────────────────────────┘      │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Pipeline Flow

```
CICIDS2017 Dataset ──▶ producer.py ──▶ [raw_traffic]
                                            │
                                            ▼
                                     processor.py (scales 59 features via scaler.pkl)
                                            │
                                            ▼
                                    [processed_features]
                                            │
                                            ▼
                                  inference_worker.py (IF score + AE MSE reconstruction)
                                            │ (auto hot-reloads when production updates)
                                            ▼
                                    [anomaly_scores] (stamped with model_version)
                                            │
                                            ▼
                                     fusion_worker.py (S = 0.2·IF + 0.8·AE)
                                            │
                                            ▼
                                     [fusion_scores]
                                            │
                                            ▼
                                     alert_worker.py (Threshold >= 0.85 -> ANOMALY)
                                            │
                                            ▼
                                         [alerts]
                                            │
                       ┌────────────────────┴────────────────────┐
                       ▼                                         ▼
            parquet_storage.py (Tier 1)               parquet_storage.py (Tier 2)
           Atomic Live Alert Writer                  Batch Parquet Writer (batch=100)
                       │                                         │
                       ▼                                         ▼
           data/live/latest_alert.json                data/history/alerts_*.parquet
                       │                                         │
                       ▼                                         ▼
             Flask /api/live Poll                     ADWIN Drift Monitor (10k recs)
                       │                                         │
                       ▼                                   Drift Threshold
             Dashboard Live Alert                     Reached (50 events)?
                                                            /          \
                                                          YES           NO
                                                           │             └──▶ Continue
                                                           ▼
                                               PySpark Distributed Retrain
                                               (trains candidate IF + AE on NORMAL data)
                                                           │
                                                           ▼
                                               Candidate Model Evaluation
                                               (Champion vs Challenger Quality Gates)
                                                           │
                                                           ▼
                                               Transactional Model Lifecycle
                                               (Archive v{old} -> Promote v{new})
                                                           │
                                                           ▼
                                               Model Registry Updated
                                                           │
                                                           ▼
                                               Inference Worker Hot-Swaps Models!
```

---

## 📁 Project Structure

```
adaptive-network/
│
├── README.md                              # Comprehensive system documentation
├── requirements.txt                       # Python dependencies
├── kafka config.txt                       # Kafka KRaft cluster setup & run commands
├── Final file-by-file checklist.txt       # Architecture & component implementation checklist
├── start_project.bat                      # One-click Windows full-pipeline launcher
│
├── kafka/                                 # Kafka streaming layer
│   ├── producers/
│   │   └── producer.py                    # Streams CSV flows → raw_traffic topic
│   └── consumers/
│       └── consumer_test.py               # Raw traffic diagnostic consumer
│
├── flink/                                 # Feature preprocessing layer
│   ├── config.py                          # Kafka connection & topic configurations
│   ├── processor.py                       # Normalizes & scales records → processed_features
│   ├── feature_processor.py               # Standalone preprocessing transformation
│   ├── flink_job.py                       # Flink job streaming configuration
│   ├── processed_consumer.py              # Processed features diagnostic consumer
│   ├── test.py                            # Pipeline validation script
│   ├── checkpoints/                       # State checkpoints
│   └── jobs/                              # Flink job JAR artifacts
│
├── inference/                             # Real-time inference & fusion engine
│   ├── inference_worker.py                # Dual-model scoring with zero-downtime hot-reloading
│   ├── fusion_worker.py                   # Weighted score fusion (0.2 IF + 0.8 AE)
│   ├── alert_worker.py                    # Anomaly thresholding (0.85) → alerts topic
│   ├── alert_consumer.py                  # Real-time formatted console alert consumer
│   ├── fusion_consumer.py                 # Fusion scores diagnostic consumer
│   └── score_consumer.py                  # Anomaly scores diagnostic consumer
│
├── parquet_storage.py                     # Two-tier storage: instant live alert + batch Parquet
├── adwin_drift_detection.py               # ADWIN concept drift detector with live state export
├── pyspark_retraining.py                  # Distributed candidate model retraining (Phase 11)
├── pyspark_check.py                       # Parquet historical dataset inspection utility
├── model_evaluation.py                    # Champion vs. Challenger quality evaluation (Phase 12)
├── model_versioning.py                    # Registry tracking & version utilities (Phase 13)
├── phase13_model_versioning.py            # Model archive & registry management
├── model_lifecycle.py                     # Transactional promotion/rejection/rollback (Phase 14)
├── self_correction_controller.py          # Standalone control loop runner
├── automatic_retraining_orchestrator.py   # Autonomous end-to-end master orchestrator (Phase 16)
│
├── dashboard/                             # Flask telemetry & management web app (Phase 15)
│   ├── app.py                             # Flask routes, live REST APIs & static handlers
│   ├── dashboard_data.py                  # Thread-safe data ingestion, aggregation & cache
│   ├── templates/
│   │   └── dashboard.html                 # Multi-panel responsive dashboard HTML template
│   └── static/
│       ├── dashboard.css                  # Modern dark glassmorphic styling system
│       └── dashboard.js                   # Multi-interval AJAX polling & dynamic DOM rendering
│
├── data/                                  # Data lake & state directory
│   ├── raw/                               # 8 original CICIDS2017 raw CSV files
│   ├── processed/                         # Cleaned datasets, logs, reports & pipeline states
│   │   ├── CICIDS2017_Cleaned.csv         # Cleaned traffic dataset (~883 MB)
│   │   ├── model_dataset.csv              # Production model training dataset (~770 MB)
│   │   ├── adwin_drift_results.csv        # Detailed drift detection history
│   │   ├── adwin_latest_result.json       # Live ADWIN window state & metrics
│   │   ├── adwin_state.json               # Checkpoint state persistence
│   │   ├── retraining_trigger.json        # Signal file initiating candidate retraining
│   │   ├── automatic_pipeline_status.json # Real-time orchestrator progress state
│   │   ├── automatic_pipeline_history.json# Full historical execution events
│   │   ├── automatic_retraining_state.json# Retraining coordinator checkpoint
│   │   ├── retraining_pipeline.log        # Live orchestrator terminal log stream
│   │   ├── phase11_retraining_report.csv  # PySpark training execution report
│   │   ├── phase12_model_comparison.csv   # Old vs. candidate metric comparison
│   │   ├── phase12_model_metadata.json    # Evaluation run configuration & metrics
│   │   ├── phase12_promotion_report.csv   # Formal promotion decision report
│   │   └── pyspark_retraining_report.csv  # Training metrics & parameter log
│   ├── live/                              # High-speed live alert scratch directory
│   │   └── latest_alert.json              # Sub-second live alert atomically updated
│   ├── features/                          # Feature engineering workspace
│   └── history/                           # Historical Parquet alerts lake (100-record batches)
│
├── models/                                # Model registry & serialization storage
│   └── saved_models/
│       ├── isolation_forest.pkl           # Active production Isolation Forest
│       ├── autoencoder.pth                # Active production PyTorch Autoencoder weights
│       ├── ae_threshold.pkl               # Active production AE reconstruction threshold
│       ├── ae_metadata.pkl                # Active production AE architecture metadata
│       ├── scaler.pkl                     # Active 59-feature StandardScaler
│       ├── model_registry.json            # Global model registry & version tracking
│       ├── model_version.json             # Current active production version pointer
│       ├── production/                    # Production version snapshots (`v1/`, `v2/`, ...)
│       ├── candidates/                    # Staged candidate versions (`v2/`, `v3/`, ...)
│       ├── archive/                       # Archived historical production versions
│       ├── candidate_isolation_forest.pkl # Latest candidate IF artifact
│       ├── candidate_autoencoder.pth      # Latest candidate AE artifact
│       ├── candidate_ae_threshold.pkl     # Latest candidate threshold artifact
│       ├── candidate_ae_metadata.pkl      # Latest candidate metadata artifact
│       └── candidate_training_metadata.json# Candidate training parameters & record counts
│
├── notebooks/                             # Exploratory data science notebooks
├── logs/                                  # Runtime application logs
└── tests/                                 # Unit & integration test suites
```

---

## 🔍 Module Deep-Dive

### 1. One-Click Pipeline Launcher

#### [`start_project.bat`](file:///d:/adaptive%20network/start_project.bat)
A robust Windows batch automation script designed to start all 10 pipeline components in the exact required operational order with 5-second staggered delays:
1. Validates `.venv\Scripts\python.exe` existence before executing.
2. Spawns each service in a labeled separate terminal window (`start "Title" cmd /k ...`):
   - `[1/10]` Kafka Producer
   - `[2/10]` Flink Feature Processor
   - `[3/10]` Inference Worker
   - `[4/10]` Fusion Worker
   - `[5/10]` Fusion Consumer
   - `[6/10]` Alert Worker
   - `[7/10]` Alert Consumer
   - `[8/10]` Parquet Storage Worker
   - `[9/10]` Automatic Retraining Orchestrator
   - `[10/10]` Flask Telemetry Dashboard
3. Displays the dashboard URL (`http://127.0.0.1:5000`) upon launch.

---

### 2. Kafka Streaming Layer

#### [`kafka/producers/producer.py`](file:///d:/adaptive%20network/kafka/producers/producer.py)
- Ingests `data/processed/model_dataset.csv` row-by-row and publishes records as JSON to the `raw_traffic` topic.
- Paces ingestion with an adjustable delay (default: 10ms) and flushes every 10,000 records.

#### [`kafka/consumers/consumer_test.py`](file:///d:/adaptive%20network/kafka/consumers/consumer_test.py)
- Subscribes to `raw_traffic` from the earliest offset to verify cluster health and packet stream integrity.

---

### 3. Feature Processing Layer

#### [`flink/processor.py`](file:///d:/adaptive%20network/flink/processor.py)
- Consumes raw network flow dictionaries from `raw_traffic`.
- Strips the ground truth `Label` column to prevent label leakage.
- Loads `models/saved_models/scaler.pkl` (`StandardScaler`) to normalize the 59 numerical features.
- Emits serialized vector dictionaries to `processed_features`.

---

### 4. Real-Time Inference & Zero-Downtime Hot-Reloading

#### [`inference/inference_worker.py`](file:///d:/adaptive%20network/inference/inference_worker.py)
The core scoring engine featuring **dynamic model hot-reloading**:
- **Zero-Downtime Hot-Swap**: Continuously tracks the filesystem modification timestamp (`st_mtime`) of `models/saved_models/model_registry.json`. When `current_production_version` updates following a model promotion, it automatically reloads the new Isolation Forest, Autoencoder, threshold, and metadata in memory without worker restart.
- **Model Version Tracking**: Stretches governance by tagging the active `model_version` directly into every outgoing message.
- **Safety Assertions**: Validates that incoming feature arrays match `input_dim = 59`, logs warnings for malformed flows, and guards against division-by-zero during AE score normalization.
- **Scoring Output**: Computes normalized IF outlier score and AE MSE reconstruction score, publishing to `anomaly_scores`.

---

### 5. Score Fusion & Alerting

#### [`inference/fusion_worker.py`](file:///d:/adaptive%20network/inference/fusion_worker.py)
- Consumes from `anomaly_scores` and executes weighted fusion:
  $$S_{\text{final}} = 0.2 \cdot S_{\text{IF}} + 0.8 \cdot S_{\text{AE}}$$
- Emits composite records containing individual model scores and the combined score to `fusion_scores`.

#### [`inference/alert_worker.py`](file:///d:/adaptive%20network/inference/alert_worker.py)
- Applies the anomaly threshold ($0.85$):
  - $S_{\text{final}} \ge 0.85 \implies \text{ANOMALY}$
  - $S_{\text{final}} < 0.85 \implies \text{NORMAL}$
- Enriches records with classification status, threshold value, and ISO timestamps, emitting to `alerts`.

---

### 6. Two-Tier Storage: Live Alerts & Parquet Batches

#### [`parquet_storage.py`](file:///d:/adaptive%20network/parquet_storage.py)
A specialized two-tier persistence engine:
- **Tier 1 (Instant Live Alert)**: On every single message consumed from `alerts`, it executes `write_live_alert(record)`. This writes to `data/live/latest_alert.json.tmp` and replaces `latest_alert.json` via atomic `os.replace`. The dashboard can display incoming attacks in sub-second time without waiting for batch accumulation.
- **Tier 2 (Batch Parquet Lake)**: Buffers records into batches of 100 and writes to `data/history/alerts_YYYYMMDD_HHMMSS_XXXX.parquet`. Uses atomic `.tmp` staging to prevent dashboard read conflicts.
- **Auto-Discovered Sequencing**: Automatically checks existing files in `data/history/` on boot to initialize the file counter without overwriting existing data.

---

### 7. Concept Drift Detection (ADWIN)

#### [`adwin_drift_detection.py`](file:///d:/adaptive%20network/adwin_drift_detection.py)
Monitors anomaly scores for statistical drift using the **ADWIN (Adaptive Windowing)** algorithm from River:
- **Parameters**: $\delta = 0.002$, Checkpoint Interval = $10,000$ records, Retraining Trigger Threshold = $50$ drift events.
- **Continuous State Export**: Always generates `data/processed/adwin_latest_result.json`, publishing status (`WAITING_FOR_CHECKPOINT`, `MONITORING`, or `RETRAINING_REQUIRED`), window width, estimated mean, and next checkpoint target.
- **Trigger Generation**: When cumulative drift events reach 50, it creates `data/processed/retraining_trigger.json` containing drift statistics, timestamp, and target history count to trigger retraining.

---

### 8. Distributed Candidate Retraining (PySpark)

#### [`pyspark_retraining.py`](file:///d:/adaptive%20network/pyspark_retraining.py)
Distributed candidate retraining engine activated when drift is confirmed:
- **Data Filtering**: Ingests all historical Parquet files from `data/history/` and filters exclusively for `status == "NORMAL"` traffic.
- **Isolated Version Staging**: Dynamically reads the next version identifier (`CANDIDATE_VERSION`) and stores all artifacts under `models/saved_models/candidates/v{version}/`:
  - `candidate_isolation_forest.pkl`
  - `candidate_autoencoder.pth`
  - `candidate_ae_threshold.pkl`
  - `candidate_ae_metadata.pkl`
  - `candidate_model_metadata.json`
- **Granular Progress Broadcasting**: Emits real-time progress percentages (e.g. 10% Spark load, 35% IF train, 55-70% AE epochs, 85% threshold calculation, 100% complete) to `automatic_pipeline_status.json`.
- **Zero In-Place Mutation**: Production models remain 100% untouched throughout training.

---

### 9. Champion/Challenger Model Evaluation

#### [`model_evaluation.py`](file:///d:/adaptive%20network/model_evaluation.py)
Executes comparative quality gate evaluation between the active production champion and the candidate challenger:
- **Strict Separation of Concerns**: Computes metrics and issues a decision report; it **never** mutates production models or modifies registry pointers.
- **Quality Gates**:
  - **F1 Score Gate**: Candidate F1 score must not drop by more than $2\%$ compared to production ($\Delta \text{F1} \ge -0.02$).
  - **False Alarm Rate Gate**: Candidate FAR must not increase by more than $2\%$ compared to production ($\Delta \text{FAR} \le +0.02$).
- **Report Generation**: Outputs `phase12_model_comparison.csv`, `phase12_promotion_report.csv`, and `phase12_model_metadata.json` with decision verdict (`ACCEPTED` or `REJECTED`) and explanatory reasoning.

---

### 10. Transactional Model Lifecycle & Promotion

#### [`model_lifecycle.py`](file:///d:/adaptive%20network/model_lifecycle.py)
Handles safe, atomic promotion or rejection following Phase 12 evaluation:
- **Collision Protection**: Asserts that `models/saved_models/production/v{new_version}` does not exist before starting.
- **Archive-Then-Promote Pattern**:
  1. Archives active production model files to `models/saved_models/archive/v{current_version}` (with collision disambiguation).
  2. Copies candidate files into `models/saved_models/production/v{new_version}`.
  3. Updates active root model files (`isolation_forest.pkl`, `autoencoder.pth`, etc.).
  4. Updates `models/saved_models/model_registry.json` (`current_production_version`, `previous_production_version`, `latest_candidate_version`, and model statuses `PROMOTED` / `ARCHIVED`).
- **Atomic Rollback**: If file copying fails midway, the partially created directory is removed and production remains intact.
- **Dashboard Synchronization**: Emits promotion outcome to `automatic_pipeline_status.json`.

---

### 11. Autonomous Retraining Orchestrator

#### [`automatic_retraining_orchestrator.py`](file:///d:/adaptive%20network/automatic_retraining_orchestrator.py)
The master closed-loop process manager orchestrating the complete lifecycle:
- Continuously monitors Parquet record counts in `data/history/`.
- At each 10k boundary, executes ADWIN drift analysis.
- If drift triggers, spawns PySpark retraining, executes Phase 12 evaluation, and triggers Phase 14 model lifecycle.
- Streams real-time operational messages to `data/processed/retraining_pipeline.log`.
- Broadcasts high-level state machine steps (`MONITORING` $\to$ `ADWIN_RUNNING` $\to$ `DRIFT_DETECTED` $\to$ `RETRAINING` $\to$ `EVALUATING` $\to$ `LIFECYCLE_DECISION`) to `automatic_pipeline_status.json`.

---

### 12. Telemetry Dashboard & Real-Time APIs

#### [`dashboard/app.py`](file:///d:/adaptive%20network/dashboard/app.py) & [`dashboard/dashboard_data.py`](file:///d:/adaptive%20network/dashboard/dashboard_data.py)
High-performance Flask telemetry server with real-time UI components:

**API Endpoints:**
| Endpoint | Method | Data Source | Description |
|---|---|---|---|
| `/` | `GET` | `dashboard.html` | Serves main monitoring dashboard |
| `/api/live` | `GET` | `data/live/latest_alert.json` | Instantaneous sub-second alert telemetry (`no-cache`) |
| `/api/pipeline-live` | `GET` | `data/processed/automatic_pipeline_status.json` | Real-time orchestrator stage, progress & message |
| `/api/adwin-live` | `GET` | `data/processed/adwin_latest_result.json` | Real-time ADWIN window width, mean & checkpoint |
| `/api/pipeline-log` | `GET` | `data/processed/retraining_pipeline.log` | Last 150 lines of orchestrator terminal output |
| `/api/system` | `GET` | Parquet History Aggregation | Total logs, normal count, anomaly count, anomaly rate |
| `/api/anomalies` | `GET` | `data/history/*.parquet` | Top 50 newest anomalies with scores & feature vectors |
| `/api/drift` | `GET` | `adwin_drift_results.csv` & State | ADWIN cumulative drift events and checkpoint history |
| `/api/retraining` | `GET` | Retraining Reports & Registry | Last retraining duration, record counts, metrics |
| `/api/models` | `GET` | `model_registry.json` | Model version history, statuses, and performance |
| `/api/evaluation` | `GET` | Phase 12 Reports | Champion vs. challenger comparison scorecard |
| `/api/dashboard` | `GET` | Aggregated Multi-Source | Full dashboard snapshot for initial page load |

**Frontend Architecture (`dashboard.js` & `dashboard.css`):**
- Independent polling loops: Live Alerts (1s), Orchestrator Pipeline (1s), ADWIN (1s), Model Registry (1s), Terminal Logs (500ms).
- Dynamic DOM diffing via response signature comparison to prevent unnecessary DOM reflows.
- Scrollable, compact live anomaly table with color-coded severity badges.
- Embedded interactive dark terminal showing live subprocess output.

---

## 🧠 Machine Learning Architecture

### 1. Isolation Forest (IF)
- **Role**: Fast partitioning of multidimensional feature space to isolate abnormal point anomalies.
- **Implementation**: `sklearn.ensemble.IsolationForest(contamination='auto', random_state=42)`
- **Score Transformation**: Raw decision function is inverted:
  $$S_{\text{IF}} = -\text{score\_samples}(X)$$
- **Weight in Fusion**: $20\%$ ($w_{\text{IF}} = 0.20$)

### 2. Deep PyTorch Autoencoder (AE)
- **Role**: Learns non-linear normal network flow manifold; high reconstruction error signals anomalous attack patterns.
- **Architecture**: Symmetric Bottleneck Feed-Forward Network:
  $$\text{Input (59)} \longrightarrow \text{Dense (64)} \longrightarrow \text{Dense (32)} \longrightarrow \text{Bottleneck (16)} \longrightarrow \text{Dense (32)} \longrightarrow \text{Dense (64)} \longrightarrow \text{Output (59)}$$
- **Activations**: ReLU on all hidden layers; identity on output.
- **Loss Metric**: Mean Squared Error (MSE):
  $$\text{MSE}(x, \hat{x}) = \frac{1}{d} \sum_{i=1}^{d} (x_i - \hat{x}_i)^2$$
- **Score Normalization**:
  $$S_{\text{AE}} = \min\left(\frac{\text{MSE}}{\text{Threshold}_{\text{AE}}}, 1.0\right)$$
- **Weight in Fusion**: $80\%$ ($w_{\text{AE}} = 0.80$)

### 3. Fusion & Classification Logic
$$S_{\text{final}} = (0.20 \cdot S_{\text{IF}}) + (0.80 \cdot S_{\text{AE}})$$
$$\text{Classification} = \begin{cases} \text{ANOMALY}, & \text{if } S_{\text{final}} \ge 0.85 \\ \text{NORMAL}, & \text{if } S_{\text{final}} < 0.85 \end{cases}$$

---

## 📨 Kafka Topics & Payload Schemas

| Topic | Publisher | Consumers | Key Fields |
|---|---|---|---|
| `raw_traffic` | `producer.py` | `processor.py` | 59 flow attributes + `Label` string |
| `processed_features` | `processor.py` | `inference_worker.py` | `features`: 59 scaled floats, `timestamp` |
| `anomaly_scores` | `inference_worker.py` | `fusion_worker.py` | `features`, `if_score`, `ae_error`, `ae_score`, `ae_anomaly`, `threshold`, `model_version`, `timestamp` |
| `fusion_scores` | `fusion_worker.py` | `alert_worker.py` | `features`, `if_score`, `ae_score`, `final_score`, `threshold`, `model_version`, `timestamp` |
| `alerts` | `alert_worker.py` | `parquet_storage.py` | `features`, `if_score`, `ae_score`, `final_score`, `threshold`, `status` (`ANOMALY`/`NORMAL`), `model_version`, `timestamp` |

---

## 🔄 Model Registry & Lifecycle State Machine

Model lifecycle states are tracked inside `models/saved_models/model_registry.json`:

```
                 [PySpark Retraining]
                          │
                          ▼
                    ┌───────────┐
                    │ CANDIDATE │
                    └─────┬─────┘
                          │
                [Phase 12 Evaluation]
                 (F1 & FAR Gate Check)
                     /         \
            REJECTED             ACCEPTED
               ▼                    ▼
        ┌────────────┐        ┌───────────┐
        │  REJECTED  │        │ PROMOTED  │
        │(Preserved) │        └─────┬─────┘
        └────────────┘              │
                            [Phase 14 Lifecycle]
                            (Copy to production/v{N})
                                    │
                                    ▼
                              ┌───────────┐
                              │  ACTIVE   │ ◀── [Inference Hot-Reloads]
                              └─────┬─────┘
                                    │
                         (Next Promotion Cycle)
                                    ▼
                              ┌───────────┐
                              │ ARCHIVED  │
                              └───────────┘
```

---

## 🗄️ Data Directory Layout

```
data/
├── raw/                              # 8 original CICIDS2017 raw CSV files (~885 MB total)
├── processed/                        # Cleaned CSVs, state files, and evaluation reports
│   ├── CICIDS2017_Cleaned.csv         # Cleaned traffic dataset (~883 MB)
│   ├── model_dataset.csv              # Model-ready dataset (~770 MB)
│   ├── adwin_drift_results.csv        # ADWIN drift events record log
│   ├── adwin_latest_result.json       # Live ADWIN window width & mean
│   ├── adwin_state.json               # Checkpoint state tracker
│   ├── retraining_trigger.json        # Trigger file emitted when drift threshold is reached
│   ├── automatic_pipeline_status.json # Master orchestrator live status JSON
│   ├── automatic_pipeline_history.json# Full historical execution timeline
│   ├── automatic_retraining_state.json# Retraining coordinator persistent state
│   ├── retraining_pipeline.log        # Live subprocess terminal log stream
│   ├── phase11_retraining_report.csv  # PySpark training execution report
│   ├── phase12_model_comparison.csv   # Old vs. candidate metric comparison
│   ├── phase12_model_metadata.json    # Evaluation run metadata & parameters
│   └── phase12_promotion_report.csv   # Promotion decision report
├── live/
│   └── latest_alert.json              # Sub-second live alert atomically updated by storage worker
├── history/                           # Parquet alert data lake (100 records per file)
└── features/                          # Feature engineering workspace
```

---

## 📦 Model Artifacts Directory Structure

```
models/saved_models/
├── isolation_forest.pkl              # Active production Isolation Forest
├── autoencoder.pth                   # Active production PyTorch Autoencoder
├── ae_threshold.pkl                  # Active production AE threshold
├── ae_metadata.pkl                   # Active production AE metadata (input_dim=59)
├── scaler.pkl                        # StandardScaler feature preprocessor
├── model_registry.json               # Comprehensive model version registry
├── model_version.json                # Active production version pointer
│
├── production/                       # Promoted production version snapshots
│   ├── v1/                           # Baseline production model artifacts
│   └── v2/                           # (Created upon candidate promotion)
│
├── candidates/                       # Staged candidate version snapshots
│   └── v2/                           # Staged candidate models & training metadata
│
└── archive/                          # Archived former production snapshots
    └── v1/                           # (Populated when v2 is promoted)
```

---

## ⚙️ System Configuration Reference

| Parameter | Default Value | Defining File | Operational Role |
|---|---|---|---|
| `BOOTSTRAP_SERVERS` | `localhost:9092` | All Kafka Modules | Kafka broker cluster address |
| `DELTA` | `0.002` | `adwin_drift_detection.py` | ADWIN drift detection sensitivity |
| `CHECKPOINT_SIZE` | `10,000` | `adwin_drift_detection.py` | Number of flows between ADWIN evaluations |
| `DRIFT_TRIGGER_COUNT`| `50` | `adwin_drift_detection.py` | Number of drift events required to trigger retraining |
| `IF_WEIGHT` | `0.20` | `fusion_worker.py`, `model_evaluation.py` | Isolation Forest weight in composite score |
| `AE_WEIGHT` | `0.80` | `fusion_worker.py`, `model_evaluation.py` | Autoencoder weight in composite score |
| `ALERT_THRESHOLD` | `0.85` | `alert_worker.py` | Threshold classifying flow as `ANOMALY` |
| `F1_TOLERANCE` | `0.02` | `model_evaluation.py` | Maximum permitted F1 degradation ($\le 2\%$) |
| `FAR_TOLERANCE` | `0.02` | `model_evaluation.py` | Maximum permitted False Alarm Rate rise ($\le 2\%$) |
| `PARQUET_BATCH_SIZE`| `100` | `parquet_storage.py` | Records per historical Parquet file |
| `EXPECTED_FEATURES` | `59` | All Preprocessing/Models | Numerical feature vector dimension |
| `DASHBOARD_PORT` | `5000` | `dashboard/app.py` | Flask HTTP web server port |
| `LIVE_REFRESH_INTERVAL`| `1000ms` | `dashboard/static/dashboard.js` | Polling interval for live alerts and status |
| `TERMINAL_REFRESH_INTERVAL`| `500ms` | `dashboard/static/dashboard.js` | Polling interval for live terminal log stream |

---

## 🚀 Getting Started & Execution Guide

### Prerequisites
- **Python**: 3.10+ (64-bit)
- **Apache Kafka**: 4.2.1+ (configured in KRaft mode)
- **Java JDK**: 17+ (required for Kafka and PySpark)
- **System Memory**: 8 GB RAM recommended

---

### Kafka Quick Setup (KRaft Mode)

Open a terminal in your Kafka installation directory (e.g. `~/kafka_2.13-4.2.1`):

```bash
# 1. Generate cluster storage UUID (run once)
bin/kafka-storage.sh random-uuid

# 2. Format storage directories using your cluster UUID
bin/kafka-storage.sh format -t <YOUR_CLUSTER_UUID> -c config/controller.properties
bin/kafka-storage.sh format -t <YOUR_CLUSTER_UUID> -c config/broker.properties

# 3. Start Controller and Broker (in separate terminals)
bin/kafka-server-start.sh config/controller.properties
bin/kafka-server-start.sh config/broker.properties

# 4. Create required topics
bin/kafka-topics.sh --create --topic raw_traffic --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic processed_features --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic anomaly_scores --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic fusion_scores --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic alerts --bootstrap-server localhost:9092
```

---

### Option A: One-Click Startup (Recommended for Windows)

Once Kafka is running:
1. Double-click [`start_project.bat`](file:///d:/adaptive%20network/start_project.bat) (or run `./start_project.bat` from PowerShell/CMD).
2. The script will automatically launch all 10 processes sequentially with 5-second graceful pauses between each.
3. Open your browser and navigate to:
   $$\text{\bf http://127.0.0.1:5000}$$

---

### Option B: Manual Terminal-by-Terminal Execution

To launch components individually across separate command terminals:

```bash
# Activate your virtual environment in each terminal:
.venv\Scripts\activate   # Windows
# source .venv/bin/activate # Linux / macOS

# [Terminal 1] Start the Kafka Producer
python kafka/producers/producer.py

# [Terminal 2] Start the Feature Processor
python flink/processor.py

# [Terminal 3] Start the Inference Worker (with hot-reloading)
python inference/inference_worker.py

# [Terminal 4] Start the Score Fusion Worker
python inference/fusion_worker.py

# [Terminal 5] Start the Fusion Diagnostic Consumer (optional)
python inference/fusion_consumer.py

# [Terminal 6] Start the Alert Classification Worker
python inference/alert_worker.py

# [Terminal 7] Start the Alert Console Display (optional)
python inference/alert_consumer.py

# [Terminal 8] Start the Two-Tier Parquet Storage Worker
python parquet_storage.py

# [Terminal 9] Start the Autonomous Retraining Orchestrator
python automatic_retraining_orchestrator.py

# [Terminal 10] Start the Flask Web Dashboard
python -m dashboard.app
```

Then visit: **`http://127.0.0.1:5000`**

---

## 📊 Dataset Specifications

The platform is evaluated and benchmarked on the **CICIDS2017** dataset from the Canadian Institute for Cybersecurity (UNB):

| Attribute | Specification |
|---|---|
| **Dataset Source** | Canadian Institute for Cybersecurity / UNB |
| **Duration** | 5 days of captured network traffic (Monday through Friday) |
| **Volume** | Over 2.8 million flow records across 8 raw capture files |
| **Cleaned Dataset** | `data/processed/CICIDS2017_Cleaned.csv` (~883 MB) |
| **Production Dataset** | `data/processed/model_dataset.csv` (~770 MB) |
| **Feature Dimensionality**| 59 continuous traffic features (durations, packet counts, flags, inter-arrival times, window sizes) |
| **Attack Classes** | DDoS, PortScan, DoS Hulk/GoldenEye, Web Attacks, Infiltration, Botnet, Heartbleed |
| **Normal Traffic** | Label: `BENIGN` |

---

## 📦 Dependencies

| Dependency | Version | Role in Platform |
|---|---|---|
| `torch` | 2.12.1 | Deep Learning Autoencoder model architecture & GPU/CPU tensor math |
| `scikit-learn` | 1.9.0 | Isolation Forest, StandardScaler, ROC AUC, precision, recall & F1 metrics |
| `pyspark` | 4.1.2 | Scalable distributed feature extraction & historical data lake retraining |
| `river` | 0.25.0 | ADWIN (Adaptive Windowing) statistical concept drift detection |
| `kafka-python` | 3.0.2 | High-throughput asynchronous Kafka producer and consumer clients |
| `pyarrow` | 24.0.0 | High-performance columnar Parquet file serialization and deserialization |
| `pandas` | 3.0.3 | Tabular data wrangling, cleaning, report generation, and matrix transformations |
| `numpy` | 2.4.6 | Fast vectorized numerical operations and array manipulation |
| `flask` | 3.1.0+ | Lightweight web application server for live telemetry dashboard & REST APIs |
| `joblib` | 1.5.3 | Fast model serialization and deserialization for scikit-learn estimators |
| `scipy` | 1.18.0 | Advanced statistical distributions, metrics, and distance measures |
| `matplotlib` | 3.11.0 | Offline visualization and notebook evaluation plotting |
| `seaborn` | 0.13.2 | Statistical distribution plotting in exploratory notebooks |
| `watchdog` | 6.0.0 | File system event monitoring |
| `tqdm` | 4.68.3 | Command line progress indicators |

Install all dependencies using:
```bash
pip install -r requirements.txt
```

---

<p align="center">
  <b>Adaptive Network Intrusion Detection System</b><br>
  <i>Engineered with Apache Kafka • PyTorch • PySpark • scikit-learn • River • Flask</i>
</p>
