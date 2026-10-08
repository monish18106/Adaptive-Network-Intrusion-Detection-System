"use strict";

/*
============================================================
REAL-TIME NETWORK INTRUSION DETECTION
DASHBOARD JAVASCRIPT
============================================================

DATA SOURCES

/api/dashboard
    Full dashboard / historical data

/api/live
    Latest live alert

/api/models
    Current production / candidate model versions

/api/drift
    ADWIN drift history

/api/adwin-live
    Latest ADWIN state

/api/pipeline-live
    Current automatic pipeline execution state

/api/pipeline-log
    Backend retraining pipeline log

IMPORTANT
------------------------------------------------------------
Historical dashboard data and live pipeline state are
different things.

Historical:
    /api/dashboard

Live:
    /api/live
    /api/models
    /api/drift
    /api/adwin-live
    /api/pipeline-live
    /api/pipeline-log
============================================================
*/


// ============================================================
// CONFIGURATION
// ============================================================

const DASHBOARD_INITIAL_LOAD = true;

const LIVE_REFRESH_INTERVAL = 1000;
const MODEL_REFRESH_INTERVAL = 1000;
const DRIFT_REFRESH_INTERVAL = 1000;
const ADWIN_REFRESH_INTERVAL = 1000;
const PIPELINE_REFRESH_INTERVAL = 1000;
const PIPELINE_LOG_REFRESH_INTERVAL = 500;


// ============================================================
// GLOBAL STATE
// ============================================================

let lastDashboardData = null;

let liveRefreshTimer = null;
let modelRefreshTimer = null;
let driftRefreshTimer = null;
let adwinRefreshTimer = null;
let pipelineRefreshTimer = null;
let pipelineLogRefreshTimer = null;

let liveRequestInProgress = false;
let modelRequestInProgress = false;
let driftRequestInProgress = false;
let adwinRequestInProgress = false;
let pipelineRequestInProgress = false;
let pipelineLogRequestInProgress = false;

let lastLiveSignature = null;
let lastModelSignature = null;
let lastDriftSignature = null;
let lastAdwinSignature = null;
let lastPipelineSignature = null;
let lastPipelineLogSignature = null;


// ============================================================
// LIVE COUNTERS
// ============================================================

let liveCounters = {
    total: 0,
    processed: 0,
    normal: 0,
    anomalies: 0
};


// ============================================================
// DOM HELPERS
// ============================================================

function $(id) {
    return document.getElementById(id);
}


function setText(id, value) {

    const element = $(id);

    if (!element) {
        return;
    }

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        element.textContent = "—";
        return;
    }

    element.textContent = String(value);
}


function setHTML(id, html) {

    const element = $(id);

    if (!element) {
        return;
    }

    element.innerHTML = html;
}


function safe(value, fallback = "—") {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return fallback;
    }

    return value;
}


function firstDefined(...values) {

    for (const value of values) {

        if (
            value !== null &&
            value !== undefined &&
            value !== ""
        ) {
            return value;
        }
    }

    return null;
}


// ============================================================
// NUMBER FORMATTERS
// ============================================================

function numberValue(value, fallback = null) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return fallback;
    }

    const number = Number(value);

    if (!Number.isFinite(number)) {
        return fallback;
    }

    return number;
}


function integer(value, fallback = "—") {

    const number = numberValue(value);

    if (number === null) {
        return fallback;
    }

    return Math.round(number).toLocaleString();
}


function decimal(
    value,
    digits = 4,
    fallback = "—"
) {

    const number = numberValue(value);

    if (number === null) {
        return fallback;
    }

    return number.toFixed(digits);
}


function percentage(
    value,
    digits = 2
) {

    const number = numberValue(value);

    if (number === null) {
        return "—";
    }

    return `${(number * 100).toFixed(digits)}%`;
}


// ============================================================
// VERSION FORMATTER
// ============================================================

function displayModelVersion(value) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "—";
    }

    const text = String(value);

    if (
        text.toLowerCase().startsWith("v")
    ) {
        return text;
    }

    return `v${text}`;
}


// ============================================================
// TIMESTAMP FORMATTER
// ============================================================

function formatTimestamp(value) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "—";
    }

    if (
        typeof value === "number" ||
        (
            typeof value === "string" &&
            /^-?\d+(\.\d+)?$/.test(value)
        )
    ) {

        let number = Number(value);

        if (Math.abs(number) < 100000000000) {
            number *= 1000;
        }

        const date = new Date(number);

        if (!Number.isNaN(date.getTime())) {
            return date.toLocaleString();
        }
    }

    const date = new Date(value);

    if (!Number.isNaN(date.getTime())) {
        return date.toLocaleString();
    }

    return String(value);
}


// ============================================================
// HTML ESCAPING
// ============================================================

function escapeHTML(value) {

    if (
        value === null ||
        value === undefined
    ) {
        return "";
    }

    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}


// ============================================================
// STATUS HELPERS
// ============================================================

function normalizeStatus(value) {

    return String(
        safe(value, "WAITING")
    )
        .trim()
        .toUpperCase();
}


function statusClass(status) {

    const value =
        normalizeStatus(status);

    if (
        value.includes("ERROR") ||
        value.includes("FAILED") ||
        value.includes("REJECT") ||
        value.includes("STOPPED")
    ) {
        return "badge-danger";
    }

    if (
        value.includes("COMPLETED") ||
        value.includes("PROMOTED") ||
        value.includes("PROMOTE") ||
        value.includes("PASS") ||
        value === "ACTIVE" ||
        value === "RUNNING" ||
        value === "DETECTED"
    ) {
        return "badge-success";
    }

    if (
        value.includes("TRIGGER") ||
        value.includes("TRAIN") ||
        value.includes("DRIFT") ||
        value.includes("PROCESS") ||
        value.includes("ONGOING") ||
        value.includes("WAITING")
    ) {
        return "badge-warning";
    }

    return "badge-neutral";
}


function statusBadge(status) {

    const value =
        normalizeStatus(status);

    return `
        <span class="badge ${statusClass(value)}">
            ${escapeHTML(value)}
        </span>
    `;
}


// ============================================================
// API
// ============================================================

async function apiGet(url) {

    const response =
        await fetch(
            url,
            {
                method: "GET",
                cache: "no-store",
                headers: {
                    "Accept": "application/json"
                }
            }
        );

    if (
        response.status === 404 ||
        response.status === 503
    ) {
        return {
            available: false
        };
    }

    if (!response.ok) {

        throw new Error(
            `${url} returned HTTP ${response.status}`
        );
    }

    const data =
        await response.json();

    if (
        !data ||
        typeof data !== "object"
    ) {
        throw new Error(
            `${url} returned invalid JSON`
        );
    }

    return data;
}


async function getDashboard() {
    return apiGet("/api/dashboard");
}


async function getLive() {
    return apiGet("/api/live");
}


async function getModels() {
    return apiGet("/api/models");
}


async function getDrift() {
    return apiGet("/api/drift");
}


async function getAdwinLive() {
    return apiGet("/api/adwin-live");
}


async function getPipelineLive() {
    return apiGet("/api/pipeline-live");
}


async function getPipelineLog() {
    return apiGet("/api/pipeline-log");
}


// ============================================================
// SYSTEM
// ============================================================

function initializeLiveCounters(data) {

    const system =
        data?.system || {};

    const summary =
        data?.summary || {};

    liveCounters.total =
        numberValue(
            firstDefined(
                system.total_logs,
                summary.total_logs,
                0
            ),
            0
        );

    liveCounters.processed =
        numberValue(
            firstDefined(
                system.logs_processed,
                summary.logs_processed,
                summary.processed,
                liveCounters.total
            ),
            liveCounters.total
        );

    liveCounters.normal =
        numberValue(
            firstDefined(
                system.normal_logs,
                summary.normal_logs,
                summary.normal,
                0
            ),
            0
        );

    liveCounters.anomalies =
        numberValue(
            firstDefined(
                system.anomalies_detected,
                summary.anomalies_detected,
                summary.anomalies,
                0
            ),
            0
        );
}


function renderLiveCounters() {

    setText(
        "totalLogs",
        integer(liveCounters.total, "0")
    );

    setText(
        "processedLogs",
        integer(liveCounters.processed, "0")
    );

    setText(
        "normalLogs",
        integer(liveCounters.normal, "0")
    );

    setText(
        "anomalyCount",
        integer(liveCounters.anomalies, "0")
    );

    setText(
        "anomalyTotal",
        integer(liveCounters.anomalies, "0")
    );
}


function renderSystem(data) {

    const system =
        data?.system || {};

    const summary =
        data?.summary || {};

    const versions =
        data?.version_management || {};

    const totalLogs =
        firstDefined(
            system.total_logs,
            summary.total_logs,
            0
        );

    const processedLogs =
        firstDefined(
            system.logs_processed,
            summary.logs_processed,
            summary.processed,
            totalLogs
        );

    const normalLogs =
        firstDefined(
            system.normal_logs,
            summary.normal_logs,
            summary.normal,
            0
        );

    const anomalyCount =
        firstDefined(
            system.anomalies_detected,
            summary.anomalies_detected,
            summary.anomalies,
            0
        );

    setText(
        "totalLogs",
        integer(totalLogs, "0")
    );

    setText(
        "processedLogs",
        integer(processedLogs, "0")
    );

    setText(
        "normalLogs",
        integer(normalLogs, "0")
    );

    setText(
        "anomalyCount",
        integer(anomalyCount, "0")
    );

    const systemStatus =
        firstDefined(
            system.system_status,
            system.status,
            "RUNNING"
        );

    const normalized =
        normalizeStatus(systemStatus);

    /*
    IMPORTANT:
    Actual HTML IDs are:
        systemStatusText
        systemStatusDot

    NOT:
        systemStatus
        systemDot
    */

    setText(
        "systemStatusText",
        normalized
    );

    const dot =
        $("systemStatusDot");

    if (dot) {

        dot.classList.remove(
            "online",
            "offline",
            "warning"
        );

        if (
            normalized.includes("RUN") ||
            normalized.includes("ACTIVE") ||
            normalized.includes("ONLINE")
        ) {
            dot.classList.add("online");

        } else if (
            normalized.includes("WAIT") ||
            normalized.includes("WARN")
        ) {
            dot.classList.add("warning");

        } else {
            dot.classList.add("offline");
        }
    }

    const production =
        firstDefined(
            versions.current_production_version,
            system.current_production_version,
            system.production_version,
            summary.current_production_version
        );

    setText(
        "metadataProduction",
        displayModelVersion(production)
    );

    const timestamp =
        firstDefined(
            system.timestamp,
            data.timestamp
        );

    if (timestamp !== null) {

        setText(
            "lastUpdate",
            `Updated ${formatTimestamp(timestamp)}`
        );
    }
}


// ============================================================
// PIPELINE
// ============================================================

function getCurrentPipelineStage(pipeline) {

    const stage =
        normalizeStatus(
            firstDefined(
                pipeline.stage,
                "MONITORING"
            )
        );

    if (
        stage.includes("PYSPARK") ||
        stage.includes("RETRAIN")
    ) {
        return "RETRAINING";
    }

    if (
        stage.includes("EVALUATION")
    ) {
        return "EVALUATION";
    }

    if (
        stage.includes("LIFECYCLE")
    ) {
        return "LIFECYCLE";
    }

    if (
        stage.includes("PIPELINE COMPLETE")
    ) {
        return "LIFECYCLE";
    }

    return "ADWIN";
}


function resetPipelineStages() {

    [
        "pipelineStepMonitoring",
        "pipelineStepRetraining",
        "pipelineStepEvaluation",
        "pipelineStepLifecycle"
    ].forEach(
        id => {

            const element =
                $(id);

            if (!element) {
                return;
            }

            element.classList.remove(
                "active",
                "completed",
                "running",
                "waiting",
                "failed",
                "rejected"
            );

            element.classList.add(
                "waiting"
            );
        }
    );
}


function renderPipeline(data) {

    /*
     * ========================================================
     * AUTOMATIC MODEL LIFECYCLE
     * ========================================================
     *
     * This section is intentionally STATIC.
     *
     * It explains the self-correction workflow:
     *
     * 1. ADWIN Monitoring
     * 2. PySpark Retraining
     * 3. Model Evaluation
     * 4. Model Lifecycle
     *
     * It does NOT display:
     *
     * - WAITING
     * - RUNNING
     * - COMPLETED
     * - ACTIVE
     *
     * Only the numerical/model information below remains live:
     *
     * - Checkpoint
     * - Drift Count
     * - Current Production
     * - Latest Candidate
     */


    const pipeline =
        data?.pipeline ||
        data?.pipeline_status ||
        {};

    const drift =
        data?.drift ||
        {};

    const models =
        data?.models ||
        {};

    const versions =
        data?.version_management ||
        {};


    // ========================================================
    // CHECKPOINT
    // ========================================================

    const checkpoint =
        firstDefined(
            pipeline.checkpoint,
            pipeline.last_checkpoint,
            drift.checkpoint,
            data?.drift_checkpoint,
            0
        );


    // ========================================================
    // DRIFT COUNT
    // ========================================================

    const driftCount =
        firstDefined(
            drift.total_drifts,
            drift.drift_count,
            drift.total,
            pipeline.drift_count,
            pipeline.total_drifts,
            data?.total_drifts,
            data?.drift_count,
            0
        );


    // ========================================================
    // CURRENT PRODUCTION
    //
    // IMPORTANT:
    // Use models / version_management first.
    // The previous code only checked pipeline fields,
    // which caused "—" even when the registry had a version.
    // ========================================================

    const production =
        firstDefined(
            models.current_production_version,
            versions.current_production_version,
            pipeline.current_production_version,
            pipeline.production_version,
            data?.current_production_version
        );


    // ========================================================
    // LATEST CANDIDATE
    // ========================================================

    const candidate =
        firstDefined(
            models.latest_candidate_version,
            versions.latest_candidate_version,
            pipeline.latest_candidate_version,
            pipeline.candidate_version,
            data?.latest_candidate_version
        );


    // ========================================================
    // TOP BADGE
    // ========================================================

    setHTML(
        "pipelineStatusBadge",
        statusBadge(
            "AUTOMATED WORKFLOW"
        )
    );


    // ========================================================
    // LIVE VALUES
    // ========================================================

    setText(
        "pipelineCheckpoint",
        integer(
            checkpoint,
            "0"
        )
    );

    setText(
        "pipelineDriftCount",
        integer(
            driftCount,
            "0"
        )
    );

    setText(
        "pipelineCurrentProduction",
        displayModelVersion(
            production
        )
    );

    setText(
        "pipelineLatestCandidate",
        displayModelVersion(
            candidate
        )
    );


    // ========================================================
    // STATIC WORKFLOW
    // ========================================================

    const monitoring =
        $("pipelineStepMonitoring");

    const monitoringStatus =
        $("pipelineMonitoringStatus");

    const retraining =
        $("pipelineStepRetraining");

    const retrainingStatus =
        $("pipelineRetrainingStatus");

    const evaluation =
        $("pipelineStepEvaluation");

    const evaluationStatus =
        $("pipelineEvaluationStatus");

    const lifecycle =
        $("pipelineStepLifecycle");

    const lifecycleStatus =
        $("pipelineLifecycleStatus");


    // --------------------------------------------------------
    // Remove all execution-state classes
    // --------------------------------------------------------

    [
        monitoring,
        retraining,
        evaluation,
        lifecycle
    ].forEach(
        element => {

            if (!element) {
                return;
            }

            element.classList.remove(
                "active",
                "completed",
                "running",
                "waiting",
                "failed",
                "rejected"
            );
        }
    );


    // --------------------------------------------------------
    // 01 — ADWIN MONITORING
    // --------------------------------------------------------

    if (monitoringStatus) {

        monitoringStatus.textContent =
            "Detects changes in incoming traffic with ADWIN.";
    }


    // --------------------------------------------------------
    // 02 — PYSPARK RETRAINING
    // --------------------------------------------------------

    if (retrainingStatus) {

        retrainingStatus.textContent =
            "Trains a new candidate model with recent normal data.";
    }


    // --------------------------------------------------------
    // 03 — MODEL EVALUATION
    // --------------------------------------------------------

    if (evaluationStatus) {

        evaluationStatus.textContent =
            "Compares the candidate model with the production model.";
    }


    // --------------------------------------------------------
    // 04 — MODEL LIFECYCLE
    // --------------------------------------------------------

    if (lifecycleStatus) {

        lifecycleStatus.textContent =
            "Promotes accepted models and archives the previous version.";
    }


    // ========================================================
    // TERMINAL STATUS
    //
    // This is no longer an execution status.
    // It simply describes the purpose of the section.
    // ========================================================

    setText(
        "pipelineTerminalStatus",
        "Automatic model self-correction workflow"
    );


    // Keep the backend log separate.
    //
    // Do NOT overwrite the static workflow with pipeline
    // execution messages.
}


// ============================================================
// LIVE PIPELINE
// ============================================================

function renderPipelineLive(data) {

    /*
     * ========================================================
     * LIVE PIPELINE DATA
     * ========================================================
     *
     * The Automatic Model Lifecycle cards are STATIC.
     *
     * Therefore this function may update only:
     *
     * - Checkpoint
     * - Drift Count
     * - Current Production
     * - Latest Candidate
     * - Backend pipeline log message
     *
     * It must NOT call renderPipeline().
     */


    if (
        !data ||
        data.available !== true
    ) {
        return;
    }


    const pipeline =
        data.pipeline_status ||
        {};


    // ========================================================
    // CHECKPOINT
    // ========================================================

    const checkpoint =
        firstDefined(
            pipeline.checkpoint,
            pipeline.last_checkpoint
        );


    if (
        checkpoint !== null &&
        checkpoint !== undefined &&
        checkpoint !== ""
    ) {

        setText(
            "pipelineCheckpoint",
            integer(
                checkpoint,
                "0"
            )
        );
    }


    // ========================================================
    // DRIFT COUNT
    // ========================================================

    const driftCount =
        firstDefined(
            pipeline.drift_count,
            pipeline.total_drifts,
            pipeline.new_drift_count
        );


    if (
        driftCount !== null &&
        driftCount !== undefined &&
        driftCount !== ""
    ) {

        setText(
            "pipelineDriftCount",
            integer(
                driftCount,
                "0"
            )
        );
    }


    // ========================================================
    // CURRENT PRODUCTION
    // ========================================================

    const production =
        firstDefined(
            pipeline.current_production_version,
            pipeline.production_version
        );


    if (
        production !== null &&
        production !== undefined &&
        production !== ""
    ) {

        setText(
            "pipelineCurrentProduction",
            displayModelVersion(
                production
            )
        );
    }


    // ========================================================
    // LATEST CANDIDATE
    // ========================================================

    const candidate =
        firstDefined(
            pipeline.latest_candidate_version,
            pipeline.candidate_version
        );


    if (
        candidate !== null &&
        candidate !== undefined &&
        candidate !== ""
    ) {

        setText(
            "pipelineLatestCandidate",
            displayModelVersion(
                candidate
            )
        );
    }


    // ========================================================
    // PIPELINE MESSAGE
    //
    // Keep this separate from the static workflow cards.
    // ========================================================

    const message =
        firstDefined(
            pipeline.message
        );


    if (
        message !== null &&
        message !== undefined &&
        message !== ""
    ) {

        setText(
            "pipelineLogUpdated",
            String(message)
        );
    }
}


// ============================================================
// DRIFT
// ============================================================

function renderDrift(data) {

    const drift =
        data?.drift &&
        typeof data.drift === "object"
            ? data.drift
            : {};

    let records = [];

    if (
        Array.isArray(drift.records)
    ) {
        records = drift.records;

    } else if (
        Array.isArray(drift.history)
    ) {
        records = drift.history;

    } else if (
        Array.isArray(drift.events)
    ) {
        records = drift.events;

    } else if (
        Array.isArray(data?.drift_history)
    ) {
        records = data.drift_history;
    }

    const totalDrifts =
        firstDefined(
            drift.total_drifts,
            drift.drift_count,
            drift.total,
            drift.count,
            data?.total_drifts,
            data?.drift_count,
            records.length,
            0
        );

    const newDrifts =
        firstDefined(
            drift.new_drifts,
            drift.new_drift_count,
            drift.recent_drifts,
            data?.new_drifts,
            data?.new_drift_count,
            0
        );

    let latest =
        firstDefined(
            drift.latest_drift,
            drift.latest_event,
            drift.latest,
            data?.latest_drift,
            data?.latest_drift_event
        );

    if (
        !latest &&
        records.length > 0
    ) {
        latest =
            records[
                records.length - 1
            ];
    }

    setText(
        "totalDrifts",
        integer(totalDrifts, "0")
    );

    setText(
        "newDriftCount",
        integer(newDrifts, "0")
    );

    const checkpoint =
        firstDefined(
            drift.checkpoint,
            data?.drift_checkpoint,
            0
        );

    setText(
        "pipelineDriftCount",
        integer(
            totalDrifts,
            "0"
        )
    );

    setText(
        "pipelineCheckpoint",
        integer(
            checkpoint,
            "0"
        )
    );

    if (
        latest &&
        typeof latest === "object"
    ) {

        const record =
            firstDefined(
                latest.record_index,
                latest.record,
                latest.record_id,
                drift.latest_record,
                data?.latest_drift_record
            );

        const checkpoint =
            firstDefined(
                latest.checkpoint,
                latest.last_checkpoint,
                drift.checkpoint,
                data?.drift_checkpoint
            );

        const timestamp =
            firstDefined(
                latest.timestamp,
                latest.detected_at,
                latest.time,
                latest.created_at
            );

        const score =
            firstDefined(
                latest.final_score,
                latest.score,
                latest.drift_score,
                drift.latest_score
            );

        const estimation =
            firstDefined(
                latest.adwin_estimation,
                latest.adwin,
                latest.estimation,
                drift.adwin_estimation
            );

        const width =
            firstDefined(
                latest.adwin_width,
                latest.width,
                latest.window_width,
                drift.adwin_width
            );

        setText(
            "latestDriftRecord",
            integer(record, "—")
        );

        setText(
            "driftCheckpoint",
            integer(checkpoint, "—")
        );

        setHTML(
            "latestDrift",
            `
                <div class="event-grid">

                    <div>
                        <span>TIME</span>
                        <strong>
                            ${escapeHTML(
                                formatTimestamp(timestamp)
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>RECORD</span>
                        <strong>
                            ${escapeHTML(
                                integer(record, "—")
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>SCORE</span>
                        <strong>
                            ${escapeHTML(
                                decimal(score, 4)
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>ADWIN</span>
                        <strong>
                            ${escapeHTML(
                                decimal(estimation, 4)
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>WINDOW WIDTH</span>
                        <strong>
                            ${escapeHTML(
                                integer(width, "—")
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>STATUS</span>
                        <strong>
                            ${statusBadge(
                                "DRIFT DETECTED"
                            )}
                        </strong>
                    </div>

                </div>
            `
        );

        setHTML(
            "driftBadge",
            statusBadge("DETECTED")
        );

    } else {

        setText(
            "latestDriftRecord",
            "—"
        );

        setText(
            "driftCheckpoint",
            "—"
        );

        setHTML(
            "latestDrift",
            `
                <div class="empty-state">
                    No drift event loaded.
                </div>
            `
        );

        setHTML(
            "driftBadge",
            statusBadge(
                Number(totalDrifts) > 0
                    ? "DETECTED"
                    : "WAITING"
            )
        );
    }

    renderDriftHistory(records);
}


function renderDriftHistory(records) {

    const container =
        $("driftHistory");

    if (!container) {
        return;
    }

    if (!records.length) {

        container.innerHTML = `
            <div class="empty-state">
                No drift events recorded.
            </div>
        `;

        return;
    }

    container.innerHTML =
        records
            .slice()
            .reverse()
            .map(
                (record, index) => {

                    const recordNumber =
                        records.length - index;

                    return `
                        <div class="drift-event">

                            <div class="drift-event-header">

                                <strong>
                                    Drift #${recordNumber}
                                </strong>

                                <span>
                                    ${escapeHTML(
                                        formatTimestamp(
                                            firstDefined(
                                                record.timestamp,
                                                record.detected_at,
                                                record.time
                                            )
                                        )
                                    )}
                                </span>

                            </div>

                            <div class="drift-event-grid">

                                <div>
                                    <span>RECORD</span>
                                    <strong>
                                        ${escapeHTML(
                                            integer(
                                                firstDefined(
                                                    record.record_index,
                                                    record.record
                                                )
                                            )
                                        )}
                                    </strong>
                                </div>

                                <div>
                                    <span>SCORE</span>
                                    <strong>
                                        ${escapeHTML(
                                            decimal(
                                                firstDefined(
                                                    record.final_score,
                                                    record.score
                                                ),
                                                4
                                            )
                                        )}
                                    </strong>
                                </div>

                                <div>
                                    <span>ADWIN</span>
                                    <strong>
                                        ${escapeHTML(
                                            decimal(
                                                firstDefined(
                                                    record.adwin_estimation,
                                                    record.adwin
                                                ),
                                                4
                                            )
                                        )}
                                    </strong>
                                </div>

                                <div>
                                    <span>WIDTH</span>
                                    <strong>
                                        ${escapeHTML(
                                            integer(
                                                firstDefined(
                                                    record.adwin_width,
                                                    record.width
                                                )
                                            )
                                        )}
                                    </strong>
                                </div>

                            </div>

                        </div>
                    `;
                }
            )
            .join("");
}


// ============================================================
// RETRAINING
// ============================================================

function renderRetraining(data) {

    const retraining =
        data?.retraining || {};

    const available =
        retraining.available === true;

    setHTML(
        "retrainingBadge",
        statusBadge(
            available
                ? "AVAILABLE"
                : "WAITING"
        )
    );

    setText(
        "lastRetraining",
        formatTimestamp(
            firstDefined(
                retraining.timestamp,
                retraining.metadata?.timestamp,
                retraining.latest?.timestamp
            )
        )
    );

    const trainingTime =
        firstDefined(
            retraining.training_time_seconds,
            retraining.training_time,
            retraining.metadata?.training_time_seconds
        );

    setText(
        "trainingTime",
        trainingTime === null
            ? "—"
            : `${decimal(trainingTime, 2)} s`
    );

    /*
    IMPORTANT:
    This is the actual number of NORMAL records used
    for candidate training.

    It must NOT use Phase 12 evaluation_records.
    */

    const trainingRecords =
        firstDefined(
            retraining.training_records,
            retraining.records,
            retraining.metadata?.training_records,
            retraining.latest?.training_records
        );

    setText(
        "trainingRecords",
        trainingRecords === null
            ? "—"
            : integer(trainingRecords)
    );

    const candidateVersion =
        firstDefined(
            retraining.latest_candidate_version,
            retraining.metadata?.version,
            retraining.version
        );

    setText(
        "retrainingModelVersion",
        displayModelVersion(candidateVersion)
    );
}


// ============================================================
// MODEL SUMMARY
// ============================================================

function renderModels(data) {

    const models =
        data?.models &&
        typeof data.models === "object"
            ? data.models
            : {};

    const versions =
        data?.version_management &&
        typeof data.version_management === "object"
            ? data.version_management
            : {};

    const production =
        firstDefined(
            models.current_production_version,
            versions.current_production_version,
            data?.current_production_version
        );

    const candidate =
        firstDefined(
            models.latest_candidate_version,
            versions.latest_candidate_version,
            data?.latest_candidate_version
        );

    const decision =
        firstDefined(
            models.last_decision,
            versions.last_decision,
            data?.last_decision,
            "WAITING"
        );

    setText(
        "currentProductionModel",
        displayModelVersion(production)
    );

    setText(
        "latestCandidateModel",
        displayModelVersion(candidate)
    );

    setHTML(
        "lifecycleDecision",
        statusBadge(decision)
    );

    const metadata =
        data?.model_metadata || {};

    const metadataVersion =
        firstDefined(
            metadata.version,
            production
        );

    const metadataModel =
        firstDefined(
            metadata.model_name,
            metadata.model_type,
            metadata.model,
            "Isolation Forest + Autoencoder"
        );

    const metadataFeatures =
        firstDefined(
            metadata.features,
            metadata.feature_count,
            59
        );

    const metadataThreshold =
        firstDefined(
            metadata.ae_threshold,
            metadata.threshold,
            metadata.reconstruction_threshold
        );

    const metadataCreated =
        firstDefined(
            metadata.created_at,
            metadata.training_timestamp,
            metadata.timestamp
        );

    setText(
        "modelType",
        metadataModel
    );

    setText(
        "modelVersion",
        displayModelVersion(metadataVersion)
    );

    setText(
        "modelCreated",
        formatTimestamp(metadataCreated)
    );

    setText(
        "modelFeatures",
        integer(metadataFeatures, "59")
    );

    setText(
        "modelThreshold",
        decimal(metadataThreshold, 4)
    );

    setHTML(
        "modelStatus",
        statusBadge("PRODUCTION")
    );


    // --------------------------------------------------------
    // TOP MODEL
    // --------------------------------------------------------

    const top =
        data?.top_model &&
        typeof data.top_model === "object"
            ? data.top_model
            : {};

    const topName =
        firstDefined(
            top.model_name,
            top.model,
            top.name,
            metadataModel,
            "Isolation Forest + Autoencoder"
        );

    const topVersion =
        firstDefined(
            top.version,
            production
        );

    const topScore =
        firstDefined(
            top.score,
            top.f1,
            top.metric,
            top.accuracy
        );

    setText(
        "topModelName",
        topName
    );

    setText(
        "topModelVersion",
        displayModelVersion(topVersion)
    );

    setText(
        "topModelScore",
        decimal(topScore, 4)
    );

    setHTML(
        "topModelBadge",
        statusBadge("ACTIVE")
    );
}


// ============================================================
// MODEL METADATA CONTAINER
// ============================================================

function renderModelMetadata(data) {

    const metadata =
        data?.model_metadata || {};

    const container =
        $("modelMetadataContainer");

    if (!container) {
        return;
    }

    const production =
        metadata.production || {};

    const candidate =
        metadata.candidate || {};

    const models =
        Array.isArray(metadata.models)
            ? metadata.models
            : [];

    const scoreConfig =
        metadata.score_configuration || {};

    const entries = [];

    if (
        Object.keys(production).length
    ) {
        entries.push({
            title: "Production Model",
            data: production
        });
    }

    if (
        Object.keys(candidate).length
    ) {
        entries.push({
            title: "Candidate Model",
            data: candidate
        });
    }

    if (
        !entries.length &&
        models.length
    ) {

        models.forEach(
            (model, index) => {

                entries.push({
                    title: `Model ${index + 1}`,
                    data: model
                });
            }
        );
    }

    if (!entries.length) {

        container.innerHTML = `
            <div class="empty-state">
                No model metadata available.
            </div>
        `;

        return;
    }

    let html = "";

    entries.forEach(
        entry => {

            const model =
                entry.data || {};

            html += `
                <div class="model-card">

                    <div class="model-card-header">

                        <div>
                            <strong>
                                ${escapeHTML(entry.title)}
                            </strong>

                            <small>
                                ${escapeHTML(
                                    firstDefined(
                                        model.model_type,
                                        model.type,
                                        "MODEL"
                                    )
                                )}
                            </small>
                        </div>

                    </div>

                    <div class="model-metadata-grid">

                        <div>
                            <span>VERSION</span>
                            <strong>
                                ${escapeHTML(
                                    displayModelVersion(
                                        model.version
                                    )
                                )}
                            </strong>
                        </div>

                        <div>
                            <span>F1</span>
                            <strong>
                                ${escapeHTML(
                                    decimal(
                                        firstDefined(
                                            model.f1,
                                            model.metrics?.f1
                                        ),
                                        4
                                    )
                                )}
                            </strong>
                        </div>

                        <div>
                            <span>ACCURACY</span>
                            <strong>
                                ${escapeHTML(
                                    decimal(
                                        firstDefined(
                                            model.accuracy,
                                            model.metrics?.accuracy
                                        ),
                                        4
                                    )
                                )}
                            </strong>
                        </div>

                        <div>
                            <span>DETECTION RATE</span>
                            <strong>
                                ${escapeHTML(
                                    decimal(
                                        firstDefined(
                                            model.detection_rate,
                                            model.metrics?.detection_rate
                                        ),
                                        4
                                    )
                                )}
                            </strong>
                        </div>

                        <div>
                            <span>FAR</span>
                            <strong>
                                ${escapeHTML(
                                    decimal(
                                        firstDefined(
                                            model.false_alarm_rate,
                                            model.far,
                                            model.metrics?.false_alarm_rate
                                        ),
                                        4
                                    )
                                )}
                            </strong>
                        </div>

                        <div>
                            <span>STATUS</span>
                            <strong>
                                ${statusBadge(
                                    firstDefined(
                                        model.status,
                                        entry.title
                                    )
                                )}
                            </strong>
                        </div>

                    </div>

                </div>
            `;
        }
    );

    html += `
        <div class="model-card">

            <div class="model-card-header">

                <div>
                    <strong>
                        Fusion Configuration
                    </strong>

                    <small>
                        IF + AE scoring
                    </small>
                </div>

            </div>

            <div class="model-metadata-grid">

                <div>
                    <span>IF WEIGHT</span>
                    <strong>
                        ${escapeHTML(
                            decimal(
                                scoreConfig.if_weight,
                                4
                            )
                        )}
                    </strong>
                </div>

                <div>
                    <span>AE WEIGHT</span>
                    <strong>
                        ${escapeHTML(
                            decimal(
                                scoreConfig.ae_weight,
                                4
                            )
                        )}
                    </strong>
                </div>

                <div>
                    <span>FUSION THRESHOLD</span>
                    <strong>
                        ${escapeHTML(
                            decimal(
                                scoreConfig.fusion_threshold,
                                4
                            )
                        )}
                    </strong>
                </div>

                <div>
                    <span>F1 TOLERANCE</span>
                    <strong>
                        ${escapeHTML(
                            decimal(
                                scoreConfig.f1_tolerance,
                                4
                            )
                        )}
                    </strong>
                </div>

                <div>
                    <span>FAR TOLERANCE</span>
                    <strong>
                        ${escapeHTML(
                            decimal(
                                scoreConfig.false_alarm_tolerance,
                                4
                            )
                        )}
                    </strong>
                </div>

                <div>
                    <span>EVALUATION RECORDS</span>
                    <strong>
                        ${escapeHTML(
                            integer(
                                scoreConfig.evaluation_records
                            )
                        )}
                    </strong>
                </div>

            </div>

        </div>
    `;

    container.innerHTML = html;
}


// ============================================================
// LIFECYCLE
// ============================================================

function renderLifecycle(data) {

    /*
     * ========================================================
     * MODEL LIFECYCLE
     * ========================================================
     *
     * This is an architectural explanation, not a live
     * execution-state indicator.
     *
     * Production and candidate versions remain live.
     *
     * The lifecycle decision is shown as automated rather
     * than WAITING.
     */


    const models =
        data?.models ||
        {};

    const versions =
        data?.version_management ||
        {};

    const pipeline =
        data?.pipeline ||
        data?.pipeline_status ||
        {};


    // ========================================================
    // CURRENT PRODUCTION
    // ========================================================

    const production =
        firstDefined(
            models.current_production_version,
            versions.current_production_version,
            pipeline.current_production_version,
            pipeline.production_version
        );


    // ========================================================
    // LATEST CANDIDATE
    // ========================================================

    const candidate =
        firstDefined(
            models.latest_candidate_version,
            versions.latest_candidate_version,
            pipeline.latest_candidate_version,
            pipeline.candidate_version
        );


    // ========================================================
    // DISPLAY MODEL VERSIONS
    // ========================================================

    setText(
        "currentProductionModel",
        displayModelVersion(
            production
        )
    );

    setText(
        "latestCandidateModel",
        displayModelVersion(
            candidate
        )
    );


    // ========================================================
    // AUTOMATED LIFECYCLE
    // ========================================================

    setHTML(
        "lifecycleDecision",
        statusBadge(
            "AUTOMATED"
        )
    );
}


// ============================================================
// EVALUATION
// ============================================================

function renderEvaluation(data) {

    const evaluation =
        data?.evaluation ||
        data?.model_evaluation ||
        {};

    const metadata =
        evaluation.metadata || {};

    const oldMetrics =
        evaluation.old_metrics ||
        metadata.old_metrics ||
        {};

    const candidateMetrics =
        evaluation.candidate_metrics ||
        metadata.candidate_metrics ||
        {};

    const decision =
        firstDefined(
            evaluation.decision,
            metadata.decision,
            "—"
        );

    const evaluationTime =
        firstDefined(
            evaluation.evaluation_time,
            metadata.evaluation_time,
            metadata.total_evaluation_time
        );

    /*
    Phase 12 currently does not calculate ROC-AUC.

    Therefore N/A is intentional.
    */

    const rocAuc =
        firstDefined(
            evaluation.roc_auc,
            metadata.roc_auc
        );

    setText(
        "evaluationAccuracy",
        percentage(
            candidateMetrics.accuracy
        )
    );

    setText(
        "evaluationPrecision",
        percentage(
            candidateMetrics.precision
        )
    );

    setText(
        "evaluationRecall",
        percentage(
            candidateMetrics.detection_rate
        )
    );

    setText(
        "evaluationF1",
        percentage(
            candidateMetrics.f1
        )
    );

    setText(
        "evaluationRocAuc",
        rocAuc === null
            ? "N/A"
            : decimal(rocAuc, 4)
    );

    setText(
        "evaluationTime",
        evaluationTime === null
            ? "—"
            : `${decimal(evaluationTime, 2)} s`
    );

    const decisionElement =
        $("evaluationDecision");

    if (decisionElement) {

        decisionElement.innerHTML =
            statusBadge(decision);
    }

    const comparison =
        evaluation.comparison || [];

    const body =
        $("evaluationTableBody");

    if (
        body &&
        Array.isArray(comparison)
    ) {

        if (!comparison.length) {

            body.innerHTML = `
                <tr>
                    <td colspan="7">
                        No evaluation comparison available.
                    </td>
                </tr>
            `;

        } else {

            body.innerHTML =
                comparison
                    .map(
                        row => `
                            <tr>

                                <td>
                                    ${escapeHTML(
                                        safe(row.model)
                                    )}
                                </td>

                                <td>
                                    ${escapeHTML(
                                        displayModelVersion(
                                            row.version
                                        )
                                    )}
                                </td>

                                <td>
                                    ${escapeHTML(
                                        percentage(
                                            row.accuracy
                                        )
                                    )}
                                </td>

                                <td>
                                    ${escapeHTML(
                                        percentage(
                                            row.precision
                                        )
                                    )}
                                </td>

                                <td>
                                    ${escapeHTML(
                                        percentage(
                                            row.detection_rate
                                        )
                                    )}
                                </td>

                                <td>
                                    ${escapeHTML(
                                        percentage(
                                            row.f1
                                        )
                                    )}
                                </td>

                                <td>
                                    ${escapeHTML(
                                        percentage(
                                            row.false_alarm_rate
                                        )
                                    )}
                                </td>

                            </tr>
                        `
                    )
                    .join("");
        }
    }
}


// ============================================================
// ANOMALIES
// ============================================================

function renderAnomalies(data) {

    const anomalyData =
        data?.anomalies;

    let records = [];

    if (
        Array.isArray(anomalyData)
    ) {

        records =
            anomalyData;

    } else if (
        anomalyData &&
        Array.isArray(anomalyData.records)
    ) {

        records =
            anomalyData.records;
    }

    const total =
        firstDefined(
            anomalyData?.total,
            anomalyData?.count,
            data?.system?.anomalies_detected,
            records.length
        );

    setText(
        "anomalyCountBadge",
        integer(total, "0")
    );

    setText(
        "anomalyCount",
        integer(total, "0")
    );

    const tbody =
        $("anomalyTableBody");

    if (!tbody) {
        return;
    }

    if (!records.length) {

        tbody.innerHTML = `
            <tr>
                <td
                    colspan="7"
                    class="empty-table-cell"
                >
                    No anomalies detected.
                </td>
            </tr>
        `;

        return;
    }

    tbody.innerHTML =
        records
            .slice()
            .reverse()
            .map(
                record =>
                    createAnomalyRow(record)
            )
            .join("");
}


function createAnomalyRow(record) {

    const featureCount =
        firstDefined(
            record.feature_count,
            Array.isArray(record.features)
                ? record.features.length
                : null
        );

    const featureText =
        firstDefined(
            record.feature_text,
            featureCount !== null
                ? `${featureCount} features`
                : null,
            "—"
        );

    return `
        <tr>

            <!-- TIME -->
            <td>
                ${escapeHTML(
                    formatTimestamp(
                        firstDefined(
                            record.timestamp,
                            record.time,
                            record.created_at
                        )
                    )
                )}
            </td>

            <!-- STATUS -->
            <td>
                ${statusBadge(
                    firstDefined(
                        record.status,
                        record.anomaly_status,
                        "ANOMALY"
                    )
                )}
            </td>

            <!-- AUTOENCODER -->
            <td>
                ${escapeHTML(
                    decimal(
                        firstDefined(
                            record.ae_score,
                            record.autoencoder_score,
                            record.ae_anomaly_score
                        ),
                        4
                    )
                )}
            </td>

            <!-- ISOLATION FOREST -->
            <td>
                ${escapeHTML(
                    decimal(
                        firstDefined(
                            record.if_score,
                            record.if_anomaly_score,
                            record.isolation_forest_score
                        ),
                        4
                    )
                )}
            </td>

            <!-- FUSION SCORE -->
            <td>
                ${escapeHTML(
                    decimal(
                        firstDefined(
                            record.final_score,
                            record.fusion_score,
                            record.fused_score
                        ),
                        4
                    )
                )}
            </td>

            <!-- THRESHOLD -->
            <td>
                ${escapeHTML(
                    decimal(
                        firstDefined(
                            record.threshold,
                            record.fusion_threshold
                        ),
                        4
                    )
                )}
            </td>

            <!-- FEATURES -->
            <td
                class="feature-cell"
                title="${escapeHTML(featureText)}"
            >
                ${escapeHTML(featureText)}
            </td>

        </tr>
    `;
}


// ============================================================
// LIVE ALERT
// ============================================================

function updateLiveStatus(alert) {

    const status =
        normalizeStatus(
            firstDefined(
                alert?.status,
                "WAITING"
            )
        );

    /*
    Actual dashboard IDs:
        systemStatusText
        systemStatusDot
    */

    setText(
        "systemStatusText",
        "LIVE"
    );

    const dot =
        $("systemStatusDot");

    if (dot) {

        dot.classList.remove(
            "offline",
            "warning"
        );

        dot.classList.add(
            "online"
        );
    }

    let liveElement =
        $("liveStatus");

    if (!liveElement) {

        liveElement =
            document.createElement("div");

        liveElement.id =
            "liveStatus";

        liveElement.style.position =
            "fixed";

        liveElement.style.top =
            "12px";

        liveElement.style.right =
            "12px";

        liveElement.style.zIndex =
            "9999";

        liveElement.style.padding =
            "7px 12px";

        liveElement.style.borderRadius =
            "6px";

        liveElement.style.fontSize =
            "12px";

        liveElement.style.fontFamily =
            "monospace";

        liveElement.style.fontWeight =
            "600";

        liveElement.style.background =
            "rgba(0,0,0,0.75)";

        liveElement.style.pointerEvents =
            "none";

        document.body.appendChild(
            liveElement
        );
    }

    liveElement.textContent =
        `● LIVE  ${status}`;

    liveElement.style.color =
        status === "ANOMALY"
            ? "#ff6b6b"
            : "#6ee7b7";
}


function updateLiveCounters(alert) {

    liveCounters.total += 1;
    liveCounters.processed += 1;

    const status =
        normalizeStatus(
            alert?.status
        );

    if (
        status === "ANOMALY"
    ) {
        liveCounters.anomalies += 1;
    } else {
        liveCounters.normal += 1;
    }

    renderLiveCounters();
}


function updateLiveAnomalyTable(alert) {

    if (
        normalizeStatus(
            alert?.status
        ) !== "ANOMALY"
    ) {
        return;
    }

    const tbody =
        $("anomalyTableBody");

    if (!tbody) {
        return;
    }

    const empty =
        tbody.querySelector(
            ".empty-table-cell"
        );

    if (empty) {
        tbody.innerHTML = "";
    }

    const row =
        document.createElement("tr");

    row.innerHTML =
        createAnomalyRow(alert);

    tbody.prepend(row);

    const rows =
        Array.from(
            tbody.querySelectorAll("tr")
        );

    if (rows.length > 100) {

        rows
            .slice(100)
            .forEach(
                row => row.remove()
            );
    }
}


function renderLiveAlert(data) {

    if (
        !data ||
        data.available !== true ||
        !data.latest_alert
    ) {
        return;
    }

    const alert =
        data.latest_alert;

    const signature =
        [
            alert.timestamp,
            alert.if_score,
            alert.ae_score,
            alert.final_score,
            alert.threshold,
            alert.status
        ]
            .map(
                value =>
                    String(
                        value ?? ""
                    )
            )
            .join("|");

    if (
        lastLiveSignature === null
    ) {

        lastLiveSignature =
            signature;

        updateLiveStatus(alert);

        return;
    }

    if (
        signature === lastLiveSignature
    ) {

        updateLiveStatus(alert);

        return;
    }

    lastLiveSignature =
        signature;

    updateLiveStatus(alert);

    updateLiveCounters(alert);

    if (
        normalizeStatus(
            alert.status
        ) === "ANOMALY"
    ) {

        updateLiveAnomalyTable(
            alert
        );
    }
}


// ============================================================
// ADWIN LIVE
// ============================================================

function renderAdwinLive(data) {

    if (
        !data ||
        data.available !== true
    ) {
        return;
    }

    const result =
        data.adwin_result || {};

    const totalDrifts =
        firstDefined(
            result.drift_count,
            result.total_drifts,
            result.drift_events,
            0
        );

    const newDrifts =
        firstDefined(
            result.new_drift_count,
            result.new_drifts,
            result.new_count,
            result.drift_detected === true
                ? 1
                : 0
        );

    const latestRecord =
        firstDefined(
            result.last_drift_record,
            result.latest_drift_record
        );

    const latestScore =
        firstDefined(
            result.last_drift_score,
            result.latest_drift_score
        );

    const checkpoint =
        firstDefined(
            result.checkpoint,
            result.current_checkpoint
        );

    const status =
        firstDefined(
            result.status,
            "WAITING_FOR_CHECKPOINT"
        );

    const detected =
        result.drift_detected === true;

    const timestamp =
        firstDefined(
            result.timestamp,
            result.last_updated
        );

    const estimation =
        firstDefined(
            result.adwin_estimation
        );

    const width =
        firstDefined(
            result.adwin_width
        );

    setText(
        "totalDrifts",
        integer(totalDrifts, "0")
    );

    setText(
        "newDriftCount",
        integer(newDrifts, "0")
    );

    setText(
        "pipelineDriftCount",
        integer(
            totalDrifts,
            "0"
        )
    );

    setText(
        "pipelineCheckpoint",
        integer(
            checkpoint,
            "0"
        )
    );

    setText(
        "latestDriftRecord",
        integer(latestRecord, "—")
    );

    setText(
        "driftCheckpoint",
        integer(checkpoint, "—")
    );

    setHTML(
        "driftBadge",
        statusBadge(
            detected ||
            Number(totalDrifts) > 0
                ? "DETECTED"
                : status
        )
    );

    if (
        latestRecord !== null ||
        latestScore !== null ||
        detected
    ) {

        setHTML(
            "latestDrift",
            `
                <div class="event-grid">

                    <div>
                        <span>TIME</span>
                        <strong>
                            ${escapeHTML(
                                formatTimestamp(
                                    timestamp
                                )
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>RECORD</span>
                        <strong>
                            ${escapeHTML(
                                integer(
                                    latestRecord,
                                    "—"
                                )
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>SCORE</span>
                        <strong>
                            ${escapeHTML(
                                decimal(
                                    latestScore,
                                    4
                                )
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>ADWIN</span>
                        <strong>
                            ${escapeHTML(
                                decimal(
                                    estimation,
                                    4
                                )
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>WINDOW WIDTH</span>
                        <strong>
                            ${escapeHTML(
                                integer(
                                    width,
                                    "—"
                                )
                            )}
                        </strong>
                    </div>

                    <div>
                        <span>STATUS</span>
                        <strong>
                            ${statusBadge(
                                detected
                                    ? "DRIFT DETECTED"
                                    : status
                            )}
                        </strong>
                    </div>

                </div>
            `
        );
    }
}


// ============================================================
// PIPELINE LOG
// ============================================================

function renderPipelineLog(data) {

    const container =
        $("pipelineTerminal");

    if (!container) {
        return;
    }

    const log =
        data?.pipeline_log ||
        data ||
        {};

    const records =
        Array.isArray(log)
            ? log
            : Array.isArray(log.records)
                ? log.records
                : [];

    if (!records.length) {

        container.innerHTML = `
            <div class="terminal-line">

                <span class="terminal-prefix">
                    SYSTEM
                </span>

                <span class="terminal-message">
                    Waiting for backend pipeline activity...
                </span>

            </div>
        `;

        return;
    }

    const visible =
        records.slice(-100);

    container.innerHTML =
        visible
            .map(
                record => {

                    let timestamp = "";
                    let message = "";

                    if (
                        typeof record === "string"
                    ) {

                        message =
                            record;

                    } else {

                        timestamp =
                            firstDefined(
                                record.timestamp,
                                record.time,
                                ""
                            );

                        message =
                            firstDefined(
                                record.message,
                                record.event,
                                record.status,
                                ""
                            );
                    }

                    return `
                        <div class="terminal-line">

                            <span class="terminal-prefix">
                                ${escapeHTML(
                                    timestamp
                                )}
                            </span>

                            <span class="terminal-message">
                                ${escapeHTML(
                                    message
                                )}
                            </span>

                        </div>
                    `;
                }
            )
            .join("");

    requestAnimationFrame(
        () => {

            container.scrollTop =
                container.scrollHeight;
        }
    );
}


// ============================================================
// SELF CORRECTION
// ============================================================

function renderSelfCorrection(data) {

    const state =
        data?.self_correction || {};

    if (
        state.available === false
    ) {
        return;
    }

    const message =
        firstDefined(
            state.message,
            state.current_operation,
            state.status,
            state.state
        );

    if (
        message !== null
    ) {

        /*
        Compatibility display only.

        This does NOT control retraining.
        */

        const element =
            $("pipelineMessage");

        if (element) {
            element.textContent =
                String(message);
        }
    }
}


// ============================================================
// FULL DASHBOARD
// ============================================================

function renderDashboard(data) {

    lastDashboardData =
        data;

    initializeLiveCounters(
        data
    );

    renderLiveCounters();

    renderSystem(
        data
    );

    renderPipeline(
        data
    );

    renderDrift(
        data
    );

    renderRetraining(
        data
    );

    renderModels(
        data
    );

    renderLifecycle(
        data
    );

    renderModelMetadata(
        data
    );

    renderEvaluation(
        data
    );

    renderAnomalies(
        data
    );

    renderSelfCorrection(
        data
    );

    renderPipelineLog(
        data
    );
}


// ============================================================
// LOADERS
// ============================================================

async function loadDashboard() {

    try {

        const start =
            performance.now();

        const data =
            await getDashboard();

        renderDashboard(
            data
        );

        const elapsed =
            performance.now() -
            start;

        console.log(
            `[Dashboard] Loaded in ${elapsed.toFixed(0)} ms`
        );

        startLiveRefresh();
        startModelRefresh();
        startDriftRefresh();
        startAdwinRefresh();
        startPipelineRefresh();
        startPipelineLogRefresh();

    } catch (error) {

        showError(error);

        /*
        Live endpoints can still be attempted even if
        the historical dashboard request fails.
        */

        startLiveRefresh();
        startModelRefresh();
        startDriftRefresh();
        startAdwinRefresh();
        startPipelineRefresh();
        startPipelineLogRefresh();
    }
}


async function loadLive() {

    if (liveRequestInProgress) {
        return;
    }

    liveRequestInProgress = true;

    try {

        const data =
            await getLive();

        if (
            data &&
            data.available === true
        ) {

            renderLiveAlert(data);
        }

    } catch (error) {

        console.warn(
            "Live update unavailable:",
            error
        );

    } finally {

        liveRequestInProgress =
            false;
    }
}


async function loadModelsLive() {

    if (modelRequestInProgress) {
        return;
    }

    modelRequestInProgress = true;

    try {

        const data =
            await getModels();

        if (
            !data ||
            data.available === false
        ) {
            return;
        }

        const signature =
            JSON.stringify({
                production:
                    data.current_production_version,

                candidate:
                    data.latest_candidate_version,

                decision:
                    data.last_decision,

                updated:
                    data.last_updated
            });

        if (
            signature ===
            lastModelSignature
        ) {
            return;
        }

        lastModelSignature =
            signature;

        const production =
            data.current_production_version;

        const candidate =
            data.latest_candidate_version;

        const decision =
            data.last_decision;

        /*
        Only update the actual version fields.
        Do not overwrite metadata with this lightweight endpoint.
        */

        if (
            production !== null &&
            production !== undefined &&
            production !== ""
        ) {

            setText(
                "currentProductionModel",
                displayModelVersion(
                    production
                )
            );

            setText(
                "pipelineCurrentProduction",
                displayModelVersion(
                    production
                )
            );
        }

        if (
            candidate !== null &&
            candidate !== undefined &&
            candidate !== ""
        ) {

            setText(
                "latestCandidateModel",
                displayModelVersion(
                    candidate
                )
            );

            setText(
                "pipelineLatestCandidate",
                displayModelVersion(
                    candidate
                )
            );
        }

        

    } catch (error) {

        console.warn(
            "Model update unavailable:",
            error
        );

    } finally {

        modelRequestInProgress =
            false;
    }
}


async function loadDriftLive() {

    if (driftRequestInProgress) {
        return;
    }

    driftRequestInProgress = true;

    try {

        const data =
            await getDrift();

        if (
            !data ||
            data.available === false
        ) {
            return;
        }

        const signature =
            JSON.stringify(
                data.records ||
                data.history ||
                data
            );

        if (
            signature ===
            lastDriftSignature
        ) {
            return;
        }

        lastDriftSignature =
            signature;

        renderDrift({
            drift: data
        });

    } catch (error) {

        console.warn(
            "Drift update unavailable:",
            error
        );

    } finally {

        driftRequestInProgress =
            false;
    }
}


async function loadAdwinLive() {

    if (adwinRequestInProgress) {
        return;
    }

    adwinRequestInProgress = true;

    try {

        const data =
            await getAdwinLive();

        if (
            !data ||
            data.available === false
        ) {
            return;
        }

        const signature =
            String(
                data.adwin_file_updated ??
                JSON.stringify(
                    data.adwin_result ||
                    {}
                )
            );

        if (
            signature ===
            lastAdwinSignature
        ) {
            return;
        }

        lastAdwinSignature =
            signature;

        renderAdwinLive(data);

    } catch (error) {

        console.warn(
            "ADWIN update unavailable:",
            error
        );

    } finally {

        adwinRequestInProgress =
            false;
    }
}


async function loadPipelineLive() {

    if (pipelineRequestInProgress) {
        return;
    }

    pipelineRequestInProgress = true;

    try {

        const data =
            await getPipelineLive();

        if (
            !data ||
            data.available === false
        ) {
            return;
        }

        const pipeline =
            data.pipeline_status ||
            {};

        const signature =
            JSON.stringify(pipeline);

        if (
            signature ===
            lastPipelineSignature
        ) {
            return;
        }

        lastPipelineSignature =
            signature;

        renderPipelineLive(data);

    } catch (error) {

        console.warn(
            "Pipeline live update unavailable:",
            error
        );

    } finally {

        pipelineRequestInProgress =
            false;
    }
}


async function loadPipelineLog() {

    if (pipelineLogRequestInProgress) {
        return;
    }

    pipelineLogRequestInProgress =
        true;

    try {

        const data =
            await getPipelineLog();

        if (
            !data ||
            data.available === false
        ) {
            return;
        }

        const records =
            Array.isArray(data.records)
                ? data.records
                : [];

        const signature =
            String(
                data.pipeline_log_updated ??
                records
                    .map(
                        record =>
                            typeof record === "string"
                                ? record
                                : `${record.timestamp ?? ""}|${record.message ?? ""}`
                    )
                    .join("\n")
            );

        if (
            signature ===
            lastPipelineLogSignature
        ) {
            return;
        }

        lastPipelineLogSignature =
            signature;

        renderPipelineLog({
            pipeline_log: {
                records
            }
        });

    } catch (error) {

        console.warn(
            "Pipeline log update unavailable:",
            error
        );

    } finally {

        pipelineLogRequestInProgress =
            false;
    }
}


// ============================================================
// REFRESH CONTROL
// ============================================================

function startLiveRefresh() {

    stopLiveRefresh();

    loadLive();

    liveRefreshTimer =
        setInterval(
            loadLive,
            LIVE_REFRESH_INTERVAL
        );
}


function stopLiveRefresh() {

    if (liveRefreshTimer) {

        clearInterval(
            liveRefreshTimer
        );

        liveRefreshTimer =
            null;
    }
}


function startModelRefresh() {

    stopModelRefresh();

    loadModelsLive();

    modelRefreshTimer =
        setInterval(
            loadModelsLive,
            MODEL_REFRESH_INTERVAL
        );
}


function stopModelRefresh() {

    if (modelRefreshTimer) {

        clearInterval(
            modelRefreshTimer
        );

        modelRefreshTimer =
            null;
    }
}


function startDriftRefresh() {

    stopDriftRefresh();

    loadDriftLive();

    driftRefreshTimer =
        setInterval(
            loadDriftLive,
            DRIFT_REFRESH_INTERVAL
        );
}


function stopDriftRefresh() {

    if (driftRefreshTimer) {

        clearInterval(
            driftRefreshTimer
        );

        driftRefreshTimer =
            null;
    }
}


function startAdwinRefresh() {

    stopAdwinRefresh();

    loadAdwinLive();

    adwinRefreshTimer =
        setInterval(
            loadAdwinLive,
            ADWIN_REFRESH_INTERVAL
        );
}


function stopAdwinRefresh() {

    if (adwinRefreshTimer) {

        clearInterval(
            adwinRefreshTimer
        );

        adwinRefreshTimer =
            null;
    }
}


function startPipelineRefresh() {

    stopPipelineRefresh();

    loadPipelineLive();

    pipelineRefreshTimer =
        setInterval(
            loadPipelineLive,
            PIPELINE_REFRESH_INTERVAL
        );
}


function stopPipelineRefresh() {

    if (pipelineRefreshTimer) {

        clearInterval(
            pipelineRefreshTimer
        );

        pipelineRefreshTimer =
            null;
    }
}


function startPipelineLogRefresh() {

    stopPipelineLogRefresh();

    loadPipelineLog();

    pipelineLogRefreshTimer =
        setInterval(
            loadPipelineLog,
            PIPELINE_LOG_REFRESH_INTERVAL
        );
}


function stopPipelineLogRefresh() {

    if (pipelineLogRefreshTimer) {

        clearInterval(
            pipelineLogRefreshTimer
        );

        pipelineLogRefreshTimer =
            null;
    }
}


// ============================================================
// ERROR HANDLING
// ============================================================

function showError(error) {

    console.error(
        "Dashboard error:",
        error
    );

    /*
    Actual HTML IDs:
        systemStatusText
        systemStatusDot
    */

    setText(
        "systemStatusText",
        "DATA ERROR"
    );

    const dot =
        $("systemStatusDot");

    if (dot) {

        dot.classList.remove(
            "online",
            "warning"
        );

        dot.classList.add(
            "offline"
        );
    }
}


// ============================================================
// PAGE VISIBILITY
// ============================================================

document.addEventListener(
    "visibilitychange",
    () => {

        if (document.hidden) {

            stopLiveRefresh();
            stopModelRefresh();
            stopDriftRefresh();
            stopAdwinRefresh();
            stopPipelineRefresh();
            stopPipelineLogRefresh();

        } else {

            startLiveRefresh();
            startModelRefresh();
            startDriftRefresh();
            startAdwinRefresh();
            startPipelineRefresh();
            startPipelineLogRefresh();
        }
    }
);


// ============================================================
// INITIALIZATION
// ============================================================

document.addEventListener(
    "DOMContentLoaded",
    () => {

        if (
            DASHBOARD_INITIAL_LOAD
        ) {

            loadDashboard();
        }
    }
);