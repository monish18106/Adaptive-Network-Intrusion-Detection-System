@echo off
title Adaptive Network - Full Pipeline Launcher

echo ============================================================
echo       ADAPTIVE NETWORK INTRUSION DETECTION SYSTEM
echo ============================================================
echo.
echo Starting pipeline components in order...
echo 5 second gap between each component.
echo.

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo ERROR: Virtual environment Python not found.
    echo Expected:
    echo %~dp0.venv\Scripts\python.exe
    pause
    exit /b 1
)

echo Virtual environment:
echo %~dp0.venv\Scripts\python.exe
echo.


echo [1/10] Starting Kafka Producer...
start "1 - Kafka Producer" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" kafka\producers\producer.py"

timeout /t 5 /nobreak >nul


echo [2/10] Starting Flink Processor...
start "2 - Flink Processor" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" flink\processor.py"

timeout /t 5 /nobreak >nul


echo [3/10] Starting Inference Worker...
start "3 - Inference Worker" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" inference\inference_worker.py"

timeout /t 5 /nobreak >nul


echo [4/10] Starting Fusion Worker...
start "4 - Fusion Worker" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" inference\fusion_worker.py"

timeout /t 5 /nobreak >nul


echo [5/10] Starting Fusion Consumer...
start "5 - Fusion Consumer" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" inference\fusion_consumer.py"

timeout /t 5 /nobreak >nul


echo [6/10] Starting Alert Worker...
start "6 - Alert Worker" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" inference\alert_worker.py"

timeout /t 5 /nobreak >nul


echo [7/10] Starting Alert Consumer...
start "7 - Alert Consumer" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" inference\alert_consumer.py"

timeout /t 5 /nobreak >nul


echo [8/10] Starting Parquet Storage...
start "8 - Parquet Storage" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" parquet_storage.py"

timeout /t 5 /nobreak >nul


echo [9/10] Starting Automatic Retraining Orchestrator...
start "9 - Automatic Retraining" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" automatic_retraining_orchestrator.py"

timeout /t 5 /nobreak >nul


echo [10/10] Starting Flask Dashboard...
start "10 - Flask Dashboard" cmd /k "cd /d "%~dp0" && ".venv\Scripts\python.exe" -m dashboard.app"


echo.
echo ============================================================
echo          ALL COMPONENTS STARTED SUCCESSFULLY
echo ============================================================
echo.
echo Dashboard:
echo http://127.0.0.1:5000
echo.
echo Each component is running in its own terminal.
echo ============================================================
echo.
pause