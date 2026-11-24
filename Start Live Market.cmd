@echo off
setlocal
cd /d "%~dp0" || exit /b 1
set "PY=.venv\Scripts\python.exe"

echo Starting the live market data pipeline...
docker info >nul 2>&1
if errorlevel 1 (
    docker desktop start
    if errorlevel 1 goto failed
)
docker compose --profile file-replay stop publisher
if errorlevel 1 goto failed
docker compose up -d --build timescale-writer candle-builder
if errorlevel 1 goto failed
"%PY%" -c "import confluent_kafka" >nul 2>&1
if errorlevel 1 "%PY%" -m pip install "confluent-kafka>=2.6,<3"
if errorlevel 1 goto failed

"%PY%" -m playwright install firefox
if errorlevel 1 goto failed
"%PY%" -m data_fetch.upstox.auto_login --fresh-login
if errorlevel 1 goto failed

echo Starting the live feed. Press Ctrl+C to stop collecting.
"%PY%" -m data_fetch.upstox.data_retrival
if errorlevel 1 goto failed
exit /b 0

:failed
echo Startup or collection stopped. See the error above.
pause
exit /b 1
