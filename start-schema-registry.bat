@echo off
REM ── start-schema-registry.bat ───────────────────────────────────────────────
REM Start Confluent Schema Registry pointing at the local Kafka broker.
REM
REM Prerequisites:
REM   - Java 17+ installed and on PATH
REM   - Confluent Platform extracted to D:\confluent
REM   - Kafka already running on localhost:9092
REM
REM Usage:
REM   start-schema-registry.bat    (runs in foreground)
REM ────────────────────────────────────────────────────────────────────────────

set CONFLUENT_HOME=D:\confluent
set SR_CONFIG=%CONFLUENT_HOME%\etc\schema-registry\schema-registry.properties

echo ============================================================
echo   Schema Registry Startup
echo ============================================================
echo.

REM ── Check Java ──────────────────────────────────────────────────────────────
java -version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Java not found on PATH.
    exit /b 1
)

REM ── Check installation ─────────────────────────────────────────────────────
if not exist "%CONFLUENT_HOME%\bin\schema-registry-start" (
    if not exist "%CONFLUENT_HOME%\bin\schema-registry-start.bat" (
        echo [ERROR] Schema Registry not found at %CONFLUENT_HOME%
        echo         Download from https://packages.confluent.io/archive/7.7/
        exit /b 1
    )
)

REM ── Wait for Kafka to be ready ──────────────────────────────────────────────
echo [INFO] Checking Kafka at localhost:9092 ...
:wait_kafka
powershell -Command "try { $c = New-Object System.Net.Sockets.TcpClient('localhost', 9092); $c.Close(); exit 0 } catch { exit 1 }" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo        ... Kafka not ready, waiting 3s
    timeout /t 3 /nobreak >nul 2>&1
    goto wait_kafka
)
echo [OK] Kafka is reachable.
echo.

REM ── Start Schema Registry (Native Java Launch) ─────────────────────────────
echo [INFO] Starting Schema Registry on http://localhost:8081 ...
echo [INFO] Press Ctrl+C to stop.
echo.

setlocal EnableDelayedExpansion
set CP=
for /d %%d in ("%CONFLUENT_HOME%\share\java\*") do (
    if defined CP (
        set "CP=!CP!;%%d\*"
    ) else (
        set "CP=%%d\*"
    )
)

java -cp "!CP!" io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain "%SR_CONFIG%"

echo.
echo Schema Registry stopped.
endlocal
