@echo off
REM ── stop-all.bat ────────────────────────────────────────────────────────────
REM Gracefully stop Schema Registry and Kafka broker.
REM ────────────────────────────────────────────────────────────────────────────

set KAFKA_HOME=D:\kafka
set CONFLUENT_HOME=D:\confluent

echo ============================================================
echo   Stopping all services
echo ============================================================
echo.

REM ── Stop Schema Registry ────────────────────────────────────────────────────
echo [INFO] Stopping Schema Registry ...
if exist "%CONFLUENT_HOME%\bin\schema-registry-stop.bat" (
    call "%CONFLUENT_HOME%\bin\schema-registry-stop.bat" >nul 2>&1
) else if exist "%CONFLUENT_HOME%\bin\schema-registry-stop" (
    call "%CONFLUENT_HOME%\bin\schema-registry-stop" >nul 2>&1
)
REM Also kill by window title as a fallback
taskkill /FI "WINDOWTITLE eq Schema Registry*" /F >nul 2>&1
echo [OK] Schema Registry stopped.

REM ── Stop Kafka broker ──────────────────────────────────────────────────────
echo [INFO] Stopping Kafka broker ...
if exist "%KAFKA_HOME%\bin\windows\kafka-server-stop.bat" (
    call "%KAFKA_HOME%\bin\windows\kafka-server-stop.bat" >nul 2>&1
)
REM Also kill by window title as a fallback
taskkill /FI "WINDOWTITLE eq Kafka Broker*" /F >nul 2>&1
echo [OK] Kafka stopped.

echo.
echo ============================================================
echo   All services stopped.
echo ============================================================
