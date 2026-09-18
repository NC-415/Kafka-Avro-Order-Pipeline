@echo off
REM ── start-kafka.bat ─────────────────────────────────────────────────────────
REM Start Apache Kafka in KRaft mode (no ZooKeeper) and create topics.
REM
REM Prerequisites:
REM   - Java 17+ installed and on PATH
REM   - Kafka extracted to D:\kafka
REM
REM Usage:
REM   start-kafka.bat         (runs in foreground)
REM ────────────────────────────────────────────────────────────────────────────

set KAFKA_HOME=D:\kafka
set KAFKA_CONFIG=%KAFKA_HOME%\config\kraft\server.properties
set KAFKA_LOG_DIR=%KAFKA_HOME%\kafka-logs
set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G

echo ============================================================
echo   Kafka Startup (KRaft mode, single-node)
echo ============================================================
echo.

REM ── Check Java ──────────────────────────────────────────────────────────────
java -version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Java not found on PATH. Install JDK 17+ first.
    exit /b 1
)

REM ── Check Kafka installation ────────────────────────────────────────────────
if not exist "%KAFKA_HOME%\bin\windows\kafka-server-start.bat" (
    echo [ERROR] Kafka not found at %KAFKA_HOME%
    echo         Download from https://kafka.apache.org/downloads
    echo         Extract to D:\kafka
    exit /b 1
)

REM ── Format storage (first time only) ────────────────────────────────────────
set KAFKA_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qw
if not exist "%KAFKA_LOG_DIR%\meta.properties" (
    echo [INFO] First run — formatting KRaft storage...
    call "%KAFKA_HOME%\bin\windows\kafka-storage.bat" format ^
        -t %KAFKA_CLUSTER_ID% ^
        -c "%KAFKA_CONFIG%"
    echo.
)

REM ── Start Kafka broker ──────────────────────────────────────────────────────
echo [INFO] Starting Kafka broker on localhost:9092 ...
echo [INFO] Press Ctrl+C to stop.
echo.

start "Kafka Broker" /B %KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_CONFIG%

REM ── Wait for broker to be ready ─────────────────────────────────────────────
echo [INFO] Waiting for broker to be ready...
:wait_loop
timeout /t 2 /nobreak >nul 2>&1
call "%KAFKA_HOME%\bin\windows\kafka-topics.bat" --bootstrap-server localhost:9092 --list >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo        ... still starting
    goto wait_loop
)
echo [OK] Broker is ready.
echo.

REM ── Create topics ───────────────────────────────────────────────────────────
echo [INFO] Creating topic: orders (3 partitions, RF=1)
call "%KAFKA_HOME%\bin\windows\kafka-topics.bat" --bootstrap-server localhost:9092 --create ^
    --if-not-exists ^
    --topic orders ^
    --partitions 3 ^
    --replication-factor 1 ^
    --config retention.ms=604800000

echo [INFO] Creating topic: orders.DLQ (3 partitions, RF=1, 14-day retention)
call "%KAFKA_HOME%\bin\windows\kafka-topics.bat" --bootstrap-server localhost:9092 --create ^
    --if-not-exists ^
    --topic orders.DLQ ^
    --partitions 3 ^
    --replication-factor 1 ^
    --config retention.ms=1209600000

echo.
echo [INFO] Topics created:
call "%KAFKA_HOME%\bin\windows\kafka-topics.bat" --bootstrap-server localhost:9092 --list

echo.
echo ============================================================
echo   Kafka is running. Do NOT close this window.
echo   Broker:  localhost:9092
echo   Topics:  orders, orders.DLQ
echo ============================================================
echo.

REM Keep alive — wait for Ctrl+C
pause >nul
