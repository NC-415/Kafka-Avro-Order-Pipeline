@echo off
echo =======================================================
echo        Assignment 3: Automated Demo Orchestrator
echo =======================================================
echo.
echo This script will open all the required terminal windows
echo automatically so you can easily record your screen.
echo.
echo [1/4] Starting Kafka...
start "Kafka Broker" cmd /k ".\start-kafka.bat"

echo Waiting for Kafka to bind to port 9092...
:wait_kafka
powershell -Command "try { $c = New-Object System.Net.Sockets.TcpClient('localhost', 9092); $c.Close(); exit 0 } catch { exit 1 }" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    timeout /t 2 /nobreak >nul 2>&1
    goto wait_kafka
)

echo [2/4] Starting Schema Registry...
start "Schema Registry" cmd /k ".\start-schema-registry.bat"

echo Waiting for Schema Registry to bind to port 8081...
:wait_sr
powershell -Command "try { $c = New-Object System.Net.Sockets.TcpClient('localhost', 8081); $c.Close(); exit 0 } catch { exit 1 }" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    timeout /t 2 /nobreak >nul 2>&1
    goto wait_sr
)

echo [3/4] Registering Schema and Starting Dashboard...
python register-schema.py
start "Web Dashboard" cmd /k ".\run-dashboard.bat"

echo [4/4] Starting Consumer...
start "Consumer" cmd /k ".\run-consumer.bat"
timeout /t 3 /nobreak >nul

echo.
echo =======================================================
echo   ALL SERVICES ARE RUNNING!
echo   1. Open http://localhost:8080 in your browser
echo   2. Arrange your windows so the dashboard and terminals are visible
echo   3. START YOUR SCREEN RECORDER NOW (Win + Alt + R, or OBS)
echo =======================================================
echo.
pause

echo.
echo [5/5] Firing Producer Fault-Injection Demo...
start "Producer Demo" cmd /k ".\run-demo.bat"

echo.
echo The demo is now running! 
echo Watch the dashboard and the consumer terminal.
echo.
echo Wait for the producer to finish sending all 40 records...
pause

echo.
echo [Bonus] Inspecting the Dead Letter Queue...
start "DLQ Inspector" cmd /c ".\run-dlq.bat & pause"

echo.
echo Recording complete! You can stop your screen recorder.
echo Press any key to stop all background services and clean up...
pause

call .\stop-all.bat
echo All done!
