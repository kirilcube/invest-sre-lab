@echo off
chcp 65001 >nul

if "%1"=="up" (
    docker compose up -d --build
    exit /b
)

if "%1"=="down" (
    docker compose down
    exit /b
)

if "%1"=="reset" (
    echo 🛑 Stopping services and destroying incident data...
    docker compose down -v
    echo 🧹 Cleanup complete. Database, Kafka, and metrics are wiped clean!
    echo 🚀 Booting up the lab again...
    docker compose up -d --build
    exit /b
)

if "%1"=="clean" (
    echo 🛑 Destroying all data volumes...
    docker compose down -v
    exit /b
)

echo.
echo 🛠️ Available commands are:
echo   .\lab.bat up    - Run the lab (keep the data)
echo   .\lab.bat down  - Stop the lab (keep the data)
echo   .\lab.bat reset - Full reset + restart
echo   .\lab.bat clean - Clean up the data
echo.