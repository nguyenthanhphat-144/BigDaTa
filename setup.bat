@echo off
chcp 65001 > nul
echo ===============================================================================
echo   THIET LAP MOI TRUONG DU AN BIG DATA FRAUD DETECTION (NHOM 15)
echo ===============================================================================

cd /d "%~dp0"

REM 1. Kiem tra Python
echo [1/5] Kiem tra trinh thong dich Python...
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Khong tim thay Python! Vui long cai dat Python 3.10 hoac 3.11 tu python.org.
    pause
    exit /b 1
)

REM 2. Tao moi truong ao venv neu chua co
if not exist "venv311" (
    if not exist "venv" (
        echo [2/5] Dang tao moi truong ao Python (venv)...
        python -m venv venv
    )
)

REM 3. Cai dat thu vien tu requirements.txt
echo [3/5] Dang cai dat cac thu vien can thiet tu requirements.txt...
if exist "venv311\Scripts\pip.exe" (
    venv311\Scripts\pip.exe install -r requirements.txt
) else if exist "venv\Scripts\pip.exe" (
    venv\Scripts\pip.exe install -r requirements.txt
) else (
    pip install -r requirements.txt
)

REM 4. Giai nen tap du lieu creditcard.csv neu chua co
echo [4/5] Kiem tra tap du lieu creditcard.csv...
if not exist "creditcard.csv" (
    if exist "data\creditcard.csv.zip" (
        echo Dang giai nen tap du lieu tu data\creditcard.csv.zip...
        powershell -Command "Expand-Archive -Path '.\data\creditcard.csv.zip' -DestinationPath '.' -Force"
        echo Da giai nen thanh cong creditcard.csv!
    )
) else (
    echo Tap du lieu creditcard.csv da san sang.
)

REM 5. Kiem tra thu muc tmp
if not exist "tmp" mkdir tmp

echo [5/5] Kiem tra cum Docker...
where docker >nul 2>&1
if %errorlevel% equ 0 (
    echo Khoi dong cum Docker Compose (Kafka, HDFS)...
    docker compose up -d
) else (
    echo [CANH BAO] Khong tim thay Docker Desktop tren may. Vui long bat Docker Desktop truoc khi chay streaming!
)

echo ===============================================================================
echo   CAI DAT HOAN TAT!
echo   De khoi dong toan bo he thong (4 cua so), chay lenh:
echo      powershell -ExecutionPolicy Bypass -File .\chay.ps1
echo   Hoac click dup chuot vao file: chay.bat
echo ===============================================================================
pause
